/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies Block Access Lists (BALs) to already-downloaded accounts and storage during snap/2 pivot
 * catch-up, updating the flat database, trie nodes, and storage roots.
 */
public class SnapV2BlockAccessListApplier {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2BlockAccessListApplier.class);

  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final MutableBlockchain blockchain;
  private final ProtocolSchedule protocolSchedule;

  public SnapV2BlockAccessListApplier(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule) {
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
  }

  public BatchState applyBlockAccessLists(
      final long fromBlock,
      final long toBlock,
      final DownloadedAccountRangeTracker accountRangeTracker,
      final DownloadedStorageRangeTracker storageRangeTracker) {

    LOG.info(
        "Applying snap/2 BALs for blocks [{}, {}] (completed ranges: {}, pending ranges: {})",
        fromBlock,
        toBlock,
        accountRangeTracker.completedRangeCount(),
        accountRangeTracker.pendingRangeCount());

    final WorldStateKeyValueStorage.Updater updater = worldStateStorageCoordinator.updater();
    final MerkleTrie<Bytes, Bytes> accountTrie = openAccountTrie();

    final Map<Hash, PerAccountChanges> changes =
        collectAccountChanges(fromBlock, toBlock, accountRangeTracker);
    if (changes.isEmpty()) {
      LOG.info("No persisted accounts affected by BALs in blocks [{}, {}]", fromBlock, toBlock);
      return new BatchState(accountTrie, updater);
    }

    final BalApplicationStats stats =
        stageAccountChanges(
            changes, accountTrie, updater, storageRangeTracker, accountRangeTracker);

    stageAccountTrieChanges(accountTrie, updater);

    LOG.info(
        "Applied snap/2 BALs: {} accounts, {} storage slots, {} storage roots updated",
        stats.accounts,
        stats.storageSlots,
        stats.storageRoots);

    return new BatchState(accountTrie, updater);
  }

  private Map<Hash, PerAccountChanges> collectAccountChanges(
      final long fromBlock,
      final long toBlock,
      final DownloadedAccountRangeTracker accountRangeTracker) {

    final Map<Hash, PerAccountChanges> changesByHash = new LinkedHashMap<>();

    for (long blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
      final BlockHeader blockHeader = loadBlockHeader(blockNumber);

      if (!protocolSchedule.getByBlockHeader(blockHeader).isBlockAccessListEnabled()) {
        LOG.debug("Skipping block {}: BALs not enabled", blockNumber);
        continue;
      }

      final BlockAccessList bal = loadBal(blockNumber, blockHeader);
      verifyBalHash(blockNumber, blockHeader, bal);

      if (bal.isEmpty()) {
        continue;
      }

      for (final BlockAccessListChanges.AccountFinalChanges afc :
          BlockAccessListChanges.latestChanges(bal)) {
        final Hash accountHash = afc.address().addressHash();
        final Bytes32 accountHashBytes = asBytes32(accountHash);

        if (!accountRangeTracker.isAccountHashPersisted(accountHashBytes) || !afc.hasAnyChange()) {
          continue;
        }

        changesByHash.computeIfAbsent(accountHash, k -> new PerAccountChanges()).mergeFinal(afc);
      }
    }

    return changesByHash;
  }

  public Set<Hash> collectPendingStorageAffected(
      final BlockHeader currentPivotBlockHeader,
      final BlockHeader newPivotBlockHeader,
      final DownloadedAccountRangeTracker accountRangeTracker) {

    final long fromBlock = currentPivotBlockHeader.getNumber() + 1;
    final long toBlock = newPivotBlockHeader.getNumber();
    final Set<Hash> pendingAffected = new HashSet<>();

    for (long blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
      final BlockHeader blockHeader = loadBlockHeader(blockNumber);

      if (!protocolSchedule.getByBlockHeader(blockHeader).isBlockAccessListEnabled()) {
        continue;
      }

      final BlockAccessList bal = loadBal(blockNumber, blockHeader);
      verifyBalHash(blockNumber, blockHeader, bal);

      if (bal.isEmpty()) {
        continue;
      }

      for (final BlockAccessListChanges.AccountFinalChanges afc :
          BlockAccessListChanges.latestChanges(bal)) {
        final Bytes32 accountHashBytes = asBytes32(afc.address().addressHash());

        if (accountRangeTracker.isAccountHashPending(accountHashBytes)
            && !afc.storageChanges().isEmpty()) {
          pendingAffected.add(afc.address().addressHash());
        }
      }
    }

    return pendingAffected;
  }

  /**
   * Rewrites stale storage roots in the flat db and account trie leaves using verified roots
   * fetched at the new pivot. Needed because storage range downloads never update account state,
   * leaving roots computed over partial storage stale.
   *
   * @return the number of corrected accounts
   */
  public int patchStorageRoots(final BatchState batch, final Map<Hash, Bytes32> correctRoots) {
    if (correctRoots.isEmpty()) {
      return 0;
    }

    final WorldStateKeyValueStorage.Updater updater = batch.updater();
    final MerkleTrie<Bytes, Bytes> accountTrie = batch.accountTrie();

    int patched = 0;
    for (final Map.Entry<Hash, Bytes32> entry : correctRoots.entrySet()) {
      final Hash accountHash = entry.getKey();
      final Hash correctRoot = Hash.wrap(entry.getValue());
      final PmtStateTrieAccountValue existingAccount = readExistingAccount(accountHash);
      if (existingAccount == null) {
        LOG.warn(
            "snap/2 skipping storage root patch for account {}: not found locally", accountHash);
        continue;
      }
      if (existingAccount.getStorageRoot().equals(correctRoot)) {
        continue;
      }
      final PmtStateTrieAccountValue correctedAccount =
          new PmtStateTrieAccountValue(
              existingAccount.getNonce(),
              existingAccount.getBalance(),
              correctRoot,
              existingAccount.getCodeHash());
      final Bytes encodedAccount = RLP.encode(correctedAccount::writeTo);
      applyForStrategy(
          updater,
          onBonsai -> onBonsai.putAccountInfoState(accountHash, encodedAccount),
          onForest -> {});
      accountTrie.put(accountHash.getBytes(), encodedAccount);
      patched++;
    }

    if (patched > 0) {
      stageAccountTrieChanges(accountTrie, updater);
    }
    return patched;
  }

  /**
   * Applies the peer-fetched canonical state ({@link FetchedReorgState}) for the entries {@link
   * ReorgPlan} lists for re-fetch. Must run after the canonical BALs have been applied, since it
   * overwrites whatever those entries currently hold.
   *
   * <p>Accounts absent at the new pivot are deleted: the flat account, account-trie leaf, and all
   * locally persisted storage slots are removed (via flat db prefix-scan, which works for partial
   * storage tries unlike a trie walk). Storage trie nodes and code are left as unreachable
   * content-addressed data.
   *
   * <p>Accounts and slots that do exist at the new pivot are rewritten from the fetched canonical
   * data. For accounts in completed ranges the locally recomputed storage root after slot fixes
   * must equal the fetched canonical root; a mismatch means the fetched data and the local state
   * disagree, so the recovery aborts with {@link WorldStateDownloaderException}.
   *
   * @return the deleted accounts and the canonical storage roots of all rewritten accounts
   */
  public ReorgRecoveryResult applyReorgCorrections(
      final ReorgPlan plan,
      final FetchedReorgState fetched,
      final DownloadedAccountRangeTracker accountRangeTracker,
      final DownloadedStorageRangeTracker storageRangeTracker) {

    final Set<Hash> refetchedAccounts = new HashSet<>(plan.accountsToRefetch());
    refetchedAccounts.addAll(plan.slotsToRefetch().keySet());
    if (refetchedAccounts.isEmpty()) {
      return new ReorgRecoveryResult(Set.of(), Map.of());
    }

    final WorldStateKeyValueStorage.Updater updater = worldStateStorageCoordinator.updater();
    final MerkleTrie<Bytes, Bytes> accountTrie = openAccountTrie();

    final Set<Hash> deletedAccounts = new HashSet<>();
    final Map<Hash, Bytes32> correctedRoots = new HashMap<>();

    // Delete the accounts that do not exist at the new pivot.
    for (final Hash accountHash : refetchedAccounts) {
      final Optional<PmtStateTrieAccountValue> fetchedAccount = fetched.accounts().get(accountHash);
      if (fetchedAccount == null) {
        throw new WorldStateDownloaderException(
            "snap/2 reorg state fetch did not cover account " + accountHash);
      }
      if (fetchedAccount.isEmpty()) {
        deleteAccount(accountHash, accountTrie, updater, storageRangeTracker);
        deletedAccounts.add(accountHash);
      }
    }

    // Fix diverged slots on surviving accounts. Each storage trie is opened at its current local
    // root, before any flat account is rewritten below.
    for (final Map.Entry<Hash, Set<Hash>> entry : plan.slotsToRefetch().entrySet()) {
      final Hash accountHash = entry.getKey();
      if (deletedAccounts.contains(accountHash)) {
        continue;
      }
      final PmtStateTrieAccountValue canonicalAccount = fetchedAccountOrThrow(fetched, accountHash);
      final Map<Hash, Optional<UInt256>> fetchedSlots =
          fetched.slotsByAccount().getOrDefault(accountHash, Map.of());
      fixDivergedSlots(
          accountHash,
          entry.getValue(),
          fetchedSlots,
          canonicalAccount,
          accountRangeTracker,
          updater);
    }

    // Rewrite the flat accounts of all surviving re-fetched accounts from the canonical data:
    // this repairs orphaned-fork-only scalar changes and installs the canonical storage root.
    for (final Hash accountHash : refetchedAccounts) {
      if (deletedAccounts.contains(accountHash)) {
        continue;
      }
      final PmtStateTrieAccountValue canonicalAccount = fetchedAccountOrThrow(fetched, accountHash);
      final Bytes encodedAccount = RLP.encode(canonicalAccount::writeTo);
      applyForStrategy(
          updater,
          onBonsai -> onBonsai.putAccountInfoState(accountHash, encodedAccount),
          onForest -> {});
      accountTrie.put(accountHash.getBytes(), encodedAccount);
      storeFetchedCodeIfMissing(accountHash, canonicalAccount, fetched, updater);
      correctedRoots.put(accountHash, Bytes32.wrap(canonicalAccount.getStorageRoot().getBytes()));
    }

    stageAccountTrieChanges(accountTrie, updater);
    updater.commit();

    LOG.info(
        "Applied snap/2 reorg corrections: {} accounts restored, {} accounts deleted",
        correctedRoots.size(),
        deletedAccounts.size());
    return new ReorgRecoveryResult(deletedAccounts, correctedRoots);
  }

  private void deleteAccount(
      final Hash accountHash,
      final MerkleTrie<Bytes, Bytes> accountTrie,
      final WorldStateKeyValueStorage.Updater updater,
      final DownloadedStorageRangeTracker storageRangeTracker) {
    final NavigableMap<Bytes32, Bytes> persistedSlots =
        worldStateStorageCoordinator.applyForStrategy(
            bonsai -> bonsai.streamFlatStorages(accountHash, Bytes32.ZERO, slotEntry -> true),
            forest -> new TreeMap<>());
    for (final Bytes32 slotHash : persistedSlots.keySet()) {
      applyForStrategy(
          updater,
          onBonsai -> onBonsai.removeStorageValueBySlotHash(accountHash, Hash.wrap(slotHash)),
          onForest -> {});
    }
    applyForStrategy(
        updater, onBonsai -> onBonsai.removeAccountInfoState(accountHash), onForest -> {});
    accountTrie.remove(accountHash.getBytes());
    storageRangeTracker.removeAccount(asBytes32(accountHash));
  }

  private void fixDivergedSlots(
      final Hash accountHash,
      final Set<Hash> divergedSlots,
      final Map<Hash, Optional<UInt256>> fetchedSlots,
      final PmtStateTrieAccountValue canonicalAccount,
      final DownloadedAccountRangeTracker accountRangeTracker,
      final WorldStateKeyValueStorage.Updater updater) {

    final PmtStateTrieAccountValue localAccount = readExistingAccount(accountHash);
    if (localAccount == null) {
      throw new WorldStateDownloaderException(
          "snap/2 reorg correction: account " + accountHash + " not found locally");
    }

    final MerkleTrie<Bytes, Bytes> storageTrie = openStorageTrie(accountHash, localAccount);
    final NodeUpdater storageNodeUpdater =
        (location, hash, value) ->
            applyForStrategy(
                updater,
                onBonsai -> onBonsai.putAccountStorageTrieNode(accountHash, location, hash, value),
                onForest -> {});

    for (final Hash slotHash : divergedSlots) {
      final Optional<UInt256> fetchedValue = fetchedSlots.get(slotHash);
      if (fetchedValue == null) {
        throw new WorldStateDownloaderException(
            "snap/2 reorg state fetch did not cover slot "
                + slotHash
                + " of account "
                + accountHash);
      }
      final Bytes slotPath = slotHash.getBytes();
      if (fetchedValue.isEmpty() || fetchedValue.get().equals(UInt256.ZERO)) {
        storageTrie.remove(slotPath);
        applyForStrategy(
            updater,
            onBonsai -> onBonsai.removeStorageValueBySlotHash(accountHash, slotHash),
            onForest -> {});
      } else {
        storageTrie.put(slotPath, encodeTrieValue(fetchedValue.get()));
        applyForStrategy(
            updater,
            onBonsai ->
                onBonsai.putStorageValueBySlotHash(
                    accountHash, slotHash, fetchedValue.get().toBytes()),
            onForest -> {});
      }
    }

    storageTrie.commit(storageNodeUpdater);

    // Accounts in completed ranges have their full storage locally, so the recomputed root must
    // match the canonical one. Pending accounts keep a partial trie and take the fetched root.
    final Hash recomputedRoot = Hash.wrap(storageTrie.getRootHash());
    if (accountRangeTracker.isAccountHashDownloaded(asBytes32(accountHash))
        && !recomputedRoot.equals(canonicalAccount.getStorageRoot())) {
      throw new WorldStateDownloaderException(
          String.format(
              "snap/2 reorg correction: storage root mismatch for account %s: recomputed %s after"
                  + " slot fixes but canonical root is %s",
              accountHash, recomputedRoot, canonicalAccount.getStorageRoot()));
    }
  }

  private void storeFetchedCodeIfMissing(
      final Hash accountHash,
      final PmtStateTrieAccountValue canonicalAccount,
      final FetchedReorgState fetched,
      final WorldStateKeyValueStorage.Updater updater) {
    final Hash codeHash = canonicalAccount.getCodeHash();
    if (Hash.EMPTY.equals(codeHash) || hasCodeLocally(codeHash, accountHash)) {
      return;
    }
    final Bytes code = fetched.codeByHash().get(codeHash);
    if (code == null) {
      throw new WorldStateDownloaderException(
          "snap/2 reorg correction: canonical code "
              + codeHash
              + " for account "
              + accountHash
              + " was not fetched");
    }
    applyForStrategy(
        updater, onBonsai -> onBonsai.putCode(accountHash, codeHash, code), onForest -> {});
  }

  private boolean hasCodeLocally(final Hash codeHash, final Hash accountHash) {
    return worldStateStorageCoordinator
        .getCode(codeHash, accountHash)
        .map(code -> !code.isEmpty())
        .orElse(false);
  }

  private PmtStateTrieAccountValue fetchedAccountOrThrow(
      final FetchedReorgState fetched, final Hash accountHash) {
    return fetched
        .accounts()
        .getOrDefault(accountHash, Optional.empty())
        .orElseThrow(
            () ->
                new WorldStateDownloaderException(
                    "snap/2 reorg correction: account "
                        + accountHash
                        + " unexpectedly absent at the new pivot"));
  }

  private MerkleTrie<Bytes, Bytes> openStorageTrie(
      final Hash accountHash, final PmtStateTrieAccountValue account) {
    final NodeLoader storageNodeLoader =
        (location, hash) ->
            worldStateStorageCoordinator.getAccountStorageTrieNode(accountHash, location, hash);
    return new StoredMerklePatriciaTrie<>(
        storageNodeLoader,
        Bytes32.wrap(account.getStorageRoot().getBytes()),
        Function.identity(),
        Function.identity());
  }

  private MerkleTrie<Bytes, Bytes> openAccountTrie() {
    final Function<Bytes, Bytes> identity = Function.identity();
    final NodeLoader accountNodeLoader =
        (location, hash) -> worldStateStorageCoordinator.getAccountStateTrieNode(location, hash);

    final Bytes32 rootHash =
        worldStateStorageCoordinator
            .getTrieNodeUnsafe(Bytes.EMPTY)
            .map(node -> Bytes32.wrap(Hash.hash(node).getBytes()))
            .orElse(MerkleTrie.EMPTY_TRIE_NODE_HASH);

    return new StoredMerklePatriciaTrie<>(accountNodeLoader, rootHash, identity, identity);
  }

  /**
   * Stages account, storage, and code changes into the updater batch. Updates the in-memory account
   * trie values and commits per-account storage tries, staging their trie nodes into the same
   * updater. Nothing is persisted until the caller invokes {@code updater.commit()}.
   */
  private BalApplicationStats stageAccountChanges(
      final Map<Hash, PerAccountChanges> changesByHash,
      final MerkleTrie<Bytes, Bytes> accountTrie,
      final WorldStateKeyValueStorage.Updater updater,
      final DownloadedStorageRangeTracker storageRangeTracker,
      final DownloadedAccountRangeTracker accountRangeTracker) {

    int updatedAccounts = 0;
    int updatedStorageSlots = 0;
    int updatedStorageRoots = 0;

    for (final Map.Entry<Hash, PerAccountChanges> entry : changesByHash.entrySet()) {
      final Hash accountHash = entry.getKey();
      final PerAccountChanges perAccount = entry.getValue();

      final PmtStateTrieAccountValue existingAccount = readExistingAccount(accountHash);

      final long newNonce = computeNewNonce(perAccount, existingAccount);
      final Wei newBalance = computeNewBalance(perAccount, existingAccount);
      final Hash newCodeHash = maybeStoreCode(perAccount, existingAccount, accountHash, updater);

      final Hash oldStorageRoot = storageRootOf(existingAccount);
      final boolean isAccountCompleted =
          accountRangeTracker.isAccountHashDownloaded(asBytes32(accountHash));
      final StorageRootResult storageResult =
          maybeUpdateStorageRoot(
              accountHash,
              perAccount,
              oldStorageRoot,
              updater,
              storageRangeTracker,
              isAccountCompleted);

      final PmtStateTrieAccountValue updatedAccount =
          new PmtStateTrieAccountValue(newNonce, newBalance, storageResult.root, newCodeHash);
      final Bytes encodedAccount = RLP.encode(updatedAccount::writeTo);

      applyForStrategy(
          updater,
          onBonsai -> onBonsai.putAccountInfoState(accountHash, encodedAccount),
          // TODO: What would it take to implement support for forest in BAL applier?
          onForest -> {});
      updatedAccounts++;

      accountTrie.put(accountHash.getBytes(), encodedAccount);

      if (!storageResult.root.equals(oldStorageRoot)) {
        updatedStorageRoots++;
      }
      updatedStorageSlots += storageResult.downloadedSlots;
    }

    return new BalApplicationStats(updatedAccounts, updatedStorageSlots, updatedStorageRoots);
  }

  /**
   * Walks the account trie and stages dirty merkle nodes into the updater batch. Does not persist
   * anything; the actual commit happens in the caller via {@code updater.commit()}.
   */
  private void stageAccountTrieChanges(
      final MerkleTrie<Bytes, Bytes> accountTrie, final WorldStateKeyValueStorage.Updater updater) {

    final NodeUpdater nodeUpdater =
        (location, hash, value) ->
            applyForStrategy(
                updater,
                onBonsai -> onBonsai.putAccountStateTrieNode(location, hash, value),
                onForest -> {});

    accountTrie.commit(nodeUpdater);
  }

  private PmtStateTrieAccountValue readExistingAccount(final Hash accountHash) {
    return worldStateStorageCoordinator
        .applyForStrategy(
            bonsai -> bonsai.getAccount(accountHash), forest -> Optional.<Bytes>empty())
        .map(b -> PmtStateTrieAccountValue.readFrom(RLP.input(b)))
        .orElse(null);
  }

  private static long computeNewNonce(
      final PerAccountChanges perAccount, final PmtStateTrieAccountValue existing) {
    if (perAccount.latestNonce != null) {
      return perAccount.latestNonce;
    }
    return nonceOf(existing);
  }

  private static Wei computeNewBalance(
      final PerAccountChanges perAccount, final PmtStateTrieAccountValue existing) {
    if (perAccount.latestBalance != null) {
      return perAccount.latestBalance;
    }
    return balanceOf(existing);
  }

  private Hash maybeStoreCode(
      final PerAccountChanges perAccount,
      final PmtStateTrieAccountValue existing,
      final Hash accountHash,
      final WorldStateKeyValueStorage.Updater updater) {

    if (perAccount.latestCode == null) {
      return codeHashOf(existing);
    }

    final Hash codeHash =
        perAccount.latestCode.isEmpty() ? Hash.EMPTY : Hash.hash(perAccount.latestCode);
    applyForStrategy(
        updater,
        onBonsai -> onBonsai.putCode(accountHash, codeHash, perAccount.latestCode),
        onForest -> {});
    return codeHash;
  }

  private StorageRootResult maybeUpdateStorageRoot(
      final Hash accountHash,
      final PerAccountChanges perAccount,
      final Hash oldStorageRoot,
      final WorldStateKeyValueStorage.Updater updater,
      final DownloadedStorageRangeTracker storageRangeTracker,
      final boolean isAccountCompleted) {

    if (perAccount.storageChanges.isEmpty()) {
      return new StorageRootResult(oldStorageRoot, 0);
    }

    final Bytes32 accountHashBytes = asBytes32(accountHash);

    final NodeLoader storageNodeLoader =
        (location, hash) ->
            worldStateStorageCoordinator.getAccountStorageTrieNode(accountHash, location, hash);

    final MerkleTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            storageNodeLoader,
            Bytes32.wrap(oldStorageRoot.getBytes()),
            Function.identity(),
            Function.identity());

    final NodeUpdater storageNodeUpdater =
        (location, hash, value) ->
            applyForStrategy(
                updater,
                onBonsai -> onBonsai.putAccountStorageTrieNode(accountHash, location, hash, value),
                onForest -> {});

    int downloadedSlots = 0;
    for (final PerAccountChanges.StorageSlotUpdate update : perAccount.storageChanges.values()) {
      if (!isAccountCompleted
          && !storageRangeTracker.isSlotHashDownloaded(
              accountHashBytes, asBytes32(update.slotHash))) {
        continue;
      }
      downloadedSlots++;

      final Bytes slotPath = update.slotHash.getBytes();
      if (update.newValue.equals(UInt256.ZERO)) {
        storageTrie.remove(slotPath);
        applyForStrategy(
            updater,
            onBonsai -> onBonsai.removeStorageValueBySlotHash(accountHash, update.slotHash),
            onForest -> {});
      } else {
        storageTrie.put(slotPath, encodeTrieValue(update.newValue));
        applyForStrategy(
            updater,
            onBonsai ->
                onBonsai.putStorageValueBySlotHash(
                    accountHash, update.slotHash, update.newValue.toBytes()),
            onForest -> {});
      }
    }

    if (downloadedSlots == 0) {
      return new StorageRootResult(oldStorageRoot, 0);
    }

    storageTrie.commit(storageNodeUpdater);
    return new StorageRootResult(Hash.wrap(storageTrie.getRootHash()), downloadedSlots);
  }

  private static Hash storageRootOf(final PmtStateTrieAccountValue account) {
    return account != null ? account.getStorageRoot() : Hash.EMPTY_TRIE_HASH;
  }

  private static long nonceOf(final PmtStateTrieAccountValue account) {
    return account != null ? account.getNonce() : 0L;
  }

  private static Wei balanceOf(final PmtStateTrieAccountValue account) {
    return account != null ? account.getBalance() : Wei.ZERO;
  }

  private static Hash codeHashOf(final PmtStateTrieAccountValue account) {
    return account != null ? account.getCodeHash() : Hash.EMPTY;
  }

  private static Bytes encodeTrieValue(final UInt256 storageValue) {
    return RLP.encode(out -> out.writeBytes(storageValue.toMinimalBytes()));
  }

  private BlockHeader loadBlockHeader(final long blockNumber) {
    final long bn = blockNumber;
    return blockchain
        .getBlockHeader(bn)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Missing block header " + bn + " for snap/2 BAL application"));
  }

  private void verifyBalHash(
      final long blockNumber, final BlockHeader blockHeader, final BlockAccessList bal) {

    final Hash headerBalHash =
        blockHeader
            .getBalHash()
            .orElseThrow(
                () -> new IllegalStateException("BAL hash missing in block number " + blockNumber));

    final Hash computedBalHash =
        bal.rawRlp().map(BodyValidation::balHash).orElseGet(() -> BodyValidation.balHash(bal));

    if (computedBalHash.equals(headerBalHash)) {
      return;
    }

    throw new WorldStateDownloaderException(
        String.format(
            "BAL hash mismatch for block %d: computed=%s header=%s",
            blockNumber, computedBalHash.toHexString(), headerBalHash.toHexString()));
  }

  private BlockAccessList loadBal(final long blockNumber, final BlockHeader blockHeader) {
    final long bn = blockNumber;
    return blockchain
        .getBlockAccessList(blockHeader.getHash())
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Missing BAL for block " + bn + " (" + blockHeader.getHash() + ")"));
  }

  private static Bytes32 asBytes32(final Hash hash) {
    return Bytes32.wrap(hash.getBytes());
  }

  private record BalApplicationStats(int accounts, int storageSlots, int storageRoots) {}

  record BatchState(
      MerkleTrie<Bytes, Bytes> accountTrie, WorldStateKeyValueStorage.Updater updater) {
    void commit() {
      updater.commit();
    }
  }

  private record StorageRootResult(Hash root, int downloadedSlots) {}

  /** Accumulates account changes across multiple BALs for a single account. */
  private static class PerAccountChanges {
    Long latestNonce;
    Wei latestBalance;
    Bytes latestCode;
    final Map<Hash, StorageSlotUpdate> storageChanges = new LinkedHashMap<>();

    void mergeFinal(final BlockAccessListChanges.AccountFinalChanges afc) {
      afc.balance().ifPresent(b -> latestBalance = b);
      afc.nonce().ifPresent(n -> latestNonce = n);
      afc.code().ifPresent(c -> latestCode = c);
      for (final BlockAccessListChanges.StorageFinalChange sfc : afc.storageChanges()) {
        final Hash slotHash = sfc.slot().getSlotHash();
        storageChanges.put(
            slotHash,
            new StorageSlotUpdate(slotHash, sfc.slot().getSlotKey().orElseThrow(), sfc.value()));
      }
    }

    record StorageSlotUpdate(Hash slotHash, UInt256 slotKey, UInt256 newValue) {}
  }
}
