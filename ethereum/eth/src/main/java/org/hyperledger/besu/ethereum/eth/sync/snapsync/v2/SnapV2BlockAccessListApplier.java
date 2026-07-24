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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

  public void applyBlockAccessLists(
      final long fromBlock,
      final long toBlock,
      final DownloadedAccountRangeTracker accountRangeTracker,
      final DownloadedStorageRangeTracker storageRangeTracker) {

    if (!worldStateStorageCoordinator.getDataStorageFormat().isBonsaiFormat()) {
      throw new IllegalStateException(
          "Cannot apply snap/2 BALs: data storage format "
              + worldStateStorageCoordinator.getDataStorageFormat()
              + "not supported");
    }

    LOG.info(
        "Applying snap/2 BALs for blocks [{}, {}] (completed ranges: {}, pending ranges: {})",
        fromBlock,
        toBlock,
        accountRangeTracker.completedRangeCount(),
        accountRangeTracker.pendingRangeCount());

    final WorldStateKeyValueStorage.Updater updater = worldStateStorageCoordinator.updater();

    final Map<Hash, PerAccountChanges> changes =
        collectAccountChanges(fromBlock, toBlock, accountRangeTracker);
    if (changes.isEmpty()) {
      LOG.info("No persisted accounts affected by BALs in blocks [{}, {}]", fromBlock, toBlock);
      return;
    }

    final MerkleTrie<Bytes, Bytes> accountTrie = openAccountTrie();

    final BalApplicationStats stats =
        stageAccountChanges(
            changes, accountTrie, updater, storageRangeTracker, accountRangeTracker);

    stageAccountTrieChanges(accountTrie, updater);
    updater.commit();

    LOG.info(
        "Applied snap/2 BALs: {} accounts, {} storage slots, {} storage roots updated",
        stats.accounts,
        stats.storageSlots,
        stats.storageRoots);
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
   * leaving roots computed over partial storage stale. Accounts absent at the new pivot are
   * deleted.
   *
   * @return the number of corrected and deleted accounts
   */
  public PatchResult patchStorageRoots(
      final Map<Hash, Bytes32> correctRoots, final Set<Hash> absentAccounts) {
    if (correctRoots.isEmpty() && absentAccounts.isEmpty()) {
      return new PatchResult(0, 0);
    }

    final WorldStateKeyValueStorage.Updater updater = worldStateStorageCoordinator.updater();
    final MerkleTrie<Bytes, Bytes> accountTrie = openAccountTrie();

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

    int deleted = 0;
    for (final Hash accountHash : absentAccounts) {
      applyForStrategy(
          updater, onBonsai -> onBonsai.removeAccountInfoState(accountHash), onForest -> {});
      accountTrie.remove(accountHash.getBytes());
      deleted++;
    }

    if (patched > 0 || deleted > 0) {
      stageAccountTrieChanges(accountTrie, updater);
      if (accountTrie.getRootHash().equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        // Trie emptied by deletions: the empty root is never persisted, remove the stale one.
        applyForStrategy(
            updater, onBonsai -> onBonsai.removeAccountStateTrieNode(Bytes.EMPTY), onForest -> {});
      }
      updater.commit();
    }
    return new PatchResult(patched, deleted);
  }

  public record PatchResult(int patched, int deleted) {}

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
      final boolean applyAllSlots =
          existingAccount == null
              || accountRangeTracker.isAccountHashDownloaded(asBytes32(accountHash));
      final StorageRootResult storageResult =
          maybeUpdateStorageRoot(
              accountHash, perAccount, oldStorageRoot, updater, storageRangeTracker, applyAllSlots);

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
      final boolean applyAllSlots) {

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
      if (!applyAllSlots
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
