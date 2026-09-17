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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recovers the partially-downloaded state from a chain reorganization past the current snap/2
 * pivot.
 *
 * <ul>
 *   <li>Accounts and slots that appear in the canonical BALs (whether overlapping with the orphaned
 *       fork or canonical-only) are resolved by {@link #applyCanonicalBals}, which applies the
 *       canonical BALs starting from the common ancestor {@code W}+1.
 *   <li>Everything else that needs correcting is identified by {@link #planReorg} as two sets of
 *       re-fetches (see {@link ReorgPlan}): accounts whose record must be re-fetched, and
 *       per-account storage slots.
 * </ul>
 *
 * <p>{@link #recoverFromReorg} runs the whole sequence and is the entry point used by the pivot
 * catch-up.
 */
public class SnapV2ReorgHealer {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2ReorgHealer.class);

  static final int MAX_ANCESTOR_WALK = 95;

  private final MutableBlockchain blockchain;
  private final ProtocolSchedule protocolSchedule;
  private final SnapV2BlockAccessListApplier applier;
  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final SnapV2ReorgStateFetcher stateFetcher;

  public SnapV2ReorgHealer(
      final MutableBlockchain blockchain,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolSchedule protocolSchedule,
      final SnapV2ReorgStateFetcher stateFetcher) {
    this(
        blockchain,
        protocolSchedule,
        new SnapV2BlockAccessListApplier(
            worldStateStorageCoordinator, blockchain, protocolSchedule),
        worldStateStorageCoordinator,
        stateFetcher);
  }

  SnapV2ReorgHealer(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final SnapV2BlockAccessListApplier applier,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapV2ReorgStateFetcher stateFetcher) {
    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
    this.applier = applier;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.stateFetcher = stateFetcher;
  }

  /**
   * Finds the common ancestor {@code W} of the old (now orphaned) and new (canonical) chains by
   * walking the old pivot's parent chain back until a block that is still on the canonical chain is
   * found. The walk is bounded by {@value #MAX_ANCESTOR_WALK} blocks; reorgs deeper than that are
   * unrecoverable.
   *
   * @throws ReorgUnrecoverableException if the new pivot is itself orphaned, a parent header is
   *     missing locally, or the walk reaches the bound without finding a canonical ancestor.
   */
  @VisibleForTesting
  public BlockHeader findCommonAncestor(final BlockHeader oldPivot, final BlockHeader newPivot) {
    // TODO: use another pivot instead of throwing
    if (!blockchain.blockIsOnCanonicalChain(newPivot.getHash())) {
      throw new ReorgUnrecoverableException(
          "Cannot recover reorg: new pivot "
              + newPivot.getNumber()
              + " ("
              + newPivot.getHash()
              + ") is not on the canonical chain");
    }

    long steps = 0;
    BlockHeader header = oldPivot;
    while (header != null) {
      if (blockchain.blockIsOnCanonicalChain(header.getHash())) {
        return header;
      }
      final Hash parentHash = header.getParentHash();
      final Optional<BlockHeader> parent = blockchain.getBlockHeader(parentHash);
      if (parent.isEmpty()) {
        break;
      }
      header = parent.get();
      if (++steps > MAX_ANCESTOR_WALK) {
        break;
      }
    }
    throw new ReorgUnrecoverableException(
        "Cannot recover reorg: no common ancestor within "
            + MAX_ANCESTOR_WALK
            + " blocks of old pivot "
            + oldPivot.getNumber()
            + " ("
            + oldPivot.getHash()
            + "); orphaned chain data is no longer retained");
  }

  /**
   * Builds the deterministic reorg recovery plan. All inputs are read locally: canonical headers
   * and BALs by number, orphaned headers and BALs by hash.
   *
   * @throws ReorgUnrecoverableException if no common ancestor is found within the walk bound
   * @throws IllegalStateException if a canonical header or BAL in the apply window is missing
   *     locally.
   */
  public ReorgPlan planReorg(
      final BlockHeader oldPivot,
      final BlockHeader newPivot,
      final DownloadedAccountRangeTracker accountRangeTracker,
      final DownloadedStorageRangeTracker storageRangeTracker) {

    final BlockHeader commonAncestor = findCommonAncestor(oldPivot, newPivot);
    final long fromBlock = commonAncestor.getNumber() + 1;
    final long toBlock = newPivot.getNumber();

    LOG.info(
        "snap/2 reorg plan: oldPivot={}, newPivot={}, commonAncestor={}, applying canonical BALs [{}, {}]",
        oldPivot.getNumber(),
        newPivot.getNumber(),
        commonAncestor.getNumber(),
        fromBlock,
        toBlock);

    checkBalActivation(commonAncestor, fromBlock);

    final Map<Hash, AccountTouches> orphanedTouches =
        collectOrphanedTouches(oldPivot, commonAncestor);
    final Map<Hash, AccountTouches> canonicalTouches = collectCanonicalTouches(fromBlock, toBlock);
    final Set<Hash> accountsToRefetch =
        computeAccountsToRefetch(orphanedTouches, canonicalTouches, accountRangeTracker);
    final Map<Hash, Set<Hash>> slotsToRefetch =
        computeSlotsToRefetch(
            orphanedTouches, canonicalTouches, accountRangeTracker, storageRangeTracker);

    LOG.info(
        "snap/2 reorg plan computed: accounts to refetch={}, storage accounts to refetch={}",
        accountsToRefetch.size(),
        slotsToRefetch.size());

    return new ReorgPlan(commonAncestor, oldPivot, newPivot, accountsToRefetch, slotsToRefetch);
  }

  /**
   * Applies the canonical-fork BALs for {@code [plan.fromBlock(), plan.toBlock()]}, bringing all
   * persisted accounts touched by the canonical fork up to date. Entries listed for re-fetch in
   * {@link ReorgPlan} are left for a later step.
   */
  public void applyCanonicalBals(
      final ReorgPlan plan,
      final DownloadedAccountRangeTracker accountRangeTracker,
      final DownloadedStorageRangeTracker storageRangeTracker) {
    final var batch =
        applier.applyBlockAccessLists(
            plan.fromBlock(), plan.toBlock(), accountRangeTracker, storageRangeTracker);
    batch.commit();
  }

  /**
   * Runs the full reorg recovery: plans the divergence, fetches the diverged state from peers at
   * the new pivot (in parallel with the canonical BAL application), applies the corrections, and
   * repairs the storage roots of pending accounts affected by the canonical fork.
   *
   * @throws ReorgUnrecoverableException if the reorg cannot be planned locally
   * @throws org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException if the
   *     peer-fetched state is unavailable, invalid, or inconsistent with the local state
   */
  public ReorgRecoveryResult recoverFromReorg(
      final BlockHeader oldPivot,
      final BlockHeader newPivot,
      final DownloadedAccountRangeTracker accountRangeTracker,
      final DownloadedStorageRangeTracker storageRangeTracker) {

    final ReorgPlan plan = planReorg(oldPivot, newPivot, accountRangeTracker, storageRangeTracker);

    LOG.info(
        "snap/2 reorg recovery: {} accounts to refetch, {} accounts with slots to refetch",
        plan.accountsToRefetch().size(),
        plan.slotsToRefetch().size());

    final CompletableFuture<FetchedReorgState> fetchFuture =
        plan.isClean()
            ? CompletableFuture.completedFuture(FetchedReorgState.empty())
            : fetchStateToRefetch(plan, newPivot);

    // Apply canonical BALs while the re-fetch is in flight. The BAL commit must happen
    // before fixDivergedSlots below because it opens storage tries from on-disk roots and
    // the Bonsai Updater is write-only (no read-back of uncommitted writes).
    applyCanonicalBals(plan, accountRangeTracker, storageRangeTracker);

    final FetchedReorgState fetched = joinUnwrapped(fetchFuture);
    final ReorgRecoveryResult recovery =
        applier.applyReorgCorrections(plan, fetched, accountRangeTracker, storageRangeTracker);

    LOG.info(
        "snap/2 reorg recovery complete: {} accounts restored, {} accounts deleted",
        recovery.correctedStorageRoots().size(),
        recovery.deletedAccounts().size());
    return recovery;
  }

  private CompletableFuture<FetchedReorgState> fetchStateToRefetch(
      final ReorgPlan plan, final BlockHeader newPivot) {

    final Set<Hash> accountsToFetch = new HashSet<>(plan.accountsToRefetch());
    accountsToFetch.addAll(plan.slotsToRefetch().keySet());

    return stateFetcher
        .fetchAccounts(accountsToFetch, newPivot)
        .thenCompose(accounts -> fetchMissingSlotsAndCodes(plan, newPivot, accounts));
  }

  private CompletableFuture<FetchedReorgState> fetchMissingSlotsAndCodes(
      final ReorgPlan plan,
      final BlockHeader newPivot,
      final Map<Hash, Optional<PmtStateTrieAccountValue>> accounts) {

    final Map<Hash, Map<Hash, Optional<UInt256>>> slotsByAccount = new ConcurrentHashMap<>();
    final List<CompletableFuture<Void>> slotFutures = new ArrayList<>();

    for (final Map.Entry<Hash, Optional<PmtStateTrieAccountValue>> entry : accounts.entrySet()) {
      if (entry.getValue().isEmpty()) {
        continue;
      }
      final Hash accountHash = entry.getKey();
      final Set<Hash> slotsToRefetch = plan.slotsToRefetchFor(accountHash);
      if (!slotsToRefetch.isEmpty()) {
        final Hash storageRoot = entry.getValue().get().getStorageRoot();
        slotFutures.add(
            stateFetcher
                .fetchSlots(accountHash, storageRoot, slotsToRefetch, newPivot)
                .thenAccept(slots -> slotsByAccount.put(accountHash, slots)));
      }
    }

    final Set<Hash> missingCodeHashes = collectMissingCodeHashes(accounts);
    final CompletableFuture<Map<Hash, Bytes>> codesFuture =
        stateFetcher.fetchCodes(missingCodeHashes, newPivot);

    final List<CompletableFuture<?>> all = new ArrayList<>(slotFutures);
    all.add(codesFuture);
    return CompletableFuture.allOf(all.toArray(CompletableFuture[]::new))
        .thenApply(v -> new FetchedReorgState(accounts, slotsByAccount, codesFuture.join()));
  }

  private Set<Hash> collectMissingCodeHashes(
      final Map<Hash, Optional<PmtStateTrieAccountValue>> accounts) {
    final Set<Hash> missing = new HashSet<>();
    for (final Map.Entry<Hash, Optional<PmtStateTrieAccountValue>> entry : accounts.entrySet()) {
      if (entry.getValue().isEmpty()) {
        continue;
      }
      final Hash codeHash = entry.getValue().get().getCodeHash();
      if (!Hash.EMPTY.equals(codeHash) && !hasCodeLocally(codeHash, entry.getKey())) {
        missing.add(codeHash);
      }
    }
    return missing;
  }

  private boolean hasCodeLocally(final Hash codeHash, final Hash accountHash) {
    return worldStateStorageCoordinator
        .getCode(codeHash, accountHash)
        .map(code -> !code.isEmpty())
        .orElse(false);
  }

  private static <T> T joinUnwrapped(final CompletableFuture<T> future) {
    try {
      return future.join();
    } catch (final CompletionException e) {
      if (e.getCause() instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw e;
    }
  }

  private Map<Hash, AccountTouches> collectOrphanedTouches(
      final BlockHeader oldPivot, final BlockHeader commonAncestor) {
    final Map<Hash, AccountTouches> touches = new HashMap<>();
    BlockHeader header = oldPivot;
    while (header != null && header.getNumber() > commonAncestor.getNumber()) {
      final BlockHeader current = header;
      final BlockAccessList bal =
          blockchain
              .getBlockAccessList(current.getHash())
              .orElseThrow(
                  () ->
                      new ReorgUnrecoverableException(
                          "Cannot recover reorg: orphaned BAL for block "
                              + current.getNumber()
                              + " ("
                              + current.getHash()
                              + ") is no longer retained locally"));
      collectTouches(bal, touches);
      final Hash parentHash = header.getParentHash();
      final Optional<BlockHeader> parent = blockchain.getBlockHeader(parentHash);
      if (parent.isEmpty()) {
        throw new ReorgUnrecoverableException(
            "Cannot recover reorg: orphaned parent of block " + header.getNumber() + " missing");
      }
      header = parent.get();
    }
    return touches;
  }

  private Map<Hash, AccountTouches> collectCanonicalTouches(
      final long fromBlock, final long toBlock) {
    final Map<Hash, AccountTouches> touches = new HashMap<>();
    for (long blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
      final long bn = blockNumber;
      final BlockHeader header = loadCanonicalHeader(bn);
      final BlockAccessList bal =
          blockchain
              .getBlockAccessList(header.getHash())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Missing canonical BAL for block " + bn + " (" + header.getHash() + ")"));
      collectTouches(bal, touches);
    }
    return touches;
  }

  private static void collectTouches(
      final BlockAccessList bal, final Map<Hash, AccountTouches> touches) {
    for (final BlockAccessList.AccountChanges accountChanges : bal.accountChanges()) {
      if (!accountChanges.hasAnyChange()) {
        continue;
      }
      final Hash accountHash = accountChanges.address().addressHash();
      final AccountTouches accountTouches =
          touches.computeIfAbsent(accountHash, k -> new AccountTouches());
      accountTouches.balanceChanged |= !accountChanges.balanceChanges().isEmpty();
      accountTouches.nonceChanged |= !accountChanges.nonceChanges().isEmpty();
      accountTouches.codeChanged |= !accountChanges.codeChanges().isEmpty();
      for (final BlockAccessList.SlotChanges slotChanges : accountChanges.storageChanges()) {
        accountTouches.slots.add(slotChanges.slot().getSlotHash());
      }
    }
  }

  private void checkBalActivation(final BlockHeader commonAncestor, final long fromBlock) {
    final BlockHeader firstCanonicalHeader = loadCanonicalHeader(fromBlock);
    if (!protocolSchedule.getByBlockHeader(firstCanonicalHeader).isBlockAccessListEnabled()) {
      throw new ReorgUnrecoverableException(
          "Cannot recover reorg: block "
              + fromBlock
              + " (common ancestor "
              + commonAncestor.getNumber()
              + " + 1) is below EIP-7928 (BAL) activation; reorg is deeper than the BAL-enabled"
              + " window");
    }
  }

  private Set<Hash> computeAccountsToRefetch(
      final Map<Hash, AccountTouches> orphanedTouches,
      final Map<Hash, AccountTouches> canonicalTouches,
      final DownloadedAccountRangeTracker accountRangeTracker) {
    final Set<Hash> toRefetch = new HashSet<>();
    final Set<Hash> candidates = new HashSet<>(orphanedTouches.keySet());
    candidates.addAll(canonicalTouches.keySet());
    for (final Hash account : candidates) {
      final Bytes32 accountHash = asBytes32(account);
      if (!accountRangeTracker.isAccountHashPersisted(accountHash)) {
        continue;
      }
      final AccountTouches orphaned = orphanedTouches.get(account);
      final AccountTouches canonical = canonicalTouches.get(account);
      final boolean scalarOrphanedOnly =
          orphaned != null
              && ((orphaned.balanceChanged && (canonical == null || !canonical.balanceChanged))
                  || (orphaned.nonceChanged && (canonical == null || !canonical.nonceChanged))
                  || (orphaned.codeChanged && (canonical == null || !canonical.codeChanged)));
      final boolean pendingStorageTouched =
          accountRangeTracker.isAccountHashPending(accountHash)
              && ((orphaned != null && orphaned.hasStorageChanges())
                  || (canonical != null && canonical.hasStorageChanges()));
      if (scalarOrphanedOnly || pendingStorageTouched) {
        toRefetch.add(account);
      }
    }
    return toRefetch;
  }

  private Map<Hash, Set<Hash>> computeSlotsToRefetch(
      final Map<Hash, AccountTouches> orphanedTouches,
      final Map<Hash, AccountTouches> canonicalTouches,
      final DownloadedAccountRangeTracker accountRangeTracker,
      final DownloadedStorageRangeTracker storageRangeTracker) {
    final Map<Hash, Set<Hash>> diverged = new HashMap<>();
    for (final Map.Entry<Hash, AccountTouches> entry : orphanedTouches.entrySet()) {
      final Hash account = entry.getKey();
      final Bytes32 accountHash = asBytes32(account);
      if (!accountRangeTracker.isAccountHashPersisted(accountHash)) {
        continue;
      }
      final boolean isAccountCompleted = accountRangeTracker.isAccountHashDownloaded(accountHash);
      final AccountTouches canonical = canonicalTouches.get(account);
      final Set<Hash> canonicalAccountSlots = canonical == null ? Set.of() : canonical.slots;
      final Set<Hash> divergedSlots = new HashSet<>();
      for (final Hash slot : entry.getValue().slots) {
        if (!canonicalAccountSlots.contains(slot)
            && (isAccountCompleted
                || storageRangeTracker.isSlotHashDownloaded(accountHash, asBytes32(slot)))) {
          divergedSlots.add(slot);
        }
      }
      if (!divergedSlots.isEmpty()) {
        diverged.put(account, divergedSlots);
      }
    }
    return diverged;
  }

  private BlockHeader loadCanonicalHeader(final long blockNumber) {
    return blockchain
        .getBlockHeader(blockNumber)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Missing canonical block header for block " + blockNumber));
  }

  private static Bytes32 asBytes32(final Hash hash) {
    return Bytes32.wrap(hash.getBytes());
  }

  private static final class AccountTouches {
    private boolean balanceChanged;
    private boolean nonceChanged;
    private boolean codeChanged;
    private final Set<Hash> slots = new HashSet<>();

    boolean hasStorageChanges() {
      return !slots.isEmpty();
    }
  }
}
