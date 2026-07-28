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
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes the information needed to recover from a chain reorganization past the current snap/2
 * pivot, and applies the canonical-fork BALs.
 *
 * <ul>
 *   <li>Accounts and slots that appear in the canonical BALs (whether overlapping with the orphaned
 *       fork or canonical-only) are resolved by {@link #applyCanonicalBals}, which applies the
 *       canonical BALs starting from the common ancestor {@code W}+1.
 *   <li>Everything else that needs correcting is identified by {@link #planReorg} as two sets of
 *       re-fetches (see {@link ReorgPlan}): accounts whose record must be re-fetched, and
 *       per-account storage slots.
 * </ul>
 */
public class SnapV2ReorgHealer {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2ReorgHealer.class);

  static final int MAX_ANCESTOR_WALK = 95;

  private final MutableBlockchain blockchain;
  private final ProtocolSchedule protocolSchedule;
  private final SnapV2BlockAccessListApplier applier;

  public SnapV2ReorgHealer(
      final MutableBlockchain blockchain,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolSchedule protocolSchedule) {
    this(
        blockchain,
        protocolSchedule,
        new SnapV2BlockAccessListApplier(
            worldStateStorageCoordinator, blockchain, protocolSchedule));
  }

  SnapV2ReorgHealer(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final SnapV2BlockAccessListApplier applier) {
    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
    this.applier = applier;
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
    applier.applyBlockAccessLists(
        plan.fromBlock(), plan.toBlock(), accountRangeTracker, storageRangeTracker);
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
