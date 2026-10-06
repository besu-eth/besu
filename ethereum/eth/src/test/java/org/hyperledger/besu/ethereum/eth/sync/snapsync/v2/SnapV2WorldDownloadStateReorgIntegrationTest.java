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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.v2.ReorgBlockchainBuilder.accountExists;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.v2.ReorgBlockchainBuilder.readAccount;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.v2.ReorgBlockchainBuilder.readCode;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.v2.ReorgBlockchainBuilder.readStorageSlot;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2AccountRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2StorageRangeRequest;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Integration tests for the snap/2 reorg recovery. */
class SnapV2WorldDownloadStateReorgIntegrationTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address CAROL =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address ORPHAN_EOA =
      Address.fromHexString("0x4444444444444444444444444444444444444444");
  private static final Address STORAGE_ACCT =
      Address.fromHexString("0x6666666666666666666666666666666666666666");
  private static final Address CANONICAL_NEW =
      Address.fromHexString("0x7777777777777777777777777777777777777777");
  private static final Address NEW_CONTRACT =
      Address.fromHexString("0x9999999999999999999999999999999999999999");
  private static final Address PENDING_ACCT =
      Address.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  private static final Address UNTOUCHED =
      Address.fromHexString("0x2222222222222222222222222222222222222222");

  private static final Bytes NEW_CONTRACT_CODE = Bytes.fromHexString("0x60806040523480156010");

  private static final UInt256 SLOT_BASE = UInt256.valueOf(1);
  private static final UInt256 SLOT_ORPHAN = UInt256.valueOf(2);
  private static final UInt256 SLOT_CANONICAL = UInt256.valueOf(3);
  private static final UInt256 SLOT_DOWNLOADED = UInt256.valueOf(101);
  private static final UInt256 SLOT_DEFERRED = UInt256.valueOf(102);

  private final BonsaiWorldStateKeyValueStorage localStorage =
      new BonsaiWorldStateKeyValueStorage(
          new InMemoryKeyValueStorageProvider(),
          new NoOpMetricsSystem(),
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  private final WorldStateStorageCoordinator localCoordinator =
      new WorldStateStorageCoordinator(localStorage);
  private final BonsaiWorldStateKeyValueStorage canonicalStorage =
      new BonsaiWorldStateKeyValueStorage(
          new InMemoryKeyValueStorageProvider(),
          new NoOpMetricsSystem(),
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  private final WorldStateStorageCoordinator canonicalCoordinator =
      new WorldStateStorageCoordinator(canonicalStorage);

  private final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

  private final EthContext ethContext =
      EthProtocolManagerTestBuilder.builder().build().ethContext();

  private final AtomicInteger accountFetches = new AtomicInteger();
  private final AtomicInteger storageFetches = new AtomicInteger();
  private final AtomicInteger codeFetches = new AtomicInteger();

  @Test
  void accountModifiedInOrphanedAndNewBlock_appliesCanonicalBalWithoutReDownload() {
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    // orphaned (2s): Alice = 50  — appended first so it is initially canonical
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    // canonical (2c): Alice = 80 — higher difficulty wins the reorg
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    seedCanonical(1, 2);

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);

    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);

    seedLocal(state, 1, 2);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(readAccount(localCoordinator, ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(accountFetches)
        .hasValue(
            0); // touched on both forks: canonical BAL applied directly, no re-download needed
    assertThat(storageFetches).hasValue(0);
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void accountModifiedInOrphanedButNotNewBlock_reDownloadsWhenPresent() {
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ORPHAN_EOA, Wei.of(75))), 1L);
    // orphaned (2s): ORPHAN_EOA = 60 — appended first so it is initially canonical
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ORPHAN_EOA, Wei.of(60))), 2L);
    // canonical (2c): ALICE = 80, ORPHAN_EOA untouched — higher difficulty wins the reorg
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    seedCanonical(1, 2);

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);

    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);

    seedLocal(state, 1, 2);

    state.startPivotCatchup(newPivot.getHeader());

    // ORPHAN_EOA was modified by orphaned fork but is absent from canonical BAL → re-downloaded.
    assertThat(readAccount(localCoordinator, ORPHAN_EOA).getBalance()).isEqualTo(Wei.of(75));
    assertThat(accountFetches.get()).isGreaterThanOrEqualTo(1);
    assertThat(storageFetches).hasValue(0); // ORPHAN_EOA has no storage; no storage fetch expected
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void accountModifiedInOrphanedButNotNewBlock_deletesWhenAbsentFromCanonical() {
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 1L);

    // Orphaned block creates NEW_CONTRACT with balance, code, and storage.
    final Block block2s =
        b.appendStale(
            block1.getHeader(),
            b.merge(
                b.balWithBalances(Map.of(NEW_CONTRACT, Wei.ONE)),
                b.balWithCodeChange(NEW_CONTRACT, NEW_CONTRACT_CODE),
                b.balWithStorageChanges(NEW_CONTRACT, Map.of(SLOT_BASE, UInt256.valueOf(5)))),
            2L);

    // Pre-seed local with the orphaned state while block2s is still canonical at height 2.
    applyBals(
        localCoordinator,
        1,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    // Verify NEW_CONTRACT is present locally before the canonical fork arrives.
    assertThat(readStorageSlot(localCoordinator, NEW_CONTRACT, SLOT_BASE))
        .hasValue(UInt256.valueOf(5));
    assertThat(accountExists(localCoordinator, NEW_CONTRACT)).isTrue();

    // Canonical fork (block2c) wins the reorg; NEW_CONTRACT is absent from canonical chain.
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    seedCanonical(1, 2);

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);

    // Queue a storage-range and a bytecode request for the doomed contract so the purge is tested.
    final SnapV2StorageRangeRequest storageReq =
        new SnapV2StorageRangeRequest(
            block2s.getHeader(),
            Bytes32.wrap(NEW_CONTRACT.addressHash().getBytes()),
            Bytes32.ZERO,
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE,
            RangeManager.MIN_RANGE);
    final SnapV2BytecodeRequest codeReq =
        new SnapV2BytecodeRequest(
            block2s.getHeader(),
            Bytes32.wrap(NEW_CONTRACT.addressHash().getBytes()),
            Bytes32.wrap(Hash.hash(NEW_CONTRACT_CODE).getBytes()),
            RangeManager.MIN_RANGE);
    state.enqueueRequest(storageReq);
    state.enqueueRequest(codeReq);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(accountExists(localCoordinator, NEW_CONTRACT)).isFalse();
    assertThat(readStorageSlot(localCoordinator, NEW_CONTRACT, SLOT_BASE)).isEmpty();
    // Queued child requests for the doomed contract must have been purged.
    assertThat(state.pendingStorageRequests.asList()).isEmpty();
    assertThat(state.pendingCodeRequests.asList()).isEmpty();
    assertThat(readAccount(localCoordinator, ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void accountNotModifiedInOrphanedButModifiedInNewBlock_appliesCanonicalBal() {
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(80))),
                b.balWithBalances(Map.of(CANONICAL_NEW, Wei.of(50)))),
            2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);
    seedLocal(state, 1, 2);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(readAccount(localCoordinator, CANONICAL_NEW).getBalance())
        .isEqualTo(Wei.of(50)); // created from canonical BAL
    assertThat(readAccount(localCoordinator, ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void accountNotModifiedInOrphanedOrNewBlock_skipsEntirely() {
    final Block block1 =
        b.appendBlockWithBal(
            b.header(0),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(100))),
                b.balWithBalances(Map.of(UNTOUCHED, Wei.of(100)))),
            1L);
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);
    seedLocal(state, 1, 2);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(readAccount(localCoordinator, UNTOUCHED).getBalance()).isEqualTo(Wei.of(100));
    assertThat(accountFetches).hasValue(0); // no account was orphaned-only; nothing to re-fetch
    assertThat(storageFetches).hasValue(0);
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void slotsModifiedInOrphanedButNotNewBlock_reDownloadsSlots() {
    // Block 1: STORAGE_ACCT has balance + SLOT_BASE=7 (base state).
    final var baseBal =
        b.merge(
            b.balWithBalances(Map.of(STORAGE_ACCT, Wei.of(200))),
            b.balWithStorageChanges(STORAGE_ACCT, Map.of(SLOT_BASE, UInt256.valueOf(7))));
    final Block block1 = b.appendBlockWithBal(b.header(0), baseBal, 1L);

    // Block 2s (orphaned): SLOT_BASE=100, SLOT_ORPHAN=200.
    final Block block2s =
        b.appendStale(
            block1.getHeader(),
            b.balWithStorageChanges(
                STORAGE_ACCT,
                Map.of(SLOT_BASE, UInt256.valueOf(100), SLOT_ORPHAN, UInt256.valueOf(200))),
            2L);

    // Pre-seed local with the orphaned state while block2s is still canonical at height 2.
    applyBals(
        localCoordinator,
        1,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    // Block 2c (canonical, higher difficulty): STORAGE_ACCT SLOT_CANONICAL=555 — triggers reorg.
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.balWithStorageChanges(STORAGE_ACCT, Map.of(SLOT_CANONICAL, UInt256.valueOf(555))),
            2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    // initialChildCount=0 → isAccountHashDownloaded(STORAGE_ACCT)=true → computeSlotsToRefetch
    // detects diverged orphaned slots SLOT_BASE and SLOT_ORPHAN.
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);

    // Queue a storage request for STORAGE_ACCT with the (soon-stale) orphaned storage root.
    final Bytes32 oldRoot =
        Bytes32.wrap(readAccount(localCoordinator, STORAGE_ACCT).getStorageRoot().getBytes());
    final SnapV2StorageRangeRequest frankReq =
        new SnapV2StorageRangeRequest(
            block2s.getHeader(),
            Bytes32.wrap(STORAGE_ACCT.addressHash().getBytes()),
            oldRoot,
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE,
            RangeManager.MIN_RANGE);
    state.enqueueRequest(frankReq);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(readStorageSlot(localCoordinator, STORAGE_ACCT, SLOT_BASE))
        .hasValue(UInt256.valueOf(7)); // restored to base value
    assertThat(readStorageSlot(localCoordinator, STORAGE_ACCT, SLOT_ORPHAN))
        .isEmpty(); // orphaned-only slot removed
    assertThat(readStorageSlot(localCoordinator, STORAGE_ACCT, SLOT_CANONICAL))
        .hasValue(UInt256.valueOf(555));
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);

    // Queued request retargeted to new pivot with STORAGE_ACCT's canonical root.
    final var queued = state.pendingStorageRequests.asList();
    assertThat(queued).hasSize(1);
    final SnapV2StorageRangeRequest retargeted = (SnapV2StorageRangeRequest) queued.get(0);
    assertThat(retargeted.getPivotBlockHeader()).isEqualTo(newPivot.getHeader());
    final Bytes32 canonicalFrankRoot =
        Bytes32.wrap(readAccount(localCoordinator, STORAGE_ACCT).getStorageRoot().getBytes());
    assertThat(retargeted.getStorageRoot()).isEqualTo(canonicalFrankRoot);
  }

  @Test
  void pendingRange_appliesToDownloadedSlotsAndDefersRest() {
    // Seed trackers: PENDING range, SLOT_DOWNLOADED only.
    final DownloadedAccountRangeTracker seedAccountTracker = new DownloadedAccountRangeTracker();
    seedAccountTracker.registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 1);
    final DownloadedStorageRangeTracker seedStorageTracker = new DownloadedStorageRangeTracker();
    seedStorageTracker.registerSlotRange(
        Bytes32.wrap(PENDING_ACCT.addressHash().getBytes()),
        Bytes32.wrap(new StorageSlotKey(SLOT_DOWNLOADED).getSlotHash().getBytes()),
        Bytes32.wrap(new StorageSlotKey(SLOT_DOWNLOADED).getSlotHash().getBytes()));

    // Block 1: PENDING_ACCT=100 + SLOT_DOWNLOADED=1.
    final Block block1 =
        b.appendBlockWithBal(
            b.header(0),
            b.merge(
                b.balWithBalances(Map.of(PENDING_ACCT, Wei.of(100))),
                b.balWithStorageChanges(PENDING_ACCT, Map.of(SLOT_DOWNLOADED, UInt256.valueOf(1)))),
            1L);
    applyBals(localCoordinator, 1, 1, seedAccountTracker, seedStorageTracker);
    seedCanonical(1, 1);

    // Block 2: SLOT_DEFERRED added; not in seedStorageTracker so the slot-guard blocks it.
    final Block block2 =
        b.appendBlockWithBal(
            block1.getHeader(),
            b.balWithStorageChanges(PENDING_ACCT, Map.of(SLOT_DEFERRED, UInt256.valueOf(2))),
            2L);
    applyBals(localCoordinator, 2, 2, seedAccountTracker, seedStorageTracker);
    seedCanonical(2, 2);

    // Block 3s (orphaned, low difficulty): rewrites both slots; only SLOT_DOWNLOADED lands locally.
    // appendStale uses LOW difficulty, so it is initially canonical at height 3.
    final Block block3s =
        b.appendStale(
            block2.getHeader(),
            b.balWithStorageChanges(
                PENDING_ACCT,
                Map.of(SLOT_DOWNLOADED, UInt256.valueOf(10), SLOT_DEFERRED, UInt256.valueOf(20))),
            3L);
    applyBals(localCoordinator, 3, 3, seedAccountTracker, seedStorageTracker);
    assertThat(readStorageSlot(localCoordinator, PENDING_ACCT, SLOT_DOWNLOADED))
        .hasValue(UInt256.valueOf(10)); // downloaded, rewritten
    assertThat(readStorageSlot(localCoordinator, PENDING_ACCT, SLOT_DEFERRED))
        .isEmpty(); // not downloaded, still absent

    // Block 3c (canonical, high difficulty): balance only; reorg makes it canonical at height 3.
    final Block block3c =
        b.appendCanonical(
            block2.getHeader(), b.balWithBalances(Map.of(PENDING_ACCT, Wei.of(300))), 3L);
    seedCanonical(3, 3);

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block3c.getHeader(), b.emptyBal(), 4L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block3s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 1);
    state
        .getStorageRangeTracker()
        .registerSlotRange(
            Bytes32.wrap(PENDING_ACCT.addressHash().getBytes()),
            Bytes32.wrap(new StorageSlotKey(SLOT_DOWNLOADED).getSlotHash().getBytes()),
            Bytes32.wrap(new StorageSlotKey(SLOT_DOWNLOADED).getSlotHash().getBytes()));

    // Queue PENDING_ACCT's in-progress storage request with the stale root.
    final Bytes32 oldRoot =
        Bytes32.wrap(readAccount(localCoordinator, PENDING_ACCT).getStorageRoot().getBytes());
    final SnapV2StorageRangeRequest peteReq =
        new SnapV2StorageRangeRequest(
            block3s.getHeader(),
            Bytes32.wrap(PENDING_ACCT.addressHash().getBytes()),
            oldRoot,
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE,
            RangeManager.MIN_RANGE);
    state.enqueueRequest(peteReq);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(readStorageSlot(localCoordinator, PENDING_ACCT, SLOT_DOWNLOADED))
        .hasValue(UInt256.valueOf(1));
    assertThat(readStorageSlot(localCoordinator, PENDING_ACCT, SLOT_DEFERRED)).isEmpty();
    assertThat(readAccount(localCoordinator, PENDING_ACCT).getBalance()).isEqualTo(Wei.of(300));

    // Storage root comes from root-patch, not recomputed from the still-incomplete trie.
    final PmtStateTrieAccountValue canonicalPete =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(
                canonicalCoordinator
                    .applyForStrategy(
                        bonsai -> bonsai.getAccount(PENDING_ACCT.addressHash()),
                        forest -> Optional.<Bytes>empty())
                    .orElseThrow()));
    final SnapV2StorageRangeRequest retargeted =
        (SnapV2StorageRangeRequest) state.pendingStorageRequests.asList().get(0);
    assertThat(retargeted.getPivotBlockHeader()).isEqualTo(newPivot.getHeader());
    assertThat(retargeted.getStorageRoot())
        .isEqualTo(Bytes32.wrap(canonicalPete.getStorageRoot().getBytes()));
  }

  @Test
  void notYetDownloadedRange_defersToLaterCycle() {
    // Verify that ALICE is NOT covered by the degenerate [ZERO, ZERO] range.
    final Bytes32 aliceHash = Bytes32.wrap(ALICE.addressHash().getBytes());
    assertThat(aliceHash).isNotEqualTo(Bytes32.ZERO);

    // Build chain: block1 (shared), block2s (orphaned low), block2c (canonical high).
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    // Seed canonical coordinator only; ALICE is not applied to local (her range is not persisted).
    seedCanonical(1, 2);

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    // Create download state: only [ZERO, ZERO] is persisted; ALICE's hash is NOT covered.
    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, Bytes32.ZERO, 0);
    // ALICE's hash is outside [ZERO, ZERO] → isAccountHashPersisted returns false.
    assertThat(state.getAccountRangeTracker().isAccountHashPersisted(aliceHash)).isFalse();

    // Do NOT apply any local BALs for ALICE — she is not in the persisted range.
    state.startPivotCatchup(newPivot.getHeader());

    // ALICE is not in the persisted range: skipped by planReorg and applyCanonicalBals.
    assertThat(accountExists(localCoordinator, ALICE)).isFalse();
    assertThat(accountFetches).hasValue(0); // no re-download triggered
  }

  @Test
  void sameChainPivotAdvance_appliesBalsAndRetargets_noReorg() {
    final Block block1 =
        b.appendCanonical(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    final Block block2 =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);

    seedCanonical(1, 2);

    // Build and append block3 (ALICE=80) before applying its BAL so the blockchain holds it.
    final Block block3 =
        b.appendCanonical(block2.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 3L);

    seedCanonical(3, 3);

    final SnapV2WorldDownloadState state =
        createDownloadState(
            block2.getHeader(), ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator));
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);
    seedLocal(state, 1, 2);

    // Queue an account request targeted at block2 — it must be retargeted to block3.
    final SnapV2AccountRangeRequest accountReq =
        new SnapV2AccountRangeRequest(
            block2.getHeader(), RangeManager.MIN_RANGE, RangeManager.MAX_RANGE);
    state.enqueueRequest(accountReq);

    state.startPivotCatchup(block3.getHeader());

    assertThat(readAccount(localCoordinator, ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(accountFetches).hasValue(0);
    assertThat(storageFetches).hasValue(0);
    assertThat(codeFetches).hasValue(0);
    final SnapV2AccountRangeRequest retargetedReq =
        (SnapV2AccountRangeRequest) state.pendingAccountRequests.asList().get(0);
    assertThat(retargetedReq.getPivotBlockHeader()).isEqualTo(block3.getHeader());
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator))
        .isEqualTo(ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator));
  }

  @Test
  void unrecoverableReorg_propagatesFailure() {
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    // Orphaned block whose BAL header-hash is committed but the BAL itself is NOT stored.
    final Block block2s =
        b.appendStaleWithoutStoringBal(
            block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);
    // Apply only block1 locally; block2s was never applied (simulates partial download).
    seedLocal(state, 1, 1);

    // Orphaned BAL is unreadable → startPivotCatchup completes the download future exceptionally.
    state.startPivotCatchup(newPivot.getHeader());

    assertThat(state.getDownloadFuture()).isCompletedExceptionally();
    assertThatThrownBy(() -> state.getDownloadFuture().join())
        .hasRootCauseInstanceOf(ReorgUnrecoverableException.class);
  }

  @Test
  void multiBlockReorg_recoversAcrossSeveralOrphanedAndCanonicalBlocks() {
    final Block block1 =
        b.appendBlockWithBal(
            b.header(0),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(100))),
                b.balWithBalances(Map.of(ORPHAN_EOA, Wei.of(75)))),
            1L);

    // Orphaned fork: two blocks.
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block3s =
        b.appendStale(block2s.getHeader(), b.balWithBalances(Map.of(ORPHAN_EOA, Wei.of(60))), 3L);

    // Canonical fork: two blocks + pivot pinner.
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);
    final Block block3c =
        b.appendCanonical(
            block2c.getHeader(), b.balWithBalances(Map.of(CANONICAL_NEW, Wei.of(50))), 3L);

    seedCanonical(1, 3);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block3c.getHeader(), b.emptyBal(), 4L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block3s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);
    seedLocal(state, 1, 3);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(readAccount(localCoordinator, ALICE).getBalance())
        .isEqualTo(Wei.of(80)); // touched on both forks: canonical BAL applied directly
    assertThat(readAccount(localCoordinator, ORPHAN_EOA).getBalance())
        .isEqualTo(Wei.of(75)); // touched only on orphaned fork: re-fetched from canonical network
    assertThat(readAccount(localCoordinator, CANONICAL_NEW).getBalance())
        .isEqualTo(Wei.of(50)); // touched only on canonical fork: created from canonical BAL
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void queuedStorageRequestForUnaffectedAccount_retargetedKeepingExistingRoot() {
    final var baseBal =
        b.merge(
            b.balWithBalances(Map.of(STORAGE_ACCT, Wei.of(200), CAROL, Wei.of(10))),
            b.balWithStorageChanges(STORAGE_ACCT, Map.of(SLOT_BASE, UInt256.valueOf(7))),
            b.balWithStorageChanges(CAROL, Map.of(SLOT_BASE, UInt256.valueOf(9))));
    final Block block1 = b.appendBlockWithBal(b.header(0), baseBal, 1L);
    final Block block2s =
        b.appendStale(
            block1.getHeader(),
            b.balWithStorageChanges(STORAGE_ACCT, Map.of(SLOT_ORPHAN, UInt256.valueOf(200))),
            2L);
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.balWithStorageChanges(STORAGE_ACCT, Map.of(SLOT_CANONICAL, UInt256.valueOf(555))),
            2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);
    seedLocal(state, 1, 2);

    // Read CAROL's storage root from the local DB after seeding (she is untouched by both forks).
    final Bytes32 carolRoot =
        Bytes32.wrap(readAccount(localCoordinator, CAROL).getStorageRoot().getBytes());

    final SnapV2StorageRangeRequest carolReq =
        new SnapV2StorageRangeRequest(
            block2s.getHeader(),
            Bytes32.wrap(CAROL.addressHash().getBytes()),
            carolRoot,
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE,
            RangeManager.MIN_RANGE);
    state.enqueueRequest(carolReq);

    state.startPivotCatchup(newPivot.getHeader());

    final var queued = state.pendingStorageRequests.asList();
    assertThat(queued).hasSize(1);
    final SnapV2StorageRangeRequest retargeted = (SnapV2StorageRangeRequest) queued.get(0);
    assertThat(retargeted.getPivotBlockHeader()).isEqualTo(newPivot.getHeader());
    // CAROL is absent from correctedRoots — storage root must be preserved as-is.
    assertThat(retargeted.getStorageRoot()).isEqualTo(carolRoot);
  }

  @Test
  void queuedCodeAndAccountRequests_retargetedToNewPivot() {
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);
    seedLocal(state, 1, 2);

    final SnapV2BytecodeRequest codeReq =
        new SnapV2BytecodeRequest(
            block2s.getHeader(),
            Bytes32.wrap(ALICE.addressHash().getBytes()),
            Bytes32.wrap(Hash.hash(NEW_CONTRACT_CODE).getBytes()),
            RangeManager.MIN_RANGE);
    final SnapV2AccountRangeRequest accountReq =
        new SnapV2AccountRangeRequest(
            block2s.getHeader(), RangeManager.MIN_RANGE, RangeManager.MAX_RANGE);
    state.enqueueRequest(codeReq);
    state.enqueueRequest(accountReq);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(readAccount(localCoordinator, ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(
            ((SnapV2BytecodeRequest) state.pendingCodeRequests.asList().get(0))
                .getPivotBlockHeader())
        .isEqualTo(newPivot.getHeader());
    assertThat(
            ((SnapV2AccountRangeRequest) state.pendingAccountRequests.asList().get(0))
                .getPivotBlockHeader())
        .isEqualTo(newPivot.getHeader());
  }

  @Test
  void codeModifiedInOrphanedButNotNewBlock_fetchesCanonicalCode() {
    final Bytes carolCodeCanonical = Bytes.fromHexString("0x6080604052348015600f");

    // Block 1: CAROL gets balance + carolCodeCanonical.
    final Block block1 =
        b.appendBlockWithBal(
            b.header(0),
            b.merge(
                b.balWithBalances(Map.of(CAROL, Wei.of(100))),
                b.balWithCodeChange(CAROL, carolCodeCanonical)),
            1L);

    // Block 2s (orphaned, low difficulty): CAROL nonce changes → she's in orphaned touches.
    final Block block2s = b.appendStale(block1.getHeader(), b.balWithNonceChange(CAROL, 5L), 2L);

    // Block 2c (canonical, high difficulty): empty BAL → CAROL absent from canonical touches.
    final Block block2c = b.appendCanonical(block1.getHeader(), b.emptyBal(), 2L);

    seedCanonical(1, 2);

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);

    // Apply block 1 to local: stores CAROL's account record + carolCodeCanonical bytes.
    seedLocal(state, 1, 1);

    // removeFlatCode() is a no-op on CodeHashCodeStorageStrategy; delete directly.
    final Hash carolCodeHash = Hash.hash(carolCodeCanonical);
    final var codeTx = localStorage.getComposedWorldStateStorage().startTransaction();
    codeTx.remove(KeyValueSegmentIdentifier.CODE_STORAGE, carolCodeHash.getBytes().toArrayUnsafe());
    codeTx.commit();

    // Apply block 2 (canonical block2c has empty BAL — no-op for CAROL locally).
    seedLocal(state, 2, 2);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(readCode(localCoordinator, CAROL)).hasValue(carolCodeCanonical);
    assertThat(codeFetches.get()).isGreaterThanOrEqualTo(1);
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void slotsModifiedInOrphanedAndNewBlock_appliesCanonicalSlotWithoutRefetch() {
    final Block block1 =
        b.appendBlockWithBal(
            b.header(0),
            b.merge(
                b.balWithBalances(Map.of(STORAGE_ACCT, Wei.of(200))),
                b.balWithStorageChanges(STORAGE_ACCT, Map.of(SLOT_BASE, UInt256.valueOf(7)))),
            1L);
    final Block block2s =
        b.appendStale(
            block1.getHeader(),
            b.balWithStorageChanges(STORAGE_ACCT, Map.of(SLOT_BASE, UInt256.valueOf(100))),
            2L);

    // Apply orphaned BALs locally while block2s is still canonical.
    applyBals(
        localCoordinator,
        1,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    assertThat(readStorageSlot(localCoordinator, STORAGE_ACCT, SLOT_BASE))
        .hasValue(UInt256.valueOf(100));

    // Block 2c (canonical): also modifies SLOT_BASE — same slot as the orphaned fork.
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.balWithStorageChanges(STORAGE_ACCT, Map.of(SLOT_BASE, UInt256.valueOf(999))),
            2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(readStorageSlot(localCoordinator, STORAGE_ACCT, SLOT_BASE))
        .hasValue(UInt256.valueOf(999));
    assertThat(accountFetches).hasValue(0);
    assertThat(storageFetches).hasValue(0);
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void slotsNotModifiedInOrphanedButModifiedInNewBlock_appliesCanonicalSlot() {
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);

    // Apply orphaned BALs locally while block2s is still canonical.
    applyBals(
        localCoordinator,
        1,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    assertThat(accountExists(localCoordinator, STORAGE_ACCT)).isFalse();

    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(80))),
                b.balWithBalances(Map.of(STORAGE_ACCT, Wei.of(100))),
                b.balWithStorageChanges(STORAGE_ACCT, Map.of(SLOT_CANONICAL, UInt256.valueOf(42)))),
            2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(accountExists(localCoordinator, STORAGE_ACCT)).isTrue();
    assertThat(readStorageSlot(localCoordinator, STORAGE_ACCT, SLOT_CANONICAL))
        .hasValue(UInt256.valueOf(42));
    assertThat(accountFetches).hasValue(0);
    assertThat(storageFetches).hasValue(0);
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void codeModifiedInOrphanedAndNewBlock_appliesCanonicalCodeWithoutFetch() {
    final Bytes codeV1 = Bytes.fromHexString("0xdeadbeef");
    final Bytes codeV2 = Bytes.fromHexString("0xcafebabe");

    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(CAROL, Wei.of(100))), 1L);
    final Block block2s = b.appendStale(block1.getHeader(), b.balWithCodeChange(CAROL, codeV1), 2L);

    // Apply orphaned BALs locally while block2s is still canonical.
    applyBals(
        localCoordinator,
        1,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    assertThat(readCode(localCoordinator, CAROL)).hasValue(codeV1);

    // Block 2c (canonical): deploys different code to the same contract.
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithCodeChange(CAROL, codeV2), 2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);

    state.startPivotCatchup(newPivot.getHeader());

    // codeV2 is written by the canonical BAL, not fetched from the network.
    assertThat(readCode(localCoordinator, CAROL)).hasValue(codeV2);
    assertThat(accountFetches).hasValue(0);
    assertThat(codeFetches).hasValue(0);
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  @Test
  void codeNotModifiedInOrphanedButModifiedInNewBlock_appliesCanonicalCode() {
    final Bytes canonicalCode = Bytes.fromHexString("0x60806040");

    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);

    // Apply orphaned BALs locally while block2s is still canonical.
    applyBals(
        localCoordinator,
        1,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    assertThat(accountExists(localCoordinator, NEW_CONTRACT)).isFalse();

    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(80))),
                b.balWithBalances(Map.of(NEW_CONTRACT, Wei.ONE)),
                b.balWithCodeChange(NEW_CONTRACT, canonicalCode)),
            2L);

    seedCanonical(1, 2);
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);

    state.startPivotCatchup(newPivot.getHeader());

    assertThat(accountExists(localCoordinator, NEW_CONTRACT)).isTrue();
    assertThat(readCode(localCoordinator, NEW_CONTRACT)).hasValue(canonicalCode);
    assertThat(accountFetches).hasValue(0);
    assertThat(codeFetches).hasValue(0);
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  /**
   * Builds a real {@link SnapV2WorldDownloadState} with the stale block as its starting pivot. The
   * healer serves from the canonical world state; the pivot-catchup listener returns an immediately
   * completed future (the blockchain already holds every block and BAL, so there is no chain gap).
   */
  private SnapV2WorldDownloadState createDownloadState(
      final BlockHeader stalePivot, final Hash canonicalRoot) {
    final DefaultBlockchain blockchain = b.blockchain();
    final SnapV2ReorgStateFetcher fetcher =
        ReorgBlockchainBuilder.servingFetcher(
            canonicalStorage,
            canonicalRoot,
            localCoordinator,
            accountFetches,
            storageFetches,
            codeFetches);
    final SnapV2ReorgHealer healer =
        new SnapV2ReorgHealer(
            blockchain, localCoordinator, ReorgBlockchainBuilder.balEnabledSchedule(), fetcher);
    return new SnapV2WorldDownloadState(
        localCoordinator,
        new SnapSyncStatePersistenceManager(new InMemoryKeyValueStorageProvider()),
        new SnapSyncProcessState(stalePivot),
        new InMemoryTasksPriorityQueues<>(),
        10,
        50_000L,
        new SnapSyncMetricsManager(new NoOpMetricsSystem(), ethContext),
        Clock.systemUTC(),
        SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS,
        null,
        (current, next) -> CompletableFuture.completedFuture(null),
        new SnapV2BlockAccessListApplier(
            localCoordinator, blockchain, ReorgBlockchainBuilder.balEnabledSchedule()),
        healer,
        blockchain,
        ethContext,
        1000L);
  }

  private void seedCanonical(final long from, final long to) {
    applyBals(
        canonicalCoordinator,
        from,
        to,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
  }

  private void seedLocal(final SnapV2WorldDownloadState state, final long from, final long to) {
    applyBals(
        localCoordinator, from, to, state.getAccountRangeTracker(), state.getStorageRangeTracker());
  }

  private void applyBals(
      final WorldStateStorageCoordinator coordinator,
      final long fromBlock,
      final long toBlock,
      final DownloadedAccountRangeTracker accountTracker,
      final DownloadedStorageRangeTracker storageTracker) {

    new SnapV2BlockAccessListApplier(
            coordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule())
        .applyBlockAccessLists(fromBlock, toBlock, accountTracker, storageTracker)
        .commit();
  }
}
