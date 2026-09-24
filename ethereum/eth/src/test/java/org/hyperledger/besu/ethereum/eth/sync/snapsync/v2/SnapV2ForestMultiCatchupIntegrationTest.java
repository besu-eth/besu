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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;

import java.util.Map;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/**
 * Integration test verifying Forest account-trie root-pointer threading across multiple sequential
 * pivot catch-ups. This is the Phase 2 acceptance gate.
 *
 * <p>The applier always reads the persisted account-trie root pointer from storage. Every seed call
 * and every batch commit atomically update the stored pointer, so the applier always opens the trie
 * at the correct hybrid root — even across multiple catch-ups and simulated restarts.
 */
class SnapV2ForestMultiCatchupIntegrationTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address BOB =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address CHARLIE =
      Address.fromHexString("0x3333333333333333333333333333333333333333");

  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  /**
   * Drives the Forest sync harness through two sequential catch-ups separated by an additional
   * download phase.
   *
   * <pre>
   * genesis → block1 (ALICE 100→150) → block2 (BOB 200→220)
   *
   * Sequence:
   *   1. Download [ALICE=100, BOB=200]  → persisted pointer R1
   *   2. Catch-up A→B (block1)         → tracked root R2; applier persists
   *   3. Download [CHARLIE=300]         → persisted pointer R3
   *   4. Catch-up B→C (block2)         → tracked root R4; applier persists
   *   5. Assert: ALICE=150, BOB=220, CHARLIE=300 readable; R4 = reference trie root
   * </pre>
   */
  @Test
  void rootThreadedCorrectlyAcrossSequentialCatchups() {
    // Build a 2-block canonical chain.
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(150))), 1L); // A→B
    b.appendBlockWithBal(
        block1.getHeader(), b.balWithBalances(Map.of(BOB, Wei.of(220))), 2L); // B→C

    final ForestWorldStateStorageHarness h = new ForestWorldStateStorageHarness();
    final SnapV2BlockAccessListApplier applier =
        new SnapV2BlockAccessListApplier(
            h.coordinator(), b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());

    // --- Phase 1: download [ALICE=100, BOB=200] at pivot A ---
    h.seedAccount(ALICE, 0L, Wei.of(100), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    h.seedAccount(BOB, 0L, Wei.of(200), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final Bytes32 rootAfterDownload1 = h.commitAndGetAccountRoot();

    // --- Catch-up A→B (block1: ALICE 100→150) ---
    applier.applyBlockAccessLists(1L, 1L, fullRange(), emptyStorage()).commit();
    final Bytes32 rootAfterCatchup1 = h.commitAndGetAccountRoot();

    // Applier must have persisted the pointer.
    assertThat(h.forestStorage().getWorldStateRoot()).contains(rootAfterCatchup1);
    // Root must have moved — otherwise the assertions below are vacuous.
    assertThat(rootAfterCatchup1).isNotEqualTo(rootAfterDownload1);
    assertThat(h.readAccount(ALICE).orElseThrow().getBalance()).isEqualTo(Wei.of(150));
    assertThat(h.readAccount(BOB).orElseThrow().getBalance()).isEqualTo(Wei.of(200));

    // --- Phase 2: download [CHARLIE=300] at pivot B (on top of the hybrid trie at R2) ---
    h.seedAccount(CHARLIE, 0L, Wei.of(300), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final Bytes32 rootAfterDownload2 = h.commitAndGetAccountRoot();

    // --- Catch-up B→C (block2: BOB 200→220) ---
    // The applier reads the persisted pointer (rootAfterDownload2, which includes CHARLIE) and
    // opens at the correct hybrid root; all three accounts are present in the final trie.
    applier.applyBlockAccessLists(2L, 2L, fullRange(), emptyStorage()).commit();
    final Bytes32 rootAfterCatchup2 = h.commitAndGetAccountRoot();

    assertThat(h.forestStorage().getWorldStateRoot()).contains(rootAfterCatchup2);
    assertThat(rootAfterCatchup2).isNotEqualTo(rootAfterDownload2);

    // All three accounts readable at their correct post-catch-up values.
    assertThat(h.readAccount(ALICE).orElseThrow().getBalance()).isEqualTo(Wei.of(150));
    assertThat(h.readAccount(BOB).orElseThrow().getBalance()).isEqualTo(Wei.of(220));
    assertThat(h.readAccount(CHARLIE).orElseThrow().getBalance()).isEqualTo(Wei.of(300));

    // Cross-check: tracked root matches a reference trie seeded with the final canonical values.
    assertThat(rootAfterCatchup2)
        .isEqualTo(referenceTrie(ALICE, Wei.of(150), BOB, Wei.of(220), CHARLIE, Wei.of(300)));
  }

  /**
   * Verifies root threading survives a reorg mid-sequence: orphaned BAL applied, then canonical BAL
   * applied via a second catch-up over the same block range. The second application must open at
   * the correct hybrid root left by the first.
   *
   * <pre>
   * genesis → block1_stale  (ALICE 100→180) [orphaned]
   *         → block1_canon  (ALICE 100→150) [canonical, wins]
   *         → block2_canon  (BOB  200→220)
   *
   * Sequence:
   *   1. Download [ALICE=100, BOB=200]   → persisted pointer R1
   *   2. Catch-up 1: apply block1_stale  → tracked root R2_stale (ALICE=180)
   *   3. Catch-up 2: apply block1_canon  → tracked root R2_canon (ALICE=150)
   *   4. Download [CHARLIE=300]          → persisted pointer R3
   *   5. Catch-up 3: apply block2_canon  → tracked root R4 (BOB=220)
   *   6. Assert: ALICE=150, BOB=220, CHARLIE=300 readable
   * </pre>
   */
  @Test
  void rootThreadedCorrectlyAfterReorgCatchupSequence() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

    // Block 1: orphaned fork (ALICE→180) then canonical (ALICE→150) — canonical wins by difficulty.
    b.appendStale(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(180))), 1L);
    final Block block1Canon =
        b.appendCanonical(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(150))), 1L);

    // Block 2 on top of canonical block 1 (BOB→220).
    b.appendBlockWithBal(block1Canon.getHeader(), b.balWithBalances(Map.of(BOB, Wei.of(220))), 2L);

    final ForestWorldStateStorageHarness h = new ForestWorldStateStorageHarness();
    final SnapV2BlockAccessListApplier applier =
        new SnapV2BlockAccessListApplier(
            h.coordinator(), b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());

    // --- Phase 1: download [ALICE=100, BOB=200] ---
    h.seedAccount(ALICE, 0L, Wei.of(100), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    h.seedAccount(BOB, 0L, Wei.of(200), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    // --- Catch-up 1: apply the stale block's BAL (ALICE→180) ---
    // This simulates snap/2 having applied the orphaned fork before the reorg was detected.
    // In practice the reorg healer calls applyBlockAccessLists for the canonical chain, but
    // here we test that the pointer survives a catch-up that produces a stale hybrid root.
    // We build a mini-blockchain that has block1_stale as its canonical head for this step.
    final ReorgBlockchainBuilder staleChain = new ReorgBlockchainBuilder();
    staleChain.appendBlockWithBal(
        staleChain.header(0), staleChain.balWithBalances(Map.of(ALICE, Wei.of(180))), 1L);
    final SnapV2BlockAccessListApplier staleApplier =
        new SnapV2BlockAccessListApplier(
            h.coordinator(), staleChain.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());
    staleApplier.applyBlockAccessLists(1L, 1L, fullRange(), emptyStorage()).commit();
    assertThat(h.readAccount(ALICE).orElseThrow().getBalance()).isEqualTo(Wei.of(180));

    // --- Catch-up 2: reorg detected — apply block1_canonical BAL (ALICE→150) ---
    // The applier reads the stored pointer and patches on top of it.
    applier.applyBlockAccessLists(1L, 1L, fullRange(), emptyStorage()).commit();
    assertThat(h.readAccount(ALICE).orElseThrow().getBalance()).isEqualTo(Wei.of(150));

    // --- Phase 2: download [CHARLIE=300] at pivot B ---
    h.seedAccount(CHARLIE, 0L, Wei.of(300), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    // --- Catch-up 3: apply block2_canon BAL (BOB→220) ---
    // The applier reads the stored pointer (which includes CHARLIE) and patches on top.
    applier.applyBlockAccessLists(2L, 2L, fullRange(), emptyStorage()).commit();
    final Bytes32 rootFinal = h.commitAndGetAccountRoot();

    assertThat(h.forestStorage().getWorldStateRoot()).contains(rootFinal);
    assertThat(h.readAccount(ALICE).orElseThrow().getBalance()).isEqualTo(Wei.of(150));
    assertThat(h.readAccount(BOB).orElseThrow().getBalance()).isEqualTo(Wei.of(220));
    assertThat(h.readAccount(CHARLIE).orElseThrow().getBalance()).isEqualTo(Wei.of(300));
    assertThat(rootFinal)
        .isEqualTo(referenceTrie(ALICE, Wei.of(150), BOB, Wei.of(220), CHARLIE, Wei.of(300)));
  }

  /**
   * Simulates a Forest snap/2 session restart: accounts are downloaded and the pointer persisted in
   * one "session," then a brand-new applier (same storage, fresh object) is created and used for
   * the catch-up in the next "session." The new applier must read the stored pointer rather than
   * relying on any caller-supplied value.
   *
   * <p>This is the Task 8 acceptance test. The new applier has no in-memory state from Session 1,
   * yet it correctly reads the persisted pointer and applies the BAL on top of the downloaded trie.
   */
  @Test
  void newApplierReadsPersistedPointerAfterRestart() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(150))), 1L);

    final ForestWorldStateStorageHarness h = new ForestWorldStateStorageHarness();

    // --- "Session 1": download [ALICE=100] ---
    h.seedAccount(ALICE, 0L, Wei.of(100), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final Bytes32 rootAfterDownload = h.commitAndGetAccountRoot();

    // --- "Session 2": create a NEW applier with the SAME coordinator ---
    // This simulates a node restart where the storage is reloaded but all in-memory state is gone.
    final SnapV2BlockAccessListApplier newSessionApplier =
        new SnapV2BlockAccessListApplier(
            h.coordinator(), b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());

    newSessionApplier.applyBlockAccessLists(1L, 1L, fullRange(), emptyStorage()).commit();
    final Bytes32 finalRoot = h.commitAndGetAccountRoot();

    // The catch-up applied correctly: ALICE balance is 150, pointer is updated.
    assertThat(h.forestStorage().getWorldStateRoot()).contains(finalRoot);
    assertThat(finalRoot).isNotEqualTo(rootAfterDownload);
    assertThat(h.readAccount(ALICE).orElseThrow().getBalance()).isEqualTo(Wei.of(150));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static DownloadedAccountRangeTracker fullRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }

  private static DownloadedStorageRangeTracker emptyStorage() {
    return new DownloadedStorageRangeTracker();
  }

  /**
   * Builds a fresh Forest harness seeded with the given address→balance pairs (zero nonce, empty
   * storage and code) and returns the resulting account-trie root. Used to derive a reference root
   * without relying on the state built up by the test sequence.
   */
  private static Bytes32 referenceTrie(
      final Address a1,
      final Wei b1,
      final Address a2,
      final Wei b2,
      final Address a3,
      final Wei b3) {
    final ForestWorldStateStorageHarness ref = new ForestWorldStateStorageHarness();
    ref.seedAccount(a1, 0L, b1, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    ref.seedAccount(a2, 0L, b2, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    ref.seedAccount(a3, 0L, b3, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    return ref.commitAndGetAccountRoot();
  }
}
