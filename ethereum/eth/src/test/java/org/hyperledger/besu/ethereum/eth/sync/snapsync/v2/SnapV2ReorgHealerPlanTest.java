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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link SnapV2ReorgHealer#planReorg}, covering every reorg divergence category (accounts
 * and storage slots), downloaded-range scoping, boundary conditions on the ancestor walk, error
 * paths, touch-type sensitivity, and the pending-vs-completed tracker lifecycle — all against a
 * real reorging {@link org.hyperledger.besu.ethereum.chain.DefaultBlockchain} over in-memory
 * storage.
 */
class SnapV2ReorgHealerPlanTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address BOB =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address CHARLIE =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address DAVE =
      Address.fromHexString("0x4444444444444444444444444444444444444444");
  private static final Address EVE =
      Address.fromHexString("0x5555555555555555555555555555555555555555");
  private static final Address FRANK =
      Address.fromHexString("0x6666666666666666666666666666666666666666");
  private static final Address GRACE =
      Address.fromHexString("0x7777777777777777777777777777777777777777");
  private static final Address NEW_CONTRACT =
      Address.fromHexString("0x9999999999999999999999999999999999999999");

  private final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  // ---------------------------------------------------------------------------
  // Comprehensive reorg scenarios.
  // ---------------------------------------------------------------------------

  /**
   * Full multi-block reorg mixing balance and slot changes across both forks.
   *
   * <pre>
   *            2s (A=50,B=30) -- 3s (D=60,E=20,NC=1) -- 4s (F:s1,s2; G:s5)   stale, diff 10
   *           /
   * gen -- 1 +
   *           \
   *            2c (A=80,C=100) -- 3c (D=40) -- 4c (F:s1,s3; G balance)       canonical, diff 100
   * </pre>
   *
   * Diverged accounts: B, E, NC (stale-only). Diverged slots: F.s2 and G.s5 (stale-only; F.s1
   * overlaps, F.s3 is canonical-only).
   */
  @Test
  void multiBlockReorgWithSlotsAndBalances() {
    // Common ancestor at block 1.
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    // ---- Stale chain ----
    // Block 2s: Alice=50, Bob=30  (Bob only in stale -> diverged)
    final BlockAccessList block2sBal =
        b.balWithBalances(Map.of(ALICE, Wei.of(50), BOB, Wei.of(30)));
    final Block block2s = b.appendStale(ancestor.getHeader(), block2sBal, 2L);

    // Block 3s: Dave=60, Eve=20, NewContract deployed (Eve and NewContract only in stale ->
    // diverged)
    final BlockAccessList block3sBal =
        b.balWithBalances(Map.of(DAVE, Wei.of(60), EVE, Wei.of(20), NEW_CONTRACT, Wei.of(1)));
    final Block block3s = b.appendStale(block2s.getHeader(), block3sBal, 3L);

    // Block 4s: Frank writes slots 1=100 and 2=200, Grace writes slot 5=333, both balanced.
    final UInt256 s1 = UInt256.valueOf(1);
    final UInt256 s2 = UInt256.valueOf(2);
    final UInt256 s3 = UInt256.valueOf(3);
    final UInt256 s5 = UInt256.valueOf(5);

    final BlockAccessList block4sBal =
        b.merge(
            b.balWithStorageChanges(
                FRANK, Map.of(s1, UInt256.valueOf(100), s2, UInt256.valueOf(200))),
            b.balWithStorageChanges(GRACE, Map.of(s5, UInt256.valueOf(333))),
            b.balWithBalanceTouches(FRANK, GRACE));
    final Block block4s = b.appendStale(block3s.getHeader(), block4sBal, 4L);

    // ---- Canonical chain (wins the reorg when block2c is appended) ----
    // Block 2c: Alice=80, Charlie=100 (Charlie is canonical-only -> applied if persisted)
    final BlockAccessList block2cBal =
        b.balWithBalances(Map.of(ALICE, Wei.of(80), CHARLIE, Wei.of(100)));
    final Block block2c = b.appendCanonical(ancestor.getHeader(), block2cBal, 2L);

    // Block 3c: Dave=40 (overlaps with stale 3s — Eve and NewContract are absent -> diverged)
    final Block block3c =
        b.appendCanonical(block2c.getHeader(), b.balWithBalances(Map.of(DAVE, Wei.of(40))), 3L);

    // Block 4c: Frank writes slots 1=111 and 3=555 (slot 2 absent -> diverged), Grace has only a
    // balance (slot 5 absent -> diverged).
    final BlockAccessList block4cBal =
        b.merge(
            b.balWithStorageChanges(
                FRANK, Map.of(s1, UInt256.valueOf(111), s3, UInt256.valueOf(555))),
            b.balWithBalanceTouches(FRANK, GRACE));
    final Block block4c = b.appendCanonical(block3c.getHeader(), block4cBal, 4L);

    // The reorg replaced the stale chain; verify canonical status.
    assertThat(b.blockchain().blockIsOnCanonicalChain(block4s.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(block4c.getHash())).isTrue();

    final ReorgPlan plan = plan(block4s, block4c, fullAccountRange(), fullStorageFor(FRANK, GRACE));

    assertThat(plan.commonAncestor().getNumber()).isEqualTo(1L);
    assertThat(plan.fromBlock()).isEqualTo(2L);
    assertThat(plan.toBlock()).isEqualTo(4L);

    // --- Diverged accounts ---
    // Bob: stale 2s only
    // Eve: stale 3s only
    // NewContract: stale 3s only (would-be contract deployment)
    assertDivergedAccounts(plan, BOB, EVE, NEW_CONTRACT);

    // Alice, Charlie, Dave, Frank, Grace — none of these are diverged.
    assertNoDivergedAccounts(plan, ALICE, CHARLIE, DAVE, FRANK, GRACE);

    // --- Diverged slots ---
    // Frank's slot 2 (100->200 in stale 4s, absent from canonical 4c).
    assertDivergedSlots(plan, FRANK, s2);
    assertNoDivergedSlots(plan, FRANK, s1, s3);

    // Grace's slot 5 (333 in stale 4s, absent from canonical 4c — Grace has a balance touch in
    // canonical 4c so the account overlaps, but the slot is diverged).
    assertDivergedSlots(plan, GRACE, s5);

    assertThat(plan.isClean()).isFalse();
  }

  /**
   * One-block reorg exercising every divergence category at once.
   *
   * <pre>
   * gen -- 1 +-- 2s (A,C,D,F balances; NC:s1,s2)   stale, diff 10
   *          +-- 2c (A,C,F,G balances)             canonical, diff 20
   * </pre>
   *
   * Diverged: D's account (balance) and NC's slots (s1,s2) -- stale-only. Overlapping (A,C,F),
   * canonical-only (G) and untouched (B,E) accounts are not diverged.
   */
  @Test
  void identifiesDivergedEntries() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot1 = UInt256.valueOf(1);
    final UInt256 slot2 = UInt256.valueOf(2);

    // Stale block (now orphaned): touches Alice, Charlie, Dave, Frank + NewContract's slots.
    final BlockAccessList staleBal =
        b.merge(
            b.balWithStorageChanges(
                NEW_CONTRACT, Map.of(slot1, UInt256.valueOf(100), slot2, UInt256.valueOf(200))),
            b.balWithBalanceTouches(ALICE, CHARLIE, DAVE, FRANK));
    final Block staleBlock = b.appendStale(ancestor.getHeader(), staleBal, 2L);

    // Canonical block: touches Alice, Charlie, Frank, Grace. Dave and NewContract are untouched.
    final Block canonicalBlock =
        b.appendCanonical(
            ancestor.getHeader(), b.balWithBalanceTouches(ALICE, CHARLIE, FRANK, GRACE), 2L);

    assertThat(b.blockchain().blockIsOnCanonicalChain(staleBlock.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(canonicalBlock.getHash())).isTrue();

    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, fullAccountRange(), fullStorageFor(NEW_CONTRACT));

    assertThat(plan.commonAncestor().getNumber()).isEqualTo(1L);
    assertThat(plan.fromBlock()).isEqualTo(2L);
    assertThat(plan.toBlock()).isEqualTo(2L);

    // Dave was touched only in the stale block (balance change) -> diverged account.
    assertDivergedAccounts(plan, DAVE);
    // NewContract changed only storage on the stale block: no scalar field diverged, so its
    // account record needs no re-fetch — the diverged slots below suffice. Alice, Charlie, and
    // Frank overlap on both forks; Grace is canonical-only; Bob and Eve are untouched.
    assertNoDivergedAccounts(plan, NEW_CONTRACT, ALICE, CHARLIE, FRANK, GRACE, BOB, EVE);

    // NewContract's slots were touched only in the stale block -> diverged.
    assertThat(plan.divergedSlotsByAccount()).containsOnlyKeys(NEW_CONTRACT.addressHash());
    assertDivergedSlots(plan, NEW_CONTRACT, slot1, slot2);

    assertThat(plan.isClean()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Core divergence behaviour: account- and slot-level divergence combinations.
  // ---------------------------------------------------------------------------

  /**
   * An account touched only on the orphaned fork is reported together with its orphaned-only slots.
   *
   * <pre>
   * gen -- 1 +-- 2s (NC balance + NC:s5)   stale
   *          +-- 2c (A balance)            canonical
   * </pre>
   *
   * Diverged: account NC and slot NC.s5.
   */
  @Test
  void divergedAccountAndItsSlotsBothReported() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot5 = UInt256.valueOf(5);

    // Stale: NewContract has BOTH a balance change AND a storage write to slot 5.
    final BlockAccessList staleBal =
        b.merge(
            b.balWithStorageChanges(NEW_CONTRACT, Map.of(slot5, UInt256.valueOf(99))),
            b.balWithBalanceTouches(NEW_CONTRACT));
    final Block staleBlock = b.appendStale(ancestor.getHeader(), staleBal, 2L);

    // Canonical: NewContract is untouched (only Alice is touched).
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, fullAccountRange(), fullStorageFor(NEW_CONTRACT));

    // NewContract was touched only on the orphaned fork -> account is diverged.
    assertDivergedAccounts(plan, NEW_CONTRACT);
    // Its slot 5 was also touched only on the orphaned fork -> slot is diverged too.
    assertDivergedSlots(plan, NEW_CONTRACT, slot5);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * Slot-level divergence survives account-level overlap.
   *
   * <pre>
   * gen -- 1 +-- 2s (NC:s5; A balance)   stale
   *          +-- 2c (NC, A balances)     canonical
   * </pre>
   *
   * NC is touched on both forks, so the account is not diverged, but NC.s5 is stale-only, which is
   * enough to make the plan non-clean.
   */
  @Test
  void divergedSlotWhenAccountOverlapsButSlotDoesNot() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slotOnlyInStale = UInt256.valueOf(5);

    // Stale: NewContract has a storage write to slot 5, plus Alice has a balance change.
    final BlockAccessList staleBal =
        b.merge(
            b.balWithStorageChanges(NEW_CONTRACT, Map.of(slotOnlyInStale, UInt256.valueOf(7))),
            b.balWithBalanceTouches(ALICE));
    final Block staleBlock = b.appendStale(ancestor.getHeader(), staleBal, 2L);

    // Canonical: NewContract and Alice are both touched, but slot 5 is absent.
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(NEW_CONTRACT, ALICE), 2L);

    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, fullAccountRange(), fullStorageFor(NEW_CONTRACT));

    // NewContract is touched on both forks -> its account hash is not diverged.
    assertNoDivergedAccounts(plan, NEW_CONTRACT, ALICE);
    // Slot 5 was touched only in the stale fork -> diverged at the slot level.
    assertDivergedSlots(plan, NEW_CONTRACT, slotOnlyInStale);
    // Slot divergence alone is enough to make the plan non-clean.
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * An account touched on the orphaned fork with storage-only changes needs no account re-fetch:
   * its nonce/balance/code are untouched, and repairing its stale slots restores the storage root.
   *
   * <pre>
   * gen -- 1 +-- 2s (NC:s5)       stale
   *          +-- 2c (A balance)   canonical
   * </pre>
   */
  @Test
  void storageOnlyStaleAccountNeedsNoAccountRefetch() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot5 = UInt256.valueOf(5);

    // Stale: NewContract writes slot 5 only. Canonical: NewContract is untouched.
    final Block staleBlock =
        b.appendStale(
            ancestor.getHeader(),
            b.balWithStorageChanges(NEW_CONTRACT, Map.of(slot5, UInt256.valueOf(99))),
            2L);
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, fullAccountRange(), fullStorageFor(NEW_CONTRACT));

    // No scalar field changed, so the account record itself is intact...
    assertNoDivergedAccounts(plan, NEW_CONTRACT);
    // ...but its stale slot must be repaired, which also restores the storage root.
    assertDivergedSlots(plan, NEW_CONTRACT, slot5);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * Identical touch sets on both forks produce a clean plan (nothing stale-only to re-fetch).
   *
   * <pre>
   * gen -- 1 +-- 2s (A, B)   stale
   *          +-- 2c (A, B)   canonical
   * </pre>
   */
  @Test
  void planIsCleanWhenForksTouchIdenticalAccounts() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock =
        b.appendStale(ancestor.getHeader(), b.balWithBalanceTouches(ALICE, BOB), 2L);
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE, BOB), 2L);

    assertClean(plan(staleBlock, canonicalBlock));
  }

  /**
   * Not a reorg at all: the old pivot is a canonical ancestor of the new pivot, so there is no
   * orphaned fork to recover from.
   *
   * <pre>
   * gen -- 1 -- 2 -- 3     single canonical chain; oldPivot=2, newPivot=3
   * </pre>
   */
  @Test
  void planIsCleanWhenOldPivotIsAlreadyCanonical() {
    // Build a straight canonical chain: genesis -> block1 -> block2 -> block3.
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2 =
        b.appendBlockWithBal(block1.getHeader(), b.balWithBalanceTouches(ALICE), 2L);
    final Block block3 = b.appendBlockWithBal(block2.getHeader(), b.balWithBalanceTouches(BOB), 3L);

    // oldPivot = block2 (canonical), newPivot = block3 (canonical descendant).
    // This is not a reorg: there is no orphaned fork to recover from.
    final ReorgPlan plan = plan(block2, block3);

    assertThat(plan.commonAncestor().getHash()).isEqualTo(block2.getHash());
    assertThat(plan.fromBlock()).isEqualTo(3L);
    assertThat(plan.toBlock()).isEqualTo(3L);
    // collectOrphanedTouches walks zero blocks (commonAncestor == oldPivot).
    assertClean(plan);
  }

  // ---------------------------------------------------------------------------
  // Scoping: divergence must respect downloaded account/storage ranges and BAL
  // content filters.
  // ---------------------------------------------------------------------------

  /**
   * Divergence is scoped to persisted account ranges: stale-only accounts whose range has not been
   * downloaded yet are excluded.
   *
   * <pre>
   * gen -- 1 +-- 2s (A, D)   stale
   *          +-- 2c (A)      canonical
   * persisted account ranges: [A,A] only -- D's range not yet downloaded
   * </pre>
   *
   * D is stale-only but nothing of it was persisted, so there is nothing to heal: plan clean.
   */
  @Test
  void excludesEntriesOutsidePersistedRanges() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock =
        b.appendStale(ancestor.getHeader(), b.balWithBalanceTouches(ALICE, DAVE), 2L);
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    // Tracker covers only ALICE, not DAVE — simulating a range that hasn't finished downloading.
    final ReorgPlan plan =
        plan(
            staleBlock,
            canonicalBlock,
            persistedAccounts(ALICE),
            new DownloadedStorageRangeTracker());

    // Dave was touched only in the stale fork but its range hasn't been downloaded yet -> excluded.
    assertNoDivergedAccounts(plan, DAVE);
    assertClean(plan);
  }

  /**
   * For a PENDING account range, only orphaned slots inside the downloaded storage ranges are
   * diverged.
   *
   * <pre>
   * gen -- 1 +-- 2s (A:s1, A:s5; A balance)   stale
   *          +-- 2c (A balance)               canonical
   * A persisted but pending (outstanding children); downloaded slots for A: {s1} only
   * </pre>
   *
   * s1 is diverged; s5 is excluded because it has not been downloaded yet (its value will arrive
   * with the pending download, already canonical).
   */
  @Test
  void excludesDivergedSlotsOutsideDownloadedStorageRange() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot1 = UInt256.valueOf(1);
    final UInt256 slot5 = UInt256.valueOf(5);

    // Stale: Alice has storage writes to slot 1 and slot 5, plus a balance touch.
    final BlockAccessList staleBal =
        b.merge(
            b.balWithStorageChanges(
                ALICE, Map.of(slot1, UInt256.valueOf(7), slot5, UInt256.valueOf(9))),
            b.balWithBalanceTouches(ALICE));
    final Block staleBlock = b.appendStale(ancestor.getHeader(), staleBal, 2L);

    // Canonical: Alice has only a balance touch (no storage) -> both slots diverged.
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    // Alice's account range is persisted but still PENDING (outstanding child requests) — only
    // for pending ranges does per-slot tracking survive; completing a range wipes it.
    // Storage tracker covers ONLY slot 1 for Alice. Slot 5's hash is outside the downloaded range.
    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, pendingAccounts(ALICE), singleSlotRange(ALICE, slot1));

    // Alice is touched on both forks -> not a diverged account.
    assertNoDivergedAccounts(plan, ALICE);
    // Slot 1 is in the downloaded range -> diverged. Slot 5 is NOT in the range -> excluded.
    assertDivergedSlots(plan, ALICE, slot1);
    assertNoDivergedSlots(plan, ALICE, slot5);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * Read-only BAL entries are not touches: an account with only storage reads never enters the
   * touch sets.
   *
   * <pre>
   * gen -- 1 +-- 2s (A balance; D reads s7)   stale
   *          +-- 2c (A balance)               canonical
   * </pre>
   *
   * D has hasAnyChange=false, so it is invisible to the plan: clean plan.
   */
  @Test
  void ignoresAccountsWithNoChangesInBal() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 daveSlot = UInt256.valueOf(7);

    // Stale BAL: Alice has a balance change (hasAnyChange=true); Dave has only a storage read
    // (hasAnyChange=false) and must be filtered out by collectTouches.
    final BlockAccessList staleBal =
        b.merge(b.balWithBalanceTouches(ALICE), b.balWithStorageReads(DAVE, daveSlot));
    final Block staleBlock = b.appendStale(ancestor.getHeader(), staleBal, 2L);

    // Canonical BAL touches only Alice. Dave is "untouched" canonically.
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, fullAccountRange(), fullStorageFor(DAVE));

    // Dave's stale entry had no changes -> it never made it into orphanedTouches -> not diverged.
    assertNoDivergedAccounts(plan, DAVE);
    assertClean(plan);
  }

  /**
   * Empty BALs on both forks: nothing touched, nothing diverged.
   *
   * <pre>
   * gen -- 1 +-- 2s (empty BAL)   stale
   *          +-- 2c (empty BAL)   canonical
   * </pre>
   */
  @Test
  void allEmptyBalsProducesCleanPlan() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock = b.appendStale(ancestor.getHeader(), b.emptyBal(), 2L);
    final Block canonicalBlock = b.appendCanonical(ancestor.getHeader(), b.emptyBal(), 2L);

    assertClean(plan(staleBlock, canonicalBlock));
  }

  // ---------------------------------------------------------------------------
  // Boundary conditions on the ancestor walk.
  // ---------------------------------------------------------------------------

  /**
   * The fork point can be genesis itself: both forks start at block 1.
   *
   * <pre>
   * gen +-- 1s (A)   stale
   *     +-- 1c (B)   canonical
   * </pre>
   *
   * commonAncestor=0, apply window [1,1]; A is diverged (stale-only), B is applied by the canonical
   * BALs.
   */
  @Test
  void commonAncestorAtGenesis() {
    // Both forks start from genesis (block 0). No "ancestor" block appended on top of genesis.
    final Block staleBlock = b.appendStale(b.header(0), b.balWithBalanceTouches(ALICE), 1L);
    final Block canonicalBlock = b.appendCanonical(b.header(0), b.balWithBalanceTouches(BOB), 1L);

    assertThat(b.blockchain().blockIsOnCanonicalChain(staleBlock.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(canonicalBlock.getHash())).isTrue();

    final ReorgPlan plan = plan(staleBlock, canonicalBlock);

    // Common ancestor is genesis (block 0).
    assertThat(plan.commonAncestor().getNumber()).isZero();
    assertThat(plan.fromBlock()).isEqualTo(1L);
    assertThat(plan.toBlock()).isEqualTo(1L);

    // Alice was touched only on the orphaned fork -> diverged.
    // Bob was touched only on the canonical fork -> applied by canonical BALs, not diverged.
    assertDivergedAccounts(plan, ALICE);
    assertNoDivergedAccounts(plan, BOB);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * The ancestor walk succeeds at exactly MAX_ANCESTOR_WALK (64) steps.
   *
   * <pre>
   * gen -- 1 -- ... -- 64 (oldPivot)        stale, diff 10
   *  +-- 1' -- ... -- 64' -- 65' (newPivot)  canonical, diff 100
   * </pre>
   *
   * Common ancestor = genesis, 64 steps below the old pivot: just within bound.
   */
  @Test
  void ancestorWalkAtExactMaxSucceeds() {
    // Original chain: MAX_ANCESTOR_WALK blocks above genesis.
    final BlockHeader oldPivot =
        b.appendStaleChain(b.header(0), 1L, SnapV2ReorgHealer.MAX_ANCESTOR_WALK);

    // Competing chain: MAX_ANCESTOR_WALK + 1 blocks from genesis -> wins the reorg.
    final BlockHeader newPivot =
        b.appendCanonicalChain(b.header(0), 1L, SnapV2ReorgHealer.MAX_ANCESTOR_WALK + 1);

    // Common ancestor is genesis (depth 64 from oldPivot) -> walk just within bound.
    final ReorgPlan plan =
        plan(oldPivot, newPivot, fullAccountRange(), new DownloadedStorageRangeTracker());
    assertThat(plan.commonAncestor().getNumber()).isZero();
  }

  /**
   * One block deeper than MAX_ANCESTOR_WALK is unrecoverable.
   *
   * <pre>
   * gen -- 1 -- ... -- 64 -- 65 (oldPivot)     stale, diff 10
   *  +-- 1' -- ... -- 65' -- 66' (newPivot)     canonical, diff 100
   * </pre>
   *
   * Common ancestor = genesis, 65 steps below the old pivot: ReorgUnrecoverableException (per
   * snap/2, a reorg this deep forces a sync restart).
   */
  @Test
  void throwsWhenAncestorWalkExceedsMax() {
    // Original chain: MAX_ANCESTOR_WALK + 1 blocks above genesis.
    final BlockHeader oldPivot =
        b.appendStaleChain(b.header(0), 1L, SnapV2ReorgHealer.MAX_ANCESTOR_WALK + 1);

    // Competing chain: MAX_ANCESTOR_WALK + 2 blocks from genesis -> wins the reorg.
    final BlockHeader newPivot =
        b.appendCanonicalChain(b.header(0), 1L, SnapV2ReorgHealer.MAX_ANCESTOR_WALK + 2);

    // Common ancestor is genesis (depth 65 from oldPivot) -> walk exceeds MAX_ANCESTOR_WALK (64).
    assertThat(b.blockchain().blockIsOnCanonicalChain(oldPivot.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(newPivot.getHash())).isTrue();

    assertThatThrownBy(
            () -> plan(oldPivot, newPivot, fullAccountRange(), new DownloadedStorageRangeTracker()))
        .isInstanceOf(ReorgUnrecoverableException.class);
  }

  // ---------------------------------------------------------------------------
  // Error paths: unrecoverable reorgs and missing-data conditions.
  // ---------------------------------------------------------------------------

  /**
   * A reorg whose apply window starts below EIP-7928 (BAL) activation is unrecoverable: snap/2
   * catch-up has no BALs to work with there.
   *
   * <pre>
   * gen -- 1 +-- 2s (A)   stale
   *          +-- 2c (B)   canonical, but the schedule reports BAL disabled at block 2
   * </pre>
   *
   * checkBalActivation on fromBlock (=2): ReorgUnrecoverableException.
   */
  @Test
  void throwsWhenReorgDipsBelowBalActivation() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock =
        b.appendStale(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(BOB), 2L);

    final SnapV2ReorgHealer healer =
        new SnapV2ReorgHealer(
            b.blockchain(),
            unusedStorageCoordinator(),
            ReorgBlockchainBuilder.balDisabledSchedule());

    assertThatThrownBy(
            () ->
                plan(
                    healer,
                    staleBlock.getHeader(),
                    canonicalBlock.getHeader(),
                    fullAccountRange(),
                    new DownloadedStorageRangeTracker()))
        .isInstanceOf(ReorgUnrecoverableException.class);
  }

  /**
   * The orphaned fork's BALs are required locally to compute the stale touch set.
   *
   * <pre>
   * gen -- 1 +-- 2s (A) -- BAL NOT stored   stale
   *          +-- 2c (B) -- BAL stored       canonical
   * </pre>
   *
   * ReorgUnrecoverableException: without orphaned BALs the diverged set cannot be computed and the
   * sync must restart (mirrors the snap/2 retention requirement).
   */
  @Test
  void throwsWhenOrphanedBalIsLocallyPruned() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    // Stale block 2s: appended WITHOUT storing its BAL (simulating a pruned BAL).
    final Block staleBlock =
        b.appendStaleWithoutStoringBal(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    // Canonical block 2c wins the reorg.
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(BOB), 2L);

    assertThatThrownBy(() -> plan(staleBlock, canonicalBlock))
        .isInstanceOf(ReorgUnrecoverableException.class);
  }

  /**
   * Walking the orphaned chain to collect touches requires every orphaned parent header.
   *
   * <pre>
   * gen -- 1 +-- 2s -- 3s (oldPivot)   stale; 2s header pruned before collectOrphanedTouches
   *          +-- 2c -- 3c (newPivot)   canonical
   * </pre>
   *
   * The spy hands out the 2s header once (for findCommonAncestor) and then empty, so the
   * touch-collection walk from 3s aborts: ReorgUnrecoverableException.
   */
  @Test
  void throwsWhenOrphanedParentHeaderMissing() {
    // Common ancestor at block 1, orphaned chain at blocks 2s and 3s, canonical chain at 2c and 3c.
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block block2s = b.appendStale(ancestor.getHeader(), b.emptyBal(), 2L);
    final Block block3s = b.appendStale(block2s.getHeader(), b.emptyBal(), 3L);

    final Block block2c = b.appendCanonical(ancestor.getHeader(), b.emptyBal(), 2L);
    final Block block3c = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L);

    assertThat(b.blockchain().blockIsOnCanonicalChain(block3s.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(block3c.getHash())).isTrue();

    // Spy: getBlockHeader(block2s.hash) succeeds once during findCommonAncestor, then fails during
    // collectOrphanedTouches (simulating a pruned orphaned parent header).
    final MutableBlockchain spy = Mockito.spy(b.blockchain());
    Mockito.when(spy.getBlockHeader(block2s.getHash()))
        .thenReturn(Optional.of(block2s.getHeader()))
        .thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                plan(
                    healerFor(spy),
                    block3s.getHeader(),
                    block3c.getHeader(),
                    fullAccountRange(),
                    new DownloadedStorageRangeTracker()))
        .isInstanceOf(ReorgUnrecoverableException.class);
  }

  /**
   * Every canonical BAL in the apply window [fromBlock, toBlock] must be present locally.
   *
   * <pre>
   * gen -- 1 +-- 2s (empty BAL, stored)   stale
   *          +-- 2c (BAL NOT stored)      canonical
   * </pre>
   *
   * collectCanonicalTouches fails fast: IllegalStateException.
   */
  @Test
  void throwsWhenCanonicalBalMissing() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock = b.appendStale(ancestor.getHeader(), b.emptyBal(), 2L);

    // Canonical block appended WITHOUT its BAL (simulating a pruned canonical BAL).
    final Block canonicalBlock =
        b.appendCanonicalWithoutStoringBal(ancestor.getHeader(), b.emptyBal(), 2L);

    assertThatThrownBy(() -> plan(staleBlock, canonicalBlock))
        .isInstanceOf(IllegalStateException.class);
  }

  /**
   * The canonical header at fromBlock must be loadable (it carries the balHash and feeds the
   * activation check).
   *
   * <pre>
   * gen -- 1 +-- 2s   stale
   *          +-- 2c   canonical; spy: getBlockHeader(2L) returns empty
   * </pre>
   *
   * loadCanonicalHeader inside checkBalActivation: IllegalStateException.
   */
  @Test
  void throwsWhenCanonicalHeaderMissing() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock = b.appendStale(ancestor.getHeader(), b.emptyBal(), 2L);
    final Block canonicalBlock = b.appendCanonical(ancestor.getHeader(), b.emptyBal(), 2L);

    // Spy: getBlockHeader(2L) returns empty -> loadCanonicalHeader throws IllegalStateException
    // during checkBalActivation (fromBlock = ancestor.getNumber() + 1 = 2).
    final MutableBlockchain spy = Mockito.spy(b.blockchain());
    Mockito.when(spy.getBlockHeader(2L)).thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                plan(
                    healerFor(spy),
                    staleBlock.getHeader(),
                    canonicalBlock.getHeader(),
                    fullAccountRange(),
                    new DownloadedStorageRangeTracker()))
        .isInstanceOf(IllegalStateException.class);
  }

  // ---------------------------------------------------------------------------
  // Tracker lifecycle: pending vs. completed account ranges.
  // ---------------------------------------------------------------------------

  /**
   * Registers the full account range as pending, then completes it with the same wiring used in
   * production ({@code SnapV2WorldDownloadState}).
   */
  private static DownloadedAccountRangeTracker completeFullAccountRange(
      final DownloadedStorageRangeTracker storageTracker) {
    final DownloadedAccountRangeTracker accountTracker = new DownloadedAccountRangeTracker();
    accountTracker.setOnRangeCompleted(storageTracker::removeAccountHashesInRange);
    accountTracker.registerPending(Bytes32.ZERO, MAX_KEY, 1);
    accountTracker.onChildCompleted(Bytes32.ZERO);
    return accountTracker;
  }

  /**
   * A completed account range implies fully-downloaded storage: slot divergence must be reported
   * even though completing the range wiped the per-slot tracker entries.
   *
   * <pre>
   * gen -- 1 +-- 2s (F:s5; A balance)   stale
   *          +-- 2c (F, A balances)     canonical
   * lifecycle: slots F:[0,max] tracked while pending -> range completes -> per-slot entries
   * removed (the SnapV2WorldDownloadState production wiring)
   * </pre>
   *
   * F overlaps on both forks (not a diverged account), but F.s5 is stale-only and fully downloaded:
   * diverged. Guards the fix that mirrors the applier's isAccountCompleted bypass.
   */
  @Test
  void reportsDivergedSlotsForCompletedAccounts() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot5 = UInt256.valueOf(5);

    // Stale: Frank writes slot 5; Alice has a balance change.
    final BlockAccessList staleBal =
        b.merge(
            b.balWithStorageChanges(FRANK, Map.of(slot5, UInt256.valueOf(7))),
            b.balWithBalanceTouches(ALICE));
    final Block staleBlock = b.appendStale(ancestor.getHeader(), staleBal, 2L);

    // Canonical: Frank and Alice are both touched, but slot 5 is absent.
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(FRANK, ALICE), 2L);

    final DownloadedStorageRangeTracker storageTracker = fullStorageFor(FRANK);
    final DownloadedAccountRangeTracker accountTracker = completeFullAccountRange(storageTracker);
    assertThat(storageTracker.isSlotHashDownloaded(accountHash(FRANK), slotHashBytes(slot5)))
        .isFalse(); // slot entries were wiped on completion, as in production

    final ReorgPlan plan = plan(staleBlock, canonicalBlock, accountTracker, storageTracker);

    // Frank overlaps on both forks -> not a diverged account.
    assertNoDivergedAccounts(plan, FRANK, ALICE);
    // Slot 5 was touched only in the stale fork and Frank's storage is fully downloaded ->
    // diverged, even though the slot tracker no longer holds per-slot entries for Frank.
    assertDivergedSlots(plan, FRANK, slot5);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * A persisted-but-pending account (leaves downloaded, child requests outstanding) is still
   * reported as diverged.
   *
   * <pre>
   * gen -- 1 +-- 2s (D balance)   stale
   *          +-- 2c (A balance)   canonical
   * account range [D,D]: registered with 1 pending child, never completed -> pending
   * </pre>
   *
   * D's leaves exist locally and may hold orphaned values: D is diverged.
   */
  @Test
  void includesDivergedAccountsInPendingRanges() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    // Stale: Dave has a balance change. Canonical: only Alice is touched.
    final Block staleBlock = b.appendStale(ancestor.getHeader(), b.balWithBalanceTouches(DAVE), 2L);
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    // Dave's account leaves are persisted, but his range still has outstanding child requests.
    // A persisted-but-pending account must still be reported as diverged.
    final ReorgPlan plan =
        plan(
            staleBlock, canonicalBlock, pendingAccounts(DAVE), new DownloadedStorageRangeTracker());

    assertDivergedAccounts(plan, DAVE);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * No persisted account leaves means no slot-level healing either, regardless of what the slot
   * tracker claims.
   *
   * <pre>
   * gen -- 1 +-- 2s (NC:s5)       stale
   *          +-- 2c (A balance)   canonical
   * persisted account ranges: [A,A] only; slot tracker claims NC:[0,max]
   * </pre>
   *
   * NC was never downloaded, so there is no local state to correct: plan clean.
   */
  @Test
  void excludesDivergedSlotsForNonPersistedAccounts() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot5 = UInt256.valueOf(5);

    // Stale: NewContract writes slot 5 (storage-only change). Canonical: only Alice is touched.
    final Block staleBlock =
        b.appendStale(
            ancestor.getHeader(),
            b.balWithStorageChanges(NEW_CONTRACT, Map.of(slot5, UInt256.valueOf(99))),
            2L);
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    // Only Alice's account range is persisted. The slot tracker claims coverage for NewContract,
    // but with no persisted account leaves there is nothing to heal at the slot level either.
    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, persistedAccounts(ALICE), fullStorageFor(NEW_CONTRACT));

    assertClean(plan);
  }

  // ---------------------------------------------------------------------------
  // BAL content: what counts as a "touch".
  // ---------------------------------------------------------------------------

  /**
   * Nonce-only and code-only changes count as touches (hasAnyChange), not just balance/storage.
   *
   * <pre>
   * gen -- 1 +-- 2s (D nonce=1; NC code)   stale
   *          +-- 2c (A balance)            canonical
   * </pre>
   *
   * Both D and NC are stale-only touches: diverged accounts.
   */
  @Test
  void treatsNonceAndCodeChangesAsTouches() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    // Stale: Dave gets a nonce-only change; NewContract gets a code-only change (a contract
    // deployment, the "delete if missing on canonical" case).
    final BlockAccessList staleBal =
        b.merge(
            b.balWithNonceChange(DAVE, 1L),
            b.balWithCodeChange(NEW_CONTRACT, Bytes.of(0x60, 0x80)));
    final Block staleBlock = b.appendStale(ancestor.getHeader(), staleBal, 2L);

    // Canonical: only Alice is touched.
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    final ReorgPlan plan = plan(staleBlock, canonicalBlock);

    // Nonce-only and code-only changes both count as touches -> both accounts are diverged.
    assertDivergedAccounts(plan, DAVE, NEW_CONTRACT);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * A storage READ does not become a diverged slot, even on an account that has real changes.
   *
   * <pre>
   * gen -- 1 +-- 2s (A balance; A reads s7)   stale
   *          +-- 2c (B balance)               canonical
   * </pre>
   *
   * A is a diverged account (balance change), but s7 was only read, so it never enters the touch
   * set: no diverged slots.
   */
  @Test
  void excludesReadOnlySlotsOfChangedAccounts() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot7 = UInt256.valueOf(7);

    // Stale: Alice has a balance change (so the account IS touched) AND a read of slot 7 (which
    // must NOT count as a slot touch).
    final BlockAccessList staleBal =
        b.merge(b.balWithBalanceTouches(ALICE), b.balWithStorageReads(ALICE, slot7));
    final Block staleBlock = b.appendStale(ancestor.getHeader(), staleBal, 2L);

    // Canonical: Alice is untouched.
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(BOB), 2L);

    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, fullAccountRange(), fullStorageFor(ALICE));

    // Alice's balance change makes her a diverged account...
    assertDivergedAccounts(plan, ALICE);
    // ...but the read-only slot 7 must not leak into the diverged slots.
    assertNoDivergedSlots(plan, ALICE, slot7);
    assertThat(plan.divergedSlotsByAccount()).isEmpty();
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * Touches for the same account merge across all orphaned blocks, not per block.
   *
   * <pre>
   *            2s (A balance) -- 3s (A:s5)     stale
   *           /
   * gen -- 1 +
   *           \
   *            2c (A balance) -- 3c (empty)    canonical
   * </pre>
   *
   * A overlaps (touched in 2s and 2c), so the account is not diverged; A.s5 comes from a different
   * orphaned block (3s) and is stale-only: diverged slot.
   */
  @Test
  void mergesTouchesAcrossOrphanedBlocks() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot5 = UInt256.valueOf(5);

    // Stale block 2s: Alice balance touch (no slots).
    final Block block2s = b.appendStale(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    // Stale block 3s: Alice writes slot 5 (storage-only change).
    final Block block3s =
        b.appendStale(
            block2s.getHeader(),
            b.balWithStorageChanges(ALICE, Map.of(slot5, UInt256.valueOf(42))),
            3L);

    // Canonical blocks 2c and 3c: Alice is balance-touched in 2c (account overlap).
    final Block block2c =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);
    final Block block3c = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L);

    assertThat(b.blockchain().blockIsOnCanonicalChain(block3s.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(block3c.getHash())).isTrue();

    final ReorgPlan plan = plan(block3s, block3c, fullAccountRange(), fullStorageFor(ALICE));

    assertThat(plan.commonAncestor().getNumber()).isEqualTo(1L);
    // Alice is touched on both forks -> not a diverged account.
    assertNoDivergedAccounts(plan, ALICE);
    // Slot 5 was touched in orphaned block 3s (a different orphaned block than Alice's 2s balance
    // touch) and is absent from the canonical fork -> diverged. Touches must merge across blocks.
    assertDivergedSlots(plan, ALICE, slot5);
    assertThat(plan.isClean()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Account divergence: per-scalar-field granularity (balance/nonce/code).
  // ---------------------------------------------------------------------------

  /**
   * The motivating scenario: the balance changed on the orphaned fork while the code changed on the
   * canonical fork. Applying the canonical BAL fixes the code but would leave the orphaned balance
   * in place, because BALs carry post-values only for the fields that changed. The account record
   * must be re-fetched.
   *
   * <pre>
   * gen -- 1 +-- 2s (A balance)   stale
   *          +-- 2c (A code)      canonical
   * </pre>
   */
  @Test
  void divergedAccountWhenBalanceOrphanedOnlyAndCodeCanonicalOnly() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock =
        b.appendStale(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);
    final Block canonicalBlock =
        b.appendCanonical(
            ancestor.getHeader(), b.balWithCodeChange(ALICE, Bytes.of(0x60, 0x80)), 2L);

    final ReorgPlan plan = plan(staleBlock, canonicalBlock);

    assertDivergedAccounts(plan, ALICE);
    assertThat(plan.divergedSlotsByAccount()).isEmpty();
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * Nonce changed on the orphaned fork only; balance changed on the canonical fork only.
   *
   * <pre>
   * gen -- 1 +-- 2s (A nonce=1)   stale
   *          +-- 2c (A balance)   canonical
   * </pre>
   */
  @Test
  void divergedAccountWhenNonceOrphanedOnlyAndBalanceCanonicalOnly() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock =
        b.appendStale(ancestor.getHeader(), b.balWithNonceChange(ALICE, 1L), 2L);
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    final ReorgPlan plan = plan(staleBlock, canonicalBlock);

    assertDivergedAccounts(plan, ALICE);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * Code changed on the orphaned fork only; balance changed on the canonical fork only.
   *
   * <pre>
   * gen -- 1 +-- 2s (A code)      stale
   *          +-- 2c (A balance)   canonical
   * </pre>
   */
  @Test
  void divergedAccountWhenCodeOrphanedOnlyAndBalanceCanonicalOnly() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock =
        b.appendStale(ancestor.getHeader(), b.balWithCodeChange(ALICE, Bytes.of(0x60, 0x80)), 2L);
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);

    final ReorgPlan plan = plan(staleBlock, canonicalBlock);

    assertDivergedAccounts(plan, ALICE);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * Balance changed on the orphaned fork only; the canonical fork touched the account solely via a
   * storage write. Account-level overlap must not hide the stale balance.
   *
   * <pre>
   * gen -- 1 +-- 2s (A balance)   stale
   *          +-- 2c (A:s3)        canonical
   * </pre>
   */
  @Test
  void divergedAccountWhenBalanceOrphanedOnlyAndStorageCanonicalOnly() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot3 = UInt256.valueOf(3);

    final Block staleBlock =
        b.appendStale(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);
    final Block canonicalBlock =
        b.appendCanonical(
            ancestor.getHeader(),
            b.balWithStorageChanges(ALICE, Map.of(slot3, UInt256.valueOf(7))),
            2L);

    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, fullAccountRange(), fullStorageFor(ALICE));

    assertDivergedAccounts(plan, ALICE);
    // The canonical slot write is not stale-only, so nothing is slot-diverged.
    assertNoDivergedSlots(plan, ALICE, slot3);
    assertThat(plan.divergedSlotsByAccount()).isEmpty();
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * The same scalar field changed on both forks is overwritten by the canonical BAL: no divergence.
   *
   * <pre>
   * gen -- 1 +-- 2s (A=50)   stale
   *          +-- 2c (A=80)   canonical
   * </pre>
   */
  @Test
  void noDivergenceWhenSameFieldChangesOnBothForks() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock =
        b.appendStale(ancestor.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block canonicalBlock =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    final ReorgPlan plan = plan(staleBlock, canonicalBlock);

    assertNoDivergedAccounts(plan, ALICE);
    assertClean(plan);
  }

  /**
   * Scalar touches merge across all blocks of a fork: a nonce changed in a later orphaned block
   * only makes the account diverged even though its balance changed on both forks.
   *
   * <pre>
   *            2s (A balance) -- 3s (A nonce)     stale
   *           /
   * gen -- 1 +
   *           \
   *            2c (A balance) -- 3c (empty)       canonical
   * </pre>
   */
  @Test
  void accountDivergenceMergesAcrossOrphanedBlocks() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block block2s = b.appendStale(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);
    final Block block3s = b.appendStale(block2s.getHeader(), b.balWithNonceChange(ALICE, 1L), 3L);

    final Block block2c =
        b.appendCanonical(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);
    final Block block3c = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L);

    assertThat(b.blockchain().blockIsOnCanonicalChain(block3s.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(block3c.getHash())).isTrue();

    final ReorgPlan plan = plan(block3s, block3c);

    // The balance overlaps on both forks, but the nonce changed on the orphaned fork only.
    assertDivergedAccounts(plan, ALICE);
    assertThat(plan.isClean()).isFalse();
  }

  /**
   * Account divergence is scoped to persisted account ranges, like slot divergence.
   *
   * <pre>
   * gen -- 1 +-- 2s (A balance)   stale
   *          +-- 2c (A code)      canonical
   * </pre>
   */
  @Test
  void excludesDivergenceWhenAccountNotPersisted() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock =
        b.appendStale(ancestor.getHeader(), b.balWithBalanceTouches(ALICE), 2L);
    final Block canonicalBlock =
        b.appendCanonical(
            ancestor.getHeader(), b.balWithCodeChange(ALICE, Bytes.of(0x60, 0x80)), 2L);

    // Only Bob's account range is persisted; Alice's scalar divergence is irrelevant locally.
    final ReorgPlan plan =
        plan(
            staleBlock,
            canonicalBlock,
            persistedAccounts(BOB),
            new DownloadedStorageRangeTracker());

    assertClean(plan);
  }

  /**
   * Account and slot divergence coexist on the same account: the orphaned fork changed the balance
   * and slot s5; the canonical fork changed only the code.
   *
   * <pre>
   * gen -- 1 +-- 2s (A balance; A:s5)   stale
   *          +-- 2c (A code)            canonical
   * </pre>
   */
  @Test
  void accountAndSlotDivergenceCoexistOnSameAccount() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final UInt256 slot5 = UInt256.valueOf(5);

    final BlockAccessList staleBal =
        b.merge(
            b.balWithBalanceTouches(ALICE),
            b.balWithStorageChanges(ALICE, Map.of(slot5, UInt256.valueOf(42))));
    final Block staleBlock = b.appendStale(ancestor.getHeader(), staleBal, 2L);
    final Block canonicalBlock =
        b.appendCanonical(
            ancestor.getHeader(), b.balWithCodeChange(ALICE, Bytes.of(0x60, 0x80)), 2L);

    final ReorgPlan plan =
        plan(staleBlock, canonicalBlock, fullAccountRange(), fullStorageFor(ALICE));

    assertDivergedAccounts(plan, ALICE);
    assertDivergedSlots(plan, ALICE, slot5);
    assertThat(plan.isClean()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Error paths: ancestor walk failures.
  // ---------------------------------------------------------------------------

  /**
   * The ancestor walk itself aborts when a parent header is missing mid-walk.
   *
   * <pre>
   * gen -- 1 +-- 2s (oldPivot)   stale; spy: header of block 1 returns empty
   *          +-- 2c (newPivot)   canonical
   * </pre>
   *
   * The walk from 2s can never reach a canonical ancestor: ReorgUnrecoverableException. Covers the
   * findCommonAncestor missing-parent branch, distinct from throwsWhenOrphanedParentHeaderMissing
   * which fails later in collectOrphanedTouches.
   */
  @Test
  void throwsWhenAncestorWalkParentHeaderMissing() {
    final Block ancestor = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block staleBlock = b.appendStale(ancestor.getHeader(), b.emptyBal(), 2L);
    final Block canonicalBlock = b.appendCanonical(ancestor.getHeader(), b.emptyBal(), 2L);

    assertThat(b.blockchain().blockIsOnCanonicalChain(staleBlock.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(canonicalBlock.getHash())).isTrue();

    // Spy: the ancestor header is unavailable during findCommonAncestor's walk (simulating a
    // pruned header), so the walk cannot reach a canonical ancestor.
    final MutableBlockchain spy = Mockito.spy(b.blockchain());
    Mockito.when(spy.getBlockHeader(ancestor.getHash())).thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                plan(
                    healerFor(spy),
                    staleBlock.getHeader(),
                    canonicalBlock.getHeader(),
                    fullAccountRange(),
                    new DownloadedStorageRangeTracker()))
        .isInstanceOf(ReorgUnrecoverableException.class);
  }

  // ---------------------------------------------------------------------------
  // Shared test plumbing: plan invocation, tracker factories and plan assertions.
  // ---------------------------------------------------------------------------

  private static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }

  private static DownloadedStorageRangeTracker fullStorageFor(final Address... accounts) {
    final DownloadedStorageRangeTracker tracker = new DownloadedStorageRangeTracker();
    for (final Address account : accounts) {
      tracker.registerSlotRange(accountHash(account), Bytes32.ZERO, MAX_KEY);
    }
    return tracker;
  }

  private SnapV2ReorgHealer healer() {
    return new SnapV2ReorgHealer(
        b.blockchain(), unusedStorageCoordinator(), ReorgBlockchainBuilder.balEnabledSchedule());
  }

  private static SnapV2ReorgHealer healerFor(final MutableBlockchain blockchain) {
    return new SnapV2ReorgHealer(
        blockchain, unusedStorageCoordinator(), ReorgBlockchainBuilder.balEnabledSchedule());
  }

  private static DownloadedStorageRangeTracker singleSlotRange(
      final Address account, final UInt256 slotKey) {
    final DownloadedStorageRangeTracker tracker = new DownloadedStorageRangeTracker();
    final Bytes32 slotHash = slotHashBytes(slotKey);
    tracker.registerSlotRange(accountHash(account), slotHash, slotHash);
    return tracker;
  }

  private static WorldStateStorageCoordinator unusedStorageCoordinator() {
    return null;
  }

  private ReorgPlan plan(final Block stale, final Block canonical) {
    return plan(stale, canonical, fullAccountRange(), new DownloadedStorageRangeTracker());
  }

  private ReorgPlan plan(
      final Block stale,
      final Block canonical,
      final DownloadedAccountRangeTracker accountTracker,
      final DownloadedStorageRangeTracker storageTracker) {
    return plan(stale.getHeader(), canonical.getHeader(), accountTracker, storageTracker);
  }

  private ReorgPlan plan(
      final BlockHeader stale,
      final BlockHeader canonical,
      final DownloadedAccountRangeTracker accountTracker,
      final DownloadedStorageRangeTracker storageTracker) {
    return plan(healer(), stale, canonical, accountTracker, storageTracker);
  }

  private static ReorgPlan plan(
      final SnapV2ReorgHealer healer,
      final BlockHeader stale,
      final BlockHeader canonical,
      final DownloadedAccountRangeTracker accountTracker,
      final DownloadedStorageRangeTracker storageTracker) {
    return healer.planReorg(stale, canonical, accountTracker, storageTracker);
  }

  private static Bytes32 accountHash(final Address account) {
    return Bytes32.wrap(account.addressHash().getBytes());
  }

  private static Bytes32 slotHashBytes(final UInt256 slot) {
    return Bytes32.wrap(ReorgBlockchainBuilder.slotHash(slot).getBytes());
  }

  /** Completed single-account ranges: leaves persisted, no outstanding child requests. */
  private static DownloadedAccountRangeTracker persistedAccounts(final Address... accounts) {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    for (final Address account : accounts) {
      tracker.registerPending(accountHash(account), accountHash(account), 0);
    }
    return tracker;
  }

  /** Pending single-account ranges: leaves persisted, child requests still outstanding. */
  private static DownloadedAccountRangeTracker pendingAccounts(final Address... accounts) {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    for (final Address account : accounts) {
      tracker.registerPending(accountHash(account), accountHash(account), 1);
    }
    return tracker;
  }

  private static Hash[] addressHashes(final Address... addresses) {
    return Arrays.stream(addresses).map(Address::addressHash).toArray(Hash[]::new);
  }

  private static Hash[] slotHashes(final UInt256... slots) {
    return Arrays.stream(slots).map(ReorgBlockchainBuilder::slotHash).toArray(Hash[]::new);
  }

  private static void assertDivergedAccounts(final ReorgPlan plan, final Address... expected) {
    assertThat(plan.divergedAccounts()).containsExactlyInAnyOrder(addressHashes(expected));
  }

  private static void assertNoDivergedAccounts(final ReorgPlan plan, final Address... unexpected) {
    assertThat(plan.divergedAccounts()).doesNotContain(addressHashes(unexpected));
  }

  private static void assertDivergedSlots(
      final ReorgPlan plan, final Address account, final UInt256... slots) {
    assertThat(plan.divergedSlotsFor(account.addressHash()))
        .containsExactlyInAnyOrder(slotHashes(slots));
  }

  private static void assertNoDivergedSlots(
      final ReorgPlan plan, final Address account, final UInt256... slots) {
    assertThat(plan.divergedSlotsFor(account.addressHash())).doesNotContain(slotHashes(slots));
  }

  private static void assertClean(final ReorgPlan plan) {
    assertThat(plan.isClean()).isTrue();
    assertThat(plan.divergedAccounts()).isEmpty();
    assertThat(plan.divergedSlotsByAccount()).isEmpty();
  }
}
