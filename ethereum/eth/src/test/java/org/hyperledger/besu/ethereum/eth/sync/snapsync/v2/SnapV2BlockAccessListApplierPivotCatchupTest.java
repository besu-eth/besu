/*
 * Copyright contributors to Besu.
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
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SnapV2BlockAccessListApplier} in the non-reorg pivot catch-up context: the chain
 * advanced from the current pivot {@code P} to a new pivot {@code P+K}, and the BALs of blocks
 * {@code P+1..P+K} must be applied selectively to the already-downloaded state.
 */
class SnapV2BlockAccessListApplierPivotCatchupTest extends SnapV2TestFixtures {

  private static final UInt256 S1 = UInt256.valueOf(1);
  private static final UInt256 S2 = UInt256.valueOf(2);
  private static final UInt256 SP1 = UInt256.valueOf(101);
  private static final UInt256 SP2 = UInt256.valueOf(102);
  private static final UInt256 SP3 = UInt256.valueOf(103);

  private static final Bytes CAROL_CODE = Bytes.fromHexString("0x6080604052348015600e");
  private static final Bytes NEW_CONTRACT_CODE = Bytes.fromHexString("0x6080604052348015600f");

  // The syncing node's partially-downloaded state.
  private final BonsaiWorldStateKeyValueStorage localStorage = newBonsaiStorage();
  private final WorldStateStorageCoordinator localCoordinator =
      new WorldStateStorageCoordinator(localStorage);
  // The network's complete state, used as the canonical reference.
  private final BonsaiWorldStateKeyValueStorage canonicalStorage = newBonsaiStorage();
  private final WorldStateStorageCoordinator canonicalCoordinator =
      new WorldStateStorageCoordinator(canonicalStorage);

  // ---------------------------------------------------------------------------
  // applyBlockAccessLists: selective application during pivot catch-up.
  // ---------------------------------------------------------------------------

  @Nested
  class ApplyBlockAccessLists {

    /**
     * The catch-up happy path on fully downloaded state: every change type in the window is applied
     * — balances, nonces, code, slot writes, slot deletions, last-write-wins across blocks, and
     * accounts created after the old pivot — and the resulting world state is exactly the canonical
     * one at the new pivot.
     *
     * <pre>
     * gen -- 1 (A=100; B=200,B.s1=5,B.s2=6) -- 2 (A=150+A.nonce=3; B.s1=50,B.s2=0; C=42+code) -- 3 (A=175)
     * local: base [1,1] applied at download time; catch-up window [2,3]
     * </pre>
     */
    @Test
    void appliesAllChangeTypesToCompletedAccounts() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      final Block block1 =
          b.appendBlockWithBal(
              b.header(0),
              b.merge(
                  b.balWithBalances(Map.of(ALICE, Wei.of(100), BOB, Wei.of(200))),
                  b.balWithStorageChanges(
                      BOB, Map.of(S1, UInt256.valueOf(5), S2, UInt256.valueOf(6)))),
              1L);
      final Block block2 =
          b.appendBlockWithBal(
              block1.getHeader(),
              b.merge(
                  b.balWithBalances(Map.of(ALICE, Wei.of(150), CAROL, Wei.of(42))),
                  b.balWithNonceChange(ALICE, 3L),
                  b.balWithCodeChange(CAROL, CAROL_CODE),
                  b.balWithStorageChanges(BOB, Map.of(S1, UInt256.valueOf(50), S2, UInt256.ZERO))),
              2L);
      final Block block3 =
          b.appendBlockWithBal(
              block2.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(175))), 3L);

      // Both sides downloaded the full base state at pivot block 1.
      applyTo(localCoordinator, b, 1, 1, fullAccountRange(), new DownloadedStorageRangeTracker());
      applyTo(
          canonicalCoordinator, b, 1, 1, fullAccountRange(), new DownloadedStorageRangeTracker());

      // The canonical reference advances straight to the new pivot.
      applyTo(
          canonicalCoordinator, b, 2, 3, fullAccountRange(), new DownloadedStorageRangeTracker());

      // The syncing node catches up: apply BALs of blocks 2..3 to the downloaded state.
      applier(localCoordinator, b)
          .applyBlockAccessLists(
              block1.getHeader().getNumber() + 1,
              block3.getHeader().getNumber(),
              fullAccountRange(),
              new DownloadedStorageRangeTracker())
          .commit();

      // Last write across the window wins for Alice; nonce from block 2 is retained.
      final PmtStateTrieAccountValue alice = readAccount(ALICE);
      assertThat(alice.getBalance()).isEqualTo(Wei.of(175));
      assertThat(alice.getNonce()).isEqualTo(3L);

      // Slot write and slot deletion (zero write) on a completed account.
      assertThat(readStorageSlot(BOB, S1)).hasValue(UInt256.valueOf(50));
      assertThat(readStorageSlot(BOB, S2)).isEmpty();

      // Account created after the old pivot, with code.
      final PmtStateTrieAccountValue carol = readAccount(CAROL);
      assertThat(carol.getBalance()).isEqualTo(Wei.of(42));
      assertThat(carol.getCodeHash()).isEqualTo(Hash.hash(CAROL_CODE));
      assertThat(readCode(CAROL)).hasValue(CAROL_CODE);

      // No healing needed: the local world state is exactly the canonical one at the new pivot.
      assertThat(worldStateRoot(localCoordinator)).isEqualTo(worldStateRoot(canonicalCoordinator));
    }

    /**
     * A pending account receives scalar updates, but only to the slot writes whose slots were
     * already downloaded. Writes to never-downloaded slots are skipped.
     *
     * <pre>
     * gen -- 1 (P=100, P.sp1=1, P.sp2=2) -- 2 (P=300, P.sp2=20, P.sp3=30)
     * local: P pending; downloaded slots at base: sp1, sp2 (sp3 never seen)
     * </pre>
     */
    @Test
    void appliesOnlyDownloadedSlotsForPendingAccounts() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      final Block block1 =
          b.appendBlockWithBal(
              b.header(0),
              b.merge(
                  b.balWithBalances(Map.of(PETE, Wei.of(100))),
                  b.balWithStorageChanges(
                      PETE, Map.of(SP1, UInt256.valueOf(1), SP2, UInt256.valueOf(2)))),
              1L);
      final Block block2 =
          b.appendBlockWithBal(
              block1.getHeader(),
              b.merge(
                  b.balWithBalances(Map.of(PETE, Wei.of(300))),
                  b.balWithStorageChanges(
                      PETE, Map.of(SP2, UInt256.valueOf(20), SP3, UInt256.valueOf(30)))),
              2L);

      final DownloadedAccountRangeTracker accountTracker = accountRangeTracker(false, PETE);
      final DownloadedStorageRangeTracker storageTracker = downloadedSlots(PETE, SP1, SP2);

      applyTo(localCoordinator, b, 1, 1, accountTracker, storageTracker);
      applyTo(
          canonicalCoordinator, b, 1, 2, fullAccountRange(), new DownloadedStorageRangeTracker());

      applier(localCoordinator, b)
          .applyBlockAccessLists(2L, block2.getHeader().getNumber(), accountTracker, storageTracker)
          .commit();

      // Scalar update is applied to the persisted pending account.
      assertThat(readAccount(PETE).getBalance()).isEqualTo(Wei.of(300));
      // The downloaded slot is updated.
      assertThat(readStorageSlot(PETE, SP2)).hasValue(UInt256.valueOf(20));
      // The never-downloaded slot is skipped: the pending storage download will fetch it.
      assertThat(readStorageSlot(PETE, SP3)).isEmpty();

      // The local storage trie holds only a subset of the canonical slots, so the locally
      // recomputed roots cannot match the canonical ones yet — this is what the root patching
      // step repairs.
      assertThat(worldStateRoot(localCoordinator))
          .isNotEqualTo(worldStateRoot(canonicalCoordinator));
    }

    /**
     * When a pending account's BAL changes touch only slots that were never downloaded, no storage
     * write happens at all and the account's storage root is left exactly as the base download
     * computed it.
     *
     * <pre>
     * gen -- 1 (Pa=100, Pa.sp1=1) -- 2 (Pa=500, Pa.sp2=99)
     * local: Pa pending; downloaded slots at base: sp1 only
     * </pre>
     */
    @Test
    void leavesStorageRootUntouchedWhenOnlyNonDownloadedSlotsChange() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      final Block block1 =
          b.appendBlockWithBal(
              b.header(0),
              b.merge(
                  b.balWithBalances(Map.of(PAULA, Wei.of(100))),
                  b.balWithStorageChanges(PAULA, Map.of(SP1, UInt256.valueOf(1)))),
              1L);
      final Block block2 =
          b.appendBlockWithBal(
              block1.getHeader(),
              b.merge(
                  b.balWithBalances(Map.of(PAULA, Wei.of(500))),
                  b.balWithStorageChanges(PAULA, Map.of(SP2, UInt256.valueOf(99)))),
              2L);

      final DownloadedAccountRangeTracker accountTracker = accountRangeTracker(false, PAULA);
      final DownloadedStorageRangeTracker storageTracker = downloadedSlots(PAULA, SP1);

      applyTo(localCoordinator, b, 1, 1, accountTracker, storageTracker);
      final Hash rootAfterBase = readAccount(PAULA).getStorageRoot();

      applier(localCoordinator, b)
          .applyBlockAccessLists(2L, block2.getHeader().getNumber(), accountTracker, storageTracker)
          .commit();

      final PmtStateTrieAccountValue paula = readAccount(PAULA);
      assertThat(paula.getBalance()).isEqualTo(Wei.of(500));
      assertThat(readStorageSlot(PAULA, SP2)).isEmpty();
      // No downloaded slot changed: the storage trie was never opened, root untouched.
      assertThat(paula.getStorageRoot()).isEqualTo(rootAfterBase);
    }

    /**
     * A brand-new account created inside the catch-up window whose account hash falls in a PENDING
     * range must receive all of its storage slots: the account did not exist at the old pivot, so
     * the BAL delta IS its complete storage.
     *
     * <pre>
     * gen -- 1 (P=100, P.sp1=1)                 old pivot
     *     -- 2 (N=50+code, N.s1=7, N.s2=8)      new pivot; N created after the old pivot
     * local: P pending with sp1 downloaded; N's range pending, N never persisted
     * </pre>
     */
    @Test
    void appliesAllSlotsForNewAccountsInPendingRanges() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      final Block block1 =
          b.appendBlockWithBal(
              b.header(0),
              b.merge(
                  b.balWithBalances(Map.of(PETE, Wei.of(100))),
                  b.balWithStorageChanges(PETE, Map.of(SP1, UInt256.valueOf(1)))),
              1L);
      final Block block2 =
          b.appendBlockWithBal(
              block1.getHeader(),
              b.merge(
                  b.balWithBalances(Map.of(NEW_CONTRACT, Wei.of(50))),
                  b.balWithCodeChange(NEW_CONTRACT, NEW_CONTRACT_CODE),
                  b.balWithStorageChanges(
                      NEW_CONTRACT, Map.of(S1, UInt256.valueOf(7), S2, UInt256.valueOf(8)))),
              2L);

      final DownloadedAccountRangeTracker accountTracker =
          accountRangeTracker(false, PETE, NEW_CONTRACT);
      final DownloadedStorageRangeTracker storageTracker = downloadedSlots(PETE, SP1);

      applyTo(localCoordinator, b, 1, 1, accountTracker, storageTracker);
      applyTo(
          canonicalCoordinator, b, 1, 2, fullAccountRange(), new DownloadedStorageRangeTracker());

      applier(localCoordinator, b)
          .applyBlockAccessLists(2L, block2.getHeader().getNumber(), accountTracker, storageTracker)
          .commit();

      // The new account is fully assembled from the BAL: scalars, code, and every slot.
      final PmtStateTrieAccountValue newContract = readAccount(NEW_CONTRACT);
      assertThat(newContract.getBalance()).isEqualTo(Wei.of(50));
      assertThat(newContract.getCodeHash()).isEqualTo(Hash.hash(NEW_CONTRACT_CODE));
      assertThat(readCode(NEW_CONTRACT)).hasValue(NEW_CONTRACT_CODE);
      assertThat(readStorageSlot(NEW_CONTRACT, S1)).hasValue(UInt256.valueOf(7));
      assertThat(readStorageSlot(NEW_CONTRACT, S2)).hasValue(UInt256.valueOf(8));

      // Its storage root is recomputed locally and matches the canonical one at the new pivot.
      assertThat(newContract.getStorageRoot()).isNotEqualTo(Hash.EMPTY_TRIE_HASH);
      assertThat(newContract.getStorageRoot())
          .isEqualTo(readAccount(canonicalCoordinator, NEW_CONTRACT).getStorageRoot());

      // The pending account from the old pivot is untouched by the window.
      assertThat(readAccount(PETE).getBalance()).isEqualTo(Wei.of(100));
      assertThat(readStorageSlot(PETE, SP1)).hasValue(UInt256.valueOf(1));

      // The local state is exactly the canonical one at the new pivot.
      assertThat(worldStateRoot(localCoordinator)).isEqualTo(worldStateRoot(canonicalCoordinator));
    }

    /**
     * Blocks for which the schedule does not enable BALs are skipped, even if a BAL is stored.
     *
     * <pre>
     * gen -- 1 (A=100) -- 2 (A=200); catch-up applied with a BAL-disabled schedule
     * </pre>
     */
    @Test
    void skipsBlocksWhereBalsAreNotEnabled() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      final Block block1 =
          b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
      final Block block2 =
          b.appendBlockWithBal(
              block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(200))), 2L);

      applyTo(localCoordinator, b, 1, 1, fullAccountRange(), new DownloadedStorageRangeTracker());

      new SnapV2BlockAccessListApplier(
              localCoordinator, b.blockchain(), ReorgBlockchainBuilder.balDisabledSchedule())
          .applyBlockAccessLists(
              2L,
              block2.getHeader().getNumber(),
              fullAccountRange(),
              new DownloadedStorageRangeTracker())
          .commit();

      assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(100));
    }
  }

  // ---------------------------------------------------------------------------
  // collectPendingStorageAffected: which pending accounts need root re-fetches.
  // ---------------------------------------------------------------------------

  @Nested
  class CollectPendingStorageAffected {

    /**
     * Only pending accounts with storage writes inside the window are collected: completed accounts
     * recompute their roots locally, scalar-only changes do not affect storage roots, reads are not
     * writes, and blocks outside the window are ignored.
     *
     * <pre>
     * gen -- 1 (all=100) -- 2 (A[completed].s1=1; Pe[pending].sp1=1; Pa[pending]=500; Pr[pending] reads s1)
     *                  \-- 3 (G[pending].s1=9)
     * </pre>
     */
    @Test
    void returnsOnlyPendingAccountsWithStorageChangesInWindow() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      final Block block1 =
          b.appendBlockWithBal(
              b.header(0),
              b.balWithBalances(
                  Map.of(
                      ALICE, Wei.of(100),
                      PETE, Wei.of(100),
                      PAULA, Wei.of(100),
                      GRACE, Wei.of(100))),
              1L);
      final Block block2 =
          b.appendBlockWithBal(
              block1.getHeader(),
              b.merge(
                  b.balWithStorageChanges(ALICE, Map.of(S1, UInt256.valueOf(1))),
                  b.balWithStorageChanges(PETE, Map.of(SP1, UInt256.valueOf(1))),
                  b.balWithBalances(Map.of(PAULA, Wei.of(500))),
                  b.balWithStorageReads(GRACE, S1)),
              2L);
      final Block block3 =
          b.appendBlockWithBal(
              block2.getHeader(),
              b.balWithStorageChanges(DAVE, Map.of(S1, UInt256.valueOf(9))),
              3L);

      final DownloadedAccountRangeTracker accountTracker = accountRangeTracker(true, ALICE);
      addPendingAccounts(accountTracker, PETE, PAULA, GRACE, DAVE);

      final SnapV2BlockAccessListApplier applier = applier(localCoordinator, b);

      // Window covering only block 2: just Pete (pending + storage write).
      assertThat(
              applier.collectPendingStorageAffected(
                  block1.getHeader(), block2.getHeader(), accountTracker))
          .containsExactlyInAnyOrder(PETE.addressHash());

      // Window extended to block 3: Dave's storage write enters the window.
      assertThat(
              applier.collectPendingStorageAffected(
                  block1.getHeader(), block3.getHeader(), accountTracker))
          .containsExactlyInAnyOrder(PETE.addressHash(), DAVE.addressHash());
    }

    /**
     * An account that falls inside a pending range but was never persisted locally (e.g. created
     * after the old pivot) is still collected: the root re-fetch resolves it at the new pivot, and
     * {@code patchStorageRoots} skips it gracefully if it is absent locally.
     *
     * <pre>
     * gen -- 1 (empty) -- 2 (Gh[pending range, never persisted].s1=7)
     * </pre>
     */
    @Test
    void includesPendingRangeAccountsNotYetPersisted() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
      final Block block2 =
          b.appendBlockWithBal(
              block1.getHeader(),
              b.balWithStorageChanges(GHOST, Map.of(S1, UInt256.valueOf(7))),
              2L);

      final DownloadedAccountRangeTracker accountTracker = accountRangeTracker(false, GHOST);
      assertThat(accountExists(GHOST)).isFalse();

      assertThat(
              applier(localCoordinator, b)
                  .collectPendingStorageAffected(
                      block1.getHeader(), block2.getHeader(), accountTracker))
          .containsExactlyInAnyOrder(GHOST.addressHash());
    }

    /**
     * The same header-commitment integrity check as the apply path: a stored BAL that does not
     * match the header's balHash aborts the collection.
     *
     * <pre>
     * gen -- 1 -- 2 (header commits to BAL(A=80) but stores BAL(B=1))
     * </pre>
     */
    @Test
    void throwsOnBalHashMismatch() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
      final Block block2 =
          b.appendCanonicalWithMismatchedBal(
              block1.getHeader(),
              b.balWithStorageChanges(ALICE, Map.of(S1, UInt256.valueOf(1))),
              b.balWithBalances(Map.of(BOB, Wei.ONE)),
              2L);

      final SnapV2BlockAccessListApplier applier = applier(localCoordinator, b);

      assertThatThrownBy(
              () ->
                  applier.collectPendingStorageAffected(
                      block1.getHeader(), block2.getHeader(), accountRangeTracker(false, ALICE)))
          .isInstanceOf(WorldStateDownloaderException.class)
          .hasMessageContaining("BAL hash mismatch");
    }
  }

  // ---------------------------------------------------------------------------
  // patchStorageRoots: repairing stale pending-account storage roots.
  // ---------------------------------------------------------------------------

  @Nested
  class PatchStorageRoots {

    /**
     * Patching rewrites only accounts whose local storage root is stale: up-to-date accounts are
     * skipped, accounts missing locally are skipped, and the rewritten leaf lands in both the flat
     * db and the account trie — bringing the local world state root to the canonical one.
     *
     * <pre>
     * gen -- 1 (P=100, P.sp1=1; C=50) -- 2 (P.sp2=2) -- 3 (empty)
     * local: C completed; P pending with only sp1 downloaded -> P's storage root is stale
     * correctRoots (as fetched at the new pivot): P + C + Ghost (not persisted locally)
     * </pre>
     *
     * Blocks 1 and 2 must be applied in separate windows: a single [1,2] window would treat Pete as
     * new and land sp2 locally as well.
     */
    @Test
    void rewritesOnlyStaleRoots() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      // Block 1 creates Pete with the downloaded slot only; block 2 introduces sp2, which the slot
      // guard then skips locally because Pete already exists.
      final Block block1 =
          b.appendBlockWithBal(
              b.header(0),
              b.merge(
                  b.balWithBalances(Map.of(PETE, Wei.of(100), CAROL, Wei.of(50))),
                  b.balWithStorageChanges(PETE, Map.of(SP1, UInt256.valueOf(1)))),
              1L);
      final Block block2 =
          b.appendBlockWithBal(
              block1.getHeader(),
              b.balWithStorageChanges(PETE, Map.of(SP2, UInt256.valueOf(2))),
              2L);
      final Block block3 = b.appendBlockWithBal(block2.getHeader(), b.emptyBal(), 3L);

      final DownloadedAccountRangeTracker accountTracker = accountRangeTracker(true, CAROL);
      addPendingAccounts(accountTracker, PETE);
      final DownloadedStorageRangeTracker storageTracker = downloadedSlots(PETE, SP1);
      applyTo(localCoordinator, b, 1, 1, accountTracker, storageTracker);
      applyTo(localCoordinator, b, 2, 2, accountTracker, storageTracker);
      applyTo(
          canonicalCoordinator, b, 1, 3, fullAccountRange(), new DownloadedStorageRangeTracker());

      final Hash staleRoot = readAccount(PETE).getStorageRoot();
      final Hash canonicalPeteRoot = readAccount(canonicalCoordinator, PETE).getStorageRoot();
      assertThat(staleRoot).isNotEqualTo(canonicalPeteRoot);

      final SnapV2BlockAccessListApplier applier = applier(localCoordinator, b);
      // An apply over the empty block yields an empty batch, as in production when no persisted
      // account was touched; patching operates on that same uncommitted batch.
      final var batch =
          applier.applyBlockAccessLists(
              3L,
              block3.getHeader().getNumber(),
              accountTracker,
              new DownloadedStorageRangeTracker());

      final Map<Hash, Bytes32> correctRoots =
          Map.of(
              PETE.addressHash(),
              Bytes32.wrap(canonicalPeteRoot.getBytes()),
              CAROL.addressHash(),
              Bytes32.wrap(readAccount(CAROL).getStorageRoot().getBytes()), // already up to date
              GHOST.addressHash(),
              Bytes32.random()); // not present locally

      final int patched = applier.patchStorageRoots(batch, correctRoots);
      batch.commit();

      // Only Pete was rewritten.
      assertThat(patched).isEqualTo(1);
      assertThat(readAccount(PETE).getStorageRoot()).isEqualTo(canonicalPeteRoot);
      // Untouched fields are preserved.
      assertThat(readAccount(PETE).getBalance()).isEqualTo(Wei.of(100));
      assertThat(readAccount(CAROL).getBalance()).isEqualTo(Wei.of(50));
      assertThat(accountExists(GHOST)).isFalse();

      // The account trie leaf carries the corrected root: full root equality with the reference.
      assertThat(worldStateRoot(localCoordinator)).isEqualTo(worldStateRoot(canonicalCoordinator));
    }

    /** An empty patch set is a no-op. */
    @Test
    void noOpForEmptyPatchSet() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
      applyTo(localCoordinator, b, 1, 1, fullAccountRange(), new DownloadedStorageRangeTracker());
      final Hash rootBefore = worldStateRoot(localCoordinator);

      final SnapV2BlockAccessListApplier applier = applier(localCoordinator, b);
      final var batch =
          applier.applyBlockAccessLists(
              2L, 1L, fullAccountRange(), new DownloadedStorageRangeTracker());

      assertThat(applier.patchStorageRoots(batch, Map.of())).isZero();
      batch.commit();

      assertThat(worldStateRoot(localCoordinator)).isEqualTo(rootBefore);
      assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(100));
    }
  }

  // ---------------------------------------------------------------------------
  // Integration: the full pivot catch-up sequence reaches the canonical state root.
  // ---------------------------------------------------------------------------

  @Nested
  class Integration {

    /**
     * The exact {@code SnapV2WorldDownloadState.finishPivotCatchup} sequence for a clean pivot
     * advance — collect pending-affected accounts, fetch their roots at the new pivot, apply the
     * BALs, patch the stale roots, commit — over a partially downloaded state. Afterwards the local
     * account trie root equals the canonical state root at the new pivot, even though the genuinely
     * pending account's flat storage still holds only the downloaded subset of its slots.
     *
     * <p>The window exercises every account role at once: a completed account receiving scalars, a
     * new storage slot and a nonce across multiple blocks (Carol); an untouched account (Dave); an
     * account created after the old pivot inside a completed range (Grace); a pending existing
     * account whose partial storage leaves its root stale and thus patched (Pete); and a brand-new
     * account whose hash falls in a pending range, whose BAL delta is its complete storage, whose
     * root is recomputed locally and therefore NOT patched (the new contract). The single {@code
     * patched == 1} proves the contrast: only Pete needed a root re-fetch.
     *
     * <pre>
     * gen -- 1 (C=1000,C.s1=1; P=100,P.sp1=1; D=75)
     *     -- 2 (P.sp2=2)
     *     -- 3 (C=2000,C.s1=11,C.s2=22; P=300,P.sp1=10,P.sp2=20; G=50)
     *     -- 4 (C.nonce=7; N=50+code,N.s1=7,N.s2=8)
     * local: C, D, G completed; P pending (sp1 downloaded); N's range pending, never persisted
     * </pre>
     *
     * Blocks 1 and 2 must be applied in separate windows: a single [1,2] window would treat Pete as
     * new and land sp2 locally as well.
     */
    @Test
    void pivotCatchUpReachesCanonicalStateRoot() {
      final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

      // Block 1 creates Pete with the downloaded slot only; block 2 introduces sp2, which the slot
      // guard then skips locally because Pete already exists.
      final Block block1 =
          b.appendBlockWithBal(
              b.header(0),
              b.merge(
                  b.balWithBalances(
                      Map.of(CAROL, Wei.of(1000), PETE, Wei.of(100), DAVE, Wei.of(75))),
                  b.balWithStorageChanges(CAROL, Map.of(S1, UInt256.valueOf(1))),
                  b.balWithStorageChanges(PETE, Map.of(SP1, UInt256.valueOf(1)))),
              1L);
      final Block block2 =
          b.appendBlockWithBal(
              block1.getHeader(),
              b.balWithStorageChanges(PETE, Map.of(SP2, UInt256.valueOf(2))),
              2L);
      final Block block3 =
          b.appendBlockWithBal(
              block2.getHeader(),
              b.merge(
                  b.balWithBalances(
                      Map.of(CAROL, Wei.of(2000), PETE, Wei.of(300), GRACE, Wei.of(50))),
                  b.balWithStorageChanges(
                      CAROL, Map.of(S1, UInt256.valueOf(11), S2, UInt256.valueOf(22))),
                  b.balWithStorageChanges(
                      PETE, Map.of(SP1, UInt256.valueOf(10), SP2, UInt256.valueOf(20)))),
              3L);
      // Block 4 carries the nonce change and deploys the new contract inside a pending range.
      final Block block4 =
          b.appendBlockWithBal(
              block3.getHeader(),
              b.merge(
                  b.balWithNonceChange(CAROL, 7L),
                  b.balWithBalances(Map.of(NEW_CONTRACT, Wei.of(50))),
                  b.balWithCodeChange(NEW_CONTRACT, NEW_CONTRACT_CODE),
                  b.balWithStorageChanges(
                      NEW_CONTRACT, Map.of(S1, UInt256.valueOf(7), S2, UInt256.valueOf(8)))),
              4L);

      final DownloadedAccountRangeTracker accountTracker =
          accountRangeTracker(true, CAROL, DAVE, GRACE);
      addPendingAccounts(accountTracker, PETE, NEW_CONTRACT);
      // The partial download at the old pivot: Carol, Dave and Grace's ranges are complete; Pete's
      // range is pending and only his sp1 slot has been downloaded; the new contract's range is
      // also pending and nothing of it exists locally yet.
      final DownloadedStorageRangeTracker storageTracker = downloadedSlots(PETE, SP1);
      applyTo(localCoordinator, b, 1, 1, accountTracker, storageTracker);
      applyTo(localCoordinator, b, 2, 2, accountTracker, storageTracker);

      // The canonical reference state at the new pivot.
      applyTo(
          canonicalCoordinator, b, 1, 4, fullAccountRange(), new DownloadedStorageRangeTracker());

      final SnapV2BlockAccessListApplier applier = applier(localCoordinator, b);

      // 1. Which pending accounts may hold stale storage roots after the catch-up? Both Pete and
      // the new contract had storage writes inside the window.
      final Set<Hash> pendingAffected =
          applier.collectPendingStorageAffected(
              block2.getHeader(), block4.getHeader(), accountTracker);
      assertThat(pendingAffected)
          .containsExactlyInAnyOrder(PETE.addressHash(), NEW_CONTRACT.addressHash());

      // 2. Fetch the correct roots at the new pivot. Production does this with proof-verified
      // GetAccountRange requests against peers; here the canonical reference state plays that role.
      final Map<Hash, Bytes32> fetchedRoots =
          Map.of(
              PETE.addressHash(),
              Bytes32.wrap(readAccount(canonicalCoordinator, PETE).getStorageRoot().getBytes()),
              NEW_CONTRACT.addressHash(),
              Bytes32.wrap(
                  readAccount(canonicalCoordinator, NEW_CONTRACT).getStorageRoot().getBytes()));

      // 3.+4. Apply the BALs of blocks 3..4 and patch the stale roots on the same batch.
      final var batch =
          applier.applyBlockAccessLists(
              3L, block4.getHeader().getNumber(), accountTracker, storageTracker);
      final int patched = applier.patchStorageRoots(batch, fetchedRoots);
      batch.commit();

      // Only Pete's root was stale: the new contract's root was recomputed locally from its
      // complete BAL delta and already matches the fetched canonical root.
      assertThat(patched).isEqualTo(1);

      // Completed account: every change applied, including the new storage slot and the nonce.
      final PmtStateTrieAccountValue carol = readAccount(CAROL);
      assertThat(carol.getBalance()).isEqualTo(Wei.of(2000));
      assertThat(carol.getNonce()).isEqualTo(7L);
      assertThat(readStorageSlot(CAROL, S1)).hasValue(UInt256.valueOf(11));
      assertThat(readStorageSlot(CAROL, S2)).hasValue(UInt256.valueOf(22));

      // Untouched by the catch-up BALs.
      assertThat(readAccount(DAVE).getBalance()).isEqualTo(Wei.of(75));

      // Created after the old pivot, inside a completed range.
      assertThat(readAccount(GRACE).getBalance()).isEqualTo(Wei.of(50));

      // Pending account: scalar and downloaded-slot updates applied, never-downloaded slot absent
      // locally, and the storage root is the patched canonical one.
      final PmtStateTrieAccountValue pete = readAccount(PETE);
      assertThat(pete.getBalance()).isEqualTo(Wei.of(300));
      assertThat(readStorageSlot(PETE, SP1)).hasValue(UInt256.valueOf(10));
      assertThat(readStorageSlot(PETE, SP2)).isEmpty();
      assertThat(pete.getStorageRoot())
          .isEqualTo(readAccount(canonicalCoordinator, PETE).getStorageRoot());

      // Brand-new account in a pending range: the BAL delta IS its complete storage, so every slot
      // lands and the locally recomputed root already equals the canonical one — no patch needed.
      final PmtStateTrieAccountValue newContract = readAccount(NEW_CONTRACT);
      assertThat(newContract.getBalance()).isEqualTo(Wei.of(50));
      assertThat(newContract.getCodeHash()).isEqualTo(Hash.hash(NEW_CONTRACT_CODE));
      assertThat(readCode(NEW_CONTRACT)).hasValue(NEW_CONTRACT_CODE);
      assertThat(readStorageSlot(NEW_CONTRACT, S1)).hasValue(UInt256.valueOf(7));
      assertThat(readStorageSlot(NEW_CONTRACT, S2)).hasValue(UInt256.valueOf(8));
      assertThat(newContract.getStorageRoot())
          .isEqualTo(readAccount(canonicalCoordinator, NEW_CONTRACT).getStorageRoot());

      // The whole point of snap/2: no final healing phase — the root already matches.
      assertThat(worldStateRoot(localCoordinator)).isEqualTo(worldStateRoot(canonicalCoordinator));
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static SnapV2BlockAccessListApplier applier(
      final WorldStateStorageCoordinator coordinator, final ReorgBlockchainBuilder b) {
    return new SnapV2BlockAccessListApplier(
        coordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());
  }

  private static void applyTo(
      final WorldStateStorageCoordinator coordinator,
      final ReorgBlockchainBuilder b,
      final long fromBlock,
      final long toBlock,
      final DownloadedAccountRangeTracker accountTracker,
      final DownloadedStorageRangeTracker storageTracker) {
    applier(coordinator, b)
        .applyBlockAccessLists(fromBlock, toBlock, accountTracker, storageTracker)
        .commit();
  }

  private PmtStateTrieAccountValue readAccount(final Address address) {
    return readAccount(localCoordinator, address);
  }

  private boolean accountExists(final Address address) {
    return accountExists(localCoordinator, address);
  }

  private Optional<UInt256> readStorageSlot(final Address address, final UInt256 slotKey) {
    return readStorageSlot(localCoordinator, address, slotKey);
  }

  private Optional<Bytes> readCode(final Address address) {
    return readCode(localCoordinator, address);
  }

  private static void addPendingAccounts(
      final DownloadedAccountRangeTracker tracker, final Address... accounts) {
    for (final Address account : accounts) {
      tracker.registerPending(accountHash(account), accountHash(account), 1);
    }
  }
}
