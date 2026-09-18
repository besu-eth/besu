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
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SnapV2BlockAccessListApplierForestTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  static Stream<StateHarness> harnesses() {
    return Stream.of(new BonsaiStateHarness(), new ForestStateHarness());
  }

  private static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("harnesses")
  void nonceOnlyChangeRetainsExistingBalance(final StateHarness h) {
    h.seedAccount(ALICE, 0L, Wei.of(500), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2 =
        b.appendCanonical(block1.getHeader(), b.balWithNonces(Map.of(ALICE, 7L)), 2L);

    final Bytes32 newRoot =
        new SnapV2BlockAccessListApplier(
                h.coordinator(), b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule())
            .applyBlockAccessLists(
                block1.getHeader().getNumber() + 1,
                block2.getHeader().getNumber(),
                h.forestStartRoot(),
                fullAccountRange(),
                new DownloadedStorageRangeTracker())
            .commit();
    h.updateAccountRoot(newRoot);

    assertThat(h.readAccount(ALICE)).isPresent();
    assertThat(h.readAccount(ALICE).get().getNonce()).isEqualTo(7L);
    assertThat(h.readAccount(ALICE).get().getBalance()).isEqualTo(Wei.of(500));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("harnesses")
  void appliesStorageSlotChangeAndRecomputesRoot(final StateHarness h) {
    final UInt256 slotKey = UInt256.valueOf(3);
    h.seedAccount(ALICE, 1L, Wei.of(10), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2 =
        b.appendCanonical(
            block1.getHeader(),
            b.balWithStorageChanges(ALICE, Map.of(slotKey, UInt256.valueOf(99))),
            2L);

    final DownloadedStorageRangeTracker storageTracker = new DownloadedStorageRangeTracker();
    final Bytes32 newRoot =
        new SnapV2BlockAccessListApplier(
                h.coordinator(), b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule())
            .applyBlockAccessLists(
                block1.getHeader().getNumber() + 1,
                block2.getHeader().getNumber(),
                h.forestStartRoot(),
                fullAccountRange(),
                storageTracker)
            .commit();
    h.updateAccountRoot(newRoot);

    assertThat(h.readStorageSlot(ALICE, slotKey)).hasValue(UInt256.valueOf(99));
    assertThat(h.readAccount(ALICE).get().getStorageRoot()).isNotEqualTo(Hash.EMPTY_TRIE_HASH);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("harnesses")
  void appliesCodeChange(final StateHarness h) {
    final Bytes newCode = Bytes.fromHexString("0x60016002");
    h.seedAccount(ALICE, 1L, Wei.of(10), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2 = b.appendCanonical(block1.getHeader(), b.balWithCode(ALICE, newCode), 2L);

    final Bytes32 newRoot =
        new SnapV2BlockAccessListApplier(
                h.coordinator(), b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule())
            .applyBlockAccessLists(
                block1.getHeader().getNumber() + 1,
                block2.getHeader().getNumber(),
                h.forestStartRoot(),
                fullAccountRange(),
                new DownloadedStorageRangeTracker())
            .commit();
    h.updateAccountRoot(newRoot);

    assertThat(h.readAccount(ALICE).get().getCodeHash()).isEqualTo(Hash.hash(newCode));
    assertThat(h.readCode(ALICE)).hasValue(newCode);
  }

  /**
   * A diverged storage slot (set only on the orphaned fork) is cleared by reorg corrections on both
   * Bonsai and Forest. Verifies that {@code fixDivergedSlots} reads the account via the MPT on
   * Forest and that storage-trie-node writes are no longer gated.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("harnesses")
  void reorgCorrectionClearsDivergedStorageSlot(final StateHarness h) {
    final UInt256 slotKey = UInt256.valueOf(1);
    final Hash slotHash = ReorgBlockchainBuilder.slotHash(slotKey);

    // Seed ALICE with slot = 100 (the orphaned-fork value); account starts with empty storage root.
    h.seedAccount(ALICE, 0L, Wei.of(50), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    h.seedStorageSlot(ALICE, slotKey, UInt256.valueOf(100));

    // Canonical at new pivot: slot is absent (canonical fork never wrote this slot).
    // The canonical account has empty storage root because the slot is gone.
    final PmtStateTrieAccountValue canonicalAlice =
        new PmtStateTrieAccountValue(0L, Wei.of(50), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    // Build plan: ALICE has one slot to fix, no full-account refetch.
    final ReorgPlan plan = planWithDivergedSlots(Map.of(ALICE.addressHash(), Set.of(slotHash)));

    // Fetched state: ALICE exists, slot is absent at the canonical pivot.
    final FetchedReorgState fetched =
        new FetchedReorgState(
            Map.of(ALICE.addressHash(), Optional.of(canonicalAlice)),
            Map.of(ALICE.addressHash(), Map.of(slotHash, Optional.empty())),
            Map.of());

    final SnapV2BlockAccessListApplier applier =
        new SnapV2BlockAccessListApplier(
            h.coordinator(),
            new ReorgBlockchainBuilder().blockchain(),
            ReorgBlockchainBuilder.balEnabledSchedule());

    // Use a fresh tracker (no downloads) so the storage-root consistency check is skipped.
    final ReorgRecoveryResult recovery =
        applier.applyReorgCorrections(
            plan,
            fetched,
            h.forestStartRoot(),
            new DownloadedAccountRangeTracker(),
            new DownloadedStorageRangeTracker());

    h.updateAccountRoot(recovery.finalAccountRoot());

    // The diverged slot must be absent after correction.
    assertThat(h.readStorageSlot(ALICE, slotKey)).isEmpty();
  }

  /**
   * A BAL that removes an account (absent at the new pivot) should result in the account being
   * absent after apply. On Forest, {@code deleteAccount} calls {@code accountTrie.remove(...)}
   * which must persist the removal correctly through the MPT.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("harnesses")
  void deleteAccountRemovesAccountFromTrie(final StateHarness h) {
    h.seedAccount(ALICE, 1L, Wei.of(100), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    final Hash aliceHash = ALICE.addressHash();

    // Plan: ALICE's canonical record must be re-fetched; pivot says she is absent.
    final ReorgPlan plan = planWithAccountsToRefetch(Set.of(aliceHash));
    final FetchedReorgState fetched =
        new FetchedReorgState(Map.of(aliceHash, Optional.empty()), Map.of(), Map.of());

    final SnapV2BlockAccessListApplier applier =
        new SnapV2BlockAccessListApplier(
            h.coordinator(),
            new ReorgBlockchainBuilder().blockchain(),
            ReorgBlockchainBuilder.balEnabledSchedule());

    final ReorgRecoveryResult recovery =
        applier.applyReorgCorrections(
            plan,
            fetched,
            h.forestStartRoot(),
            new DownloadedAccountRangeTracker(),
            new DownloadedStorageRangeTracker());

    h.updateAccountRoot(recovery.finalAccountRoot());

    assertThat(h.readAccount(ALICE)).isEmpty();
  }

  /**
   * A storage-root patch on a pending account must update the account-trie leaf on both Bonsai and
   * Forest. {@code patchStorageRoots} rewrites the account's storage root in the in-memory trie
   * (and the flat DB on Bonsai); committing the {@link SnapV2BlockAccessListApplier.BatchState}
   * must persist that rewrite.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("harnesses")
  void patchStorageRootsUpdatesAccountStorageRoot(final StateHarness h) {
    final Bytes32 newStorageRoot =
        Bytes32.fromHexString("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    h.seedAccount(ALICE, 1L, Wei.of(10), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    // Append a BAL that touches ALICE (nonce change) so she is written into the in-memory
    // account trie inside the BatchState — required for patchStorageRoots to find her there.
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2 =
        b.appendCanonical(block1.getHeader(), b.balWithNonces(Map.of(ALICE, 5L)), 2L);

    final SnapV2BlockAccessListApplier applier =
        new SnapV2BlockAccessListApplier(
            h.coordinator(), b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());

    // Get the BatchState without committing yet so we can patch before the commit.
    final var batch =
        applier.applyBlockAccessLists(
            block1.getHeader().getNumber() + 1,
            block2.getHeader().getNumber(),
            h.forestStartRoot(),
            fullAccountRange(),
            new DownloadedStorageRangeTracker());

    applier.patchStorageRoots(batch, Map.of(ALICE.addressHash(), newStorageRoot));

    final Bytes32 newRoot = batch.commit();
    h.updateAccountRoot(newRoot);

    assertThat(h.readAccount(ALICE)).isPresent();
    assertThat(h.readAccount(ALICE).get().getStorageRoot()).isEqualTo(Hash.wrap(newStorageRoot));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static ReorgPlan planWithDivergedSlots(
      final Map<Hash, Set<Hash>> divergedSlotsByAccount) {
    final BlockHeader ancestor = new BlockHeaderTestFixture().number(1).buildHeader();
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(2).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(3).buildHeader();
    return new ReorgPlan(ancestor, oldPivot, newPivot, Set.of(), divergedSlotsByAccount);
  }

  private static ReorgPlan planWithAccountsToRefetch(final Set<Hash> accountsToRefetch) {
    final BlockHeader ancestor = new BlockHeaderTestFixture().number(1).buildHeader();
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(2).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(3).buildHeader();
    return new ReorgPlan(ancestor, oldPivot, newPivot, accountsToRefetch, Map.of());
  }
}
