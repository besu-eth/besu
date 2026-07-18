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
import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SnapV2BlockAccessListApplier#applyBlockAccessLists} against real Bonsai flat
 * storage, in the reorg-recovery context: the seeded flat state reflects an orphaned fork, and
 * applying the canonical fork's BALs must overwrite or create the entries the canonical fork
 * touched, while leaving everything else unchanged — entries touched only on the orphaned fork
 * (corrected later by the re-fetch step) and entries untouched by either fork.
 */
class SnapV2BlockAccessListApplierReorgTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address BOB =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address CHARLIE =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address DAVE =
      Address.fromHexString("0x4444444444444444444444444444444444444444");
  private static final Address FRANK =
      Address.fromHexString("0x6666666666666666666666666666666666666666");
  private static final Address GRACE =
      Address.fromHexString("0x7777777777777777777777777777777777777777");

  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  private final BonsaiWorldStateKeyValueStorage bonsaiStorage =
      new BonsaiWorldStateKeyValueStorage(
          new InMemoryKeyValueStorageProvider(),
          new NoOpMetricsSystem(),
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  private final WorldStateStorageCoordinator coordinator =
      new WorldStateStorageCoordinator(bonsaiStorage);

  // ---------------------------------------------------------------------------
  // Core reorg application: which fork touched an entry decides its fate, and the apply window.
  // ---------------------------------------------------------------------------

  /**
   * The three core reorg outcomes in a single block: accounts touched on both forks are overwritten
   * from the canonical BAL, accounts touched only on the orphaned fork keep their stale value
   * pending re-fetch, and accounts untouched by either fork are left alone.
   *
   * <pre>
   * gen -- 1 +-- 2s (A=50, D=60)   stale
   *          +-- 2c (A=80)         canonical
   * seeded flat state (post-orphan): A=50, D=60, B=100
   * </pre>
   */
  @Test
  void appliesCanonicalWritesAndLeavesDivergedEntriesStale() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

    // Common ancestor = block 1.
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    // Orphaned fork block 2o: BAL sets Alice=50, Dave=60.
    b.appendStale(
        block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50), DAVE, Wei.of(60))), 2L);

    // Canonical fork block 2c (wins the reorg): BAL sets Alice=80. Dave untouched.
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    // Pre-seed flat state to reflect the state AFTER the orphaned fork was applied (the "current"
    // state when the reorg is detected): Alice=50, Dave=60, and Bob=100 (untouched by either fork).
    seedAccount(ALICE, Wei.of(50));
    seedAccount(DAVE, Wei.of(60));
    seedAccount(BOB, Wei.of(100));

    // All three accounts are in downloaded ranges.
    final DownloadedAccountRangeTracker accountTracker = fullAccountRange();
    final DownloadedStorageRangeTracker storageTracker = new DownloadedStorageRangeTracker();

    // Apply canonical BALs from the common ancestor + 1 (= block 2).
    applier(b)
        .applyBlockAccessLists(
            block1.getHeader().getNumber() + 1,
            block2c.getHeader().getNumber(),
            accountTracker,
            storageTracker);

    // Alice: touched on both forks. Canonical BAL overwrites with the correct value (80).
    assertThat(readBalance(ALICE)).isEqualTo(Wei.of(80));
    // Dave: touched only on the orphaned fork -> diverged. The applier does not touch it; it
    // retains its (incorrect) orphaned value. A later re-fetch step will correct this.
    assertThat(readBalance(DAVE)).isEqualTo(Wei.of(60));
    // Bob: untouched by either fork -> unchanged.
    assertThat(readBalance(BOB)).isEqualTo(Wei.of(100));
  }

  /**
   * An account created only on the canonical fork must be created locally, even though nothing was
   * ever downloaded for it (it did not exist at the old pivot).
   *
   * <pre>
   * gen -- 1 +-- 2s (A=50)          stale
   *          +-- 2c (A=80, G=50)    canonical
   * seeded flat state (post-orphan): A=50 only
   * </pre>
   *
   * Grace is built entirely from the canonical BAL: BAL balance, zero nonce, no code, empty storage
   * root.
   */
  @Test
  void createsCanonicalOnlyAccounts() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.balWithBalances(Map.of(ALICE, Wei.of(80), GRACE, Wei.of(50))),
            2L);

    // Post-orphan flat state: only Alice exists; Grace was never seen on either fork at download
    // time.
    seedAccount(ALICE, Wei.of(50));
    assertThat(accountExists(GRACE)).isFalse();

    applier(b)
        .applyBlockAccessLists(
            block1.getHeader().getNumber() + 1,
            block2c.getHeader().getNumber(),
            fullAccountRange(),
            new DownloadedStorageRangeTracker());

    assertThat(readBalance(ALICE)).isEqualTo(Wei.of(80));
    final PmtStateTrieAccountValue grace = readAccount(GRACE);
    assertThat(grace.getBalance()).isEqualTo(Wei.of(50));
    assertThat(grace.getNonce()).isZero();
    assertThat(grace.getCodeHash()).isEqualTo(Hash.EMPTY);
    assertThat(grace.getStorageRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
  }

  /**
   * A multi-block apply window accumulates changes: later canonical blocks overwrite earlier ones
   * (last write wins), and accounts first touched in a later block of the window are still applied.
   *
   * <pre>
   * gen -- 1 +-- 2s (A=50)                       stale
   *          +-- 2c (A=80) -- 3c (A=90, B=100)   canonical
   * seeded flat state (post-orphan): A=50 only
   * </pre>
   *
   * Window [2, 3]: Alice ends at 90 (not 80), and Bob — first touched in block 3 — is created.
   */
  @Test
  void appliesLastWriteAcrossMultipleCanonicalBlocks() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);
    final Block block3c =
        b.appendCanonical(
            block2c.getHeader(),
            b.balWithBalances(Map.of(ALICE, Wei.of(90), BOB, Wei.of(100))),
            3L);

    seedAccount(ALICE, Wei.of(50));

    applier(b)
        .applyBlockAccessLists(
            block1.getHeader().getNumber() + 1,
            block3c.getHeader().getNumber(),
            fullAccountRange(),
            new DownloadedStorageRangeTracker());

    // Alice was written by both canonical blocks: the latest value wins.
    assertThat(readBalance(ALICE)).isEqualTo(Wei.of(90));
    // Bob appears only in block 3: the window must cover every block, not just the first.
    assertThat(readBalance(BOB)).isEqualTo(Wei.of(100));
  }

  /**
   * The fromBlock parameter is honoured rather than always deriving from a pivot + 1: applying a
   * single-block window touches only that block's BAL.
   *
   * <pre>
   * gen -- 1 -- 2 (A=70)   canonical; apply window [2,2]
   * seeded flat state: A=10
   * </pre>
   */
  @Test
  void fromBlockGeneralizationExcludesBlocksBeforeCommonAncestor() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    final Block block2 =
        b.appendBlockWithBal(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(70))), 2L);

    seedAccount(ALICE, Wei.of(10));

    applier(b)
        .applyBlockAccessLists(
            2L,
            block2.getHeader().getNumber(),
            fullAccountRange(),
            new DownloadedStorageRangeTracker());

    assertThat(readBalance(ALICE)).isEqualTo(Wei.of(70));
  }

  // ---------------------------------------------------------------------------
  // Storage slots in reorgs: canonical writes applied, diverged slots left stale.
  // ---------------------------------------------------------------------------

  /**
   * The slot-level analogue of the account-level outcomes: a slot written on both forks is
   * overwritten from the canonical BAL (flat value and storage root), while a slot written only on
   * the orphaned fork keeps its stale value pending re-fetch.
   *
   * <pre>
   * gen -- 1 +-- 2s (F:s1=200, F:s2=200)   stale
   *          +-- 2c (F:s1=111)             canonical
   * seeded flat state (post-orphan): F with s1=200, s2=200
   * </pre>
   */
  @Test
  void appliesCanonicalSlotWritesAndLeavesDivergedSlotsStale() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final UInt256 slot1 = UInt256.valueOf(1);
    final UInt256 slot2 = UInt256.valueOf(2);

    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    b.appendStale(
        block1.getHeader(),
        b.balWithStorageChanges(
            FRANK, Map.of(slot1, UInt256.valueOf(200), slot2, UInt256.valueOf(200))),
        2L);
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.balWithStorageChanges(FRANK, Map.of(slot1, UInt256.valueOf(111))),
            2L);

    // Post-orphan flat state: Frank holds both orphaned slot values.
    seedAccount(FRANK, Wei.of(100));
    seedStorageSlot(FRANK, slot1, UInt256.valueOf(200));
    seedStorageSlot(FRANK, slot2, UInt256.valueOf(200));

    applier(b)
        .applyBlockAccessLists(
            block1.getHeader().getNumber() + 1,
            block2c.getHeader().getNumber(),
            fullAccountRange(),
            new DownloadedStorageRangeTracker());

    // s1: canonical write applied, and the account's storage root moved off the empty trie.
    assertThat(readStorageSlot(FRANK, slot1)).hasValue(UInt256.valueOf(111));
    assertThat(readAccount(FRANK).getStorageRoot()).isNotEqualTo(Hash.EMPTY_TRIE_HASH);
    // s2: diverged — stale orphaned value retained; a later re-fetch step will correct it.
    assertThat(readStorageSlot(FRANK, slot2)).hasValue(UInt256.valueOf(200));
  }

  /**
   * A canonical slot write of zero deletes the slot from flat storage.
   *
   * <pre>
   * gen -- 1 +-- 2s (F:s1=200)   stale
   *          +-- 2c (F:s1=0)     canonical
   * seeded flat state (post-orphan): F with s1=200
   * </pre>
   */
  @Test
  void removesSlotOnCanonicalZeroWrite() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final UInt256 slot1 = UInt256.valueOf(1);

    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    b.appendStale(
        block1.getHeader(),
        b.balWithStorageChanges(FRANK, Map.of(slot1, UInt256.valueOf(200))),
        2L);
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(), b.balWithStorageChanges(FRANK, Map.of(slot1, UInt256.ZERO)), 2L);

    seedAccount(FRANK, Wei.of(100));
    seedStorageSlot(FRANK, slot1, UInt256.valueOf(200));

    applier(b)
        .applyBlockAccessLists(
            block1.getHeader().getNumber() + 1,
            block2c.getHeader().getNumber(),
            fullAccountRange(),
            new DownloadedStorageRangeTracker());

    assertThat(readStorageSlot(FRANK, slot1)).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Range scoping: application must respect the persisted account ranges.
  // ---------------------------------------------------------------------------

  /**
   * Canonical BAL entries for accounts outside the persisted ranges are skipped entirely — in
   * particular, a canonical-only account whose range was never downloaded must NOT be created.
   *
   * <pre>
   * gen -- 1 +-- 2s (A=50)          stale
   *          +-- 2c (A=80, D=70)    canonical
   * persisted account ranges: [A,A] only; seeded flat state: A=50
   * </pre>
   *
   * Dave is deferred: his range will download the canonical value directly at the new pivot.
   */
  @Test
  void skipsAccountsOutsidePersistedRanges() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80), DAVE, Wei.of(70))), 2L);

    seedAccount(ALICE, Wei.of(50));

    // Only Alice's single-account range has been downloaded.
    final DownloadedAccountRangeTracker accountTracker = persistedAccounts(ALICE);

    applier(b)
        .applyBlockAccessLists(
            block1.getHeader().getNumber() + 1,
            block2c.getHeader().getNumber(),
            accountTracker,
            new DownloadedStorageRangeTracker());

    assertThat(readBalance(ALICE)).isEqualTo(Wei.of(80));
    assertThat(accountExists(DAVE)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // BAL content: nonce and code changes, with untouched fields preserved.
  // ---------------------------------------------------------------------------

  /**
   * Nonce and code changes from the canonical BAL are applied, while fields the BAL does not touch
   * keep their current (orphaned-fork) values.
   *
   * <pre>
   * gen -- 1 +-- 2s (A, C balance touches)   stale
   *          +-- 2c (A nonce=7; C code)      canonical
   * seeded flat state (post-orphan): A=50, C=1
   * </pre>
   */
  @Test
  void appliesNonceAndCodeChanges() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Bytes code = Bytes.of(0x60, 0x80);

    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    b.appendStale(block1.getHeader(), b.balWithBalanceTouches(ALICE, CHARLIE), 2L);
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.merge(b.balWithNonceChange(ALICE, 7L), b.balWithCodeChange(CHARLIE, code)),
            2L);

    seedAccount(ALICE, Wei.of(50));
    seedAccount(CHARLIE, Wei.of(1));

    applier(b)
        .applyBlockAccessLists(
            block1.getHeader().getNumber() + 1,
            block2c.getHeader().getNumber(),
            fullAccountRange(),
            new DownloadedStorageRangeTracker());

    // Alice: nonce applied, balance (untouched by the canonical BAL) preserved.
    final PmtStateTrieAccountValue alice = readAccount(ALICE);
    assertThat(alice.getNonce()).isEqualTo(7L);
    assertThat(alice.getBalance()).isEqualTo(Wei.of(50));

    // Charlie: code stored and code hash updated (the "deployed on both forks" case).
    final PmtStateTrieAccountValue charlie = readAccount(CHARLIE);
    assertThat(charlie.getCodeHash()).isEqualTo(Hash.hash(code));
    assertThat(readCode(CHARLIE)).hasValue(code);
  }

  // ---------------------------------------------------------------------------
  // Failure paths: integrity and availability of the data being applied.
  // ---------------------------------------------------------------------------

  /**
   * A stored BAL that does not match the header's balHash is rejected.
   *
   * <pre>
   * gen -- 1 +-- 2s (A=50)   stale
   *          +-- 2c (A=80)   canonical, but the STORED BAL says (B=1) -> hash mismatch
   * </pre>
   *
   * WorldStateDownloaderException — the same integrity check snap/2 applies to peer-served BALs,
   * here firing against corrupted local data.
   */
  @Test
  void throwsOnBalHashMismatch() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonicalWithMismatchedBal(
            block1.getHeader(),
            b.balWithBalances(Map.of(ALICE, Wei.of(80))),
            b.balWithBalances(Map.of(BOB, Wei.ONE)),
            2L);

    assertThatThrownBy(
            () ->
                applier(b)
                    .applyBlockAccessLists(
                        block1.getHeader().getNumber() + 1,
                        block2c.getHeader().getNumber(),
                        fullAccountRange(),
                        new DownloadedStorageRangeTracker()))
        .isInstanceOf(WorldStateDownloaderException.class)
        .hasMessageContaining("BAL hash mismatch");
  }

  /**
   * A canonical block in the apply window whose BAL has been pruned locally aborts the application.
   *
   * <pre>
   * gen -- 1 +-- 2s (empty)   stale
   *          +-- 2c           canonical, BAL NOT stored
   * </pre>
   *
   * IllegalStateException from loadBal.
   */
  @Test
  void throwsWhenAppliedCanonicalBalIsLocallyPruned() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);

    b.appendStale(block1.getHeader(), b.emptyBal(), 2L);
    final Block block2c = b.appendCanonicalWithoutStoringBal(block1.getHeader(), b.emptyBal(), 2L);

    assertThatThrownBy(
            () ->
                applier(b)
                    .applyBlockAccessLists(
                        block1.getHeader().getNumber() + 1,
                        block2c.getHeader().getNumber(),
                        fullAccountRange(),
                        new DownloadedStorageRangeTracker()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Missing BAL");
  }

  /**
   * The apply window must not run past the locally available canonical chain.
   *
   * <pre>
   * gen -- 1 -- 2   canonical chain head; apply window [2, 5]
   * </pre>
   *
   * Block 3's header is not available locally: IllegalStateException from loadBlockHeader.
   */
  @Test
  void throwsWhenApplyWindowExceedsLocalChain() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    b.appendBlockWithBal(block1.getHeader(), b.emptyBal(), 2L);

    assertThatThrownBy(
            () ->
                applier(b)
                    .applyBlockAccessLists(
                        2L, 5L, fullAccountRange(), new DownloadedStorageRangeTracker()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Missing block header");
  }

  // ---------------------------------------------------------------------------
  // Helpers: applier factory, tracker factories, and flat-state seed/read.
  // ---------------------------------------------------------------------------

  private SnapV2BlockAccessListApplier applier(final ReorgBlockchainBuilder b) {
    return new SnapV2BlockAccessListApplier(
        coordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());
  }

  private static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }

  /** Completed single-account ranges for exactly the given accounts. */
  private static DownloadedAccountRangeTracker persistedAccounts(final Address... accounts) {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    for (final Address account : accounts) {
      final Bytes32 accountHash = Bytes32.wrap(account.addressHash().getBytes());
      tracker.registerPending(accountHash, accountHash, 0);
    }
    return tracker;
  }

  private void seedAccount(final Address address, final Wei balance) {
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    final PmtStateTrieAccountValue account =
        new PmtStateTrieAccountValue(0L, balance, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final Bytes encoded = RLP.encode(account::writeTo);
    applyForStrategy(
        updater,
        bonsai -> bonsai.putAccountInfoState(address.addressHash(), encoded),
        forest -> {});
    updater.commit();
  }

  private void seedStorageSlot(final Address address, final UInt256 slotKey, final UInt256 value) {
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    applyForStrategy(
        updater,
        bonsai ->
            bonsai.putStorageValueBySlotHash(
                address.addressHash(), ReorgBlockchainBuilder.slotHash(slotKey), value.toBytes()),
        forest -> {});
    updater.commit();
  }

  private Wei readBalance(final Address address) {
    return readAccount(address).getBalance();
  }

  private PmtStateTrieAccountValue readAccount(final Address address) {
    return PmtStateTrieAccountValue.readFrom(RLP.input(readAccountBytes(address).orElseThrow()));
  }

  private boolean accountExists(final Address address) {
    return readAccountBytes(address).isPresent();
  }

  private Optional<Bytes> readAccountBytes(final Address address) {
    return coordinator.applyForStrategy(
        bonsai -> bonsai.getAccount(address.addressHash()), forest -> Optional.<Bytes>empty());
  }

  private Optional<UInt256> readStorageSlot(final Address address, final UInt256 slotKey) {
    return coordinator
        .applyForStrategy(
            bonsai ->
                bonsai.getStorageValueByStorageSlotKey(
                    address.addressHash(), new StorageSlotKey(slotKey)),
            forest -> Optional.<Bytes>empty())
        .map(UInt256::fromBytes);
  }

  private Optional<Bytes> readCode(final Address address) {
    final PmtStateTrieAccountValue account = readAccount(address);
    return coordinator.applyForStrategy(
        bonsai -> bonsai.getCode(account.getCodeHash(), address.addressHash()),
        forest -> Optional.<Bytes>empty());
  }
}
