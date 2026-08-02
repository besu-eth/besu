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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.manager.snap.SnapTestServing;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Full-flow tests for {@link SnapV2ReorgHealer#recoverFromReorg}. A reorg is played out on a real
 * blockchain ({@link ReorgBlockchainBuilder}) between two real Bonsai world states: the
 * <b>local</b> state stands for the partially-synced node (shared base state plus the orphaned fork
 * applied), the <b>canonical</b> state stands for the network (shared base plus the canonical fork)
 * and is served to the node through a real SnapServer. The ultimate assertion in the full-range
 * scenarios is that the local account trie root becomes identical to the canonical state root — the
 * same check the sync runs at completion.
 */
class SnapV2ReorgHealerRecoveryTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address BOB =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address CAROL =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address DAVE =
      Address.fromHexString("0x4444444444444444444444444444444444444444");
  private static final Address FRANK =
      Address.fromHexString("0x6666666666666666666666666666666666666666");
  private static final Address GRACE =
      Address.fromHexString("0x7777777777777777777777777777777777777777");
  private static final Address NEW_CONTRACT =
      Address.fromHexString("0x9999999999999999999999999999999999999999");
  private static final Address PETE =
      Address.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  private static final UInt256 S1 = UInt256.valueOf(1);
  private static final UInt256 S2 = UInt256.valueOf(2);
  private static final UInt256 S3 = UInt256.valueOf(3);
  private static final UInt256 SN = UInt256.valueOf(42);
  private static final UInt256 SP1 = UInt256.valueOf(101);
  private static final UInt256 SP2 = UInt256.valueOf(102);

  private static final Bytes CAROL_CODE_W = Bytes.fromHexString("0x6080604052348015600e");
  private static final Bytes CAROL_CODE_O = Bytes.fromHexString("0x6080604052348015600f");
  private static final Bytes NC_CODE = Bytes.fromHexString("0x60806040523480156010");

  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  private final BonsaiWorldStateKeyValueStorage localStorage = newBonsaiStorage();
  private final WorldStateStorageCoordinator localCoordinator =
      new WorldStateStorageCoordinator(localStorage);
  private final BonsaiWorldStateKeyValueStorage canonicalStorage = newBonsaiStorage();
  private final WorldStateStorageCoordinator canonicalCoordinator =
      new WorldStateStorageCoordinator(canonicalStorage);

  private final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

  /**
   * The complete reorg scenario: accounts restored from the canonical fork, an orphaned-fork-only
   * contract deleted with its storage, diverged slots restored and removed, and canonical-only
   * state created from the BALs.
   *
   * <pre>
   * gen -- 1 (A=100,B=100,D=75,F=200+s1:7,C=codeW) -- 2s (A=50, D=60, F.s1=100/F.s2=200, NC deployed, C=codeO)   orphaned
   *                                               \-- 2c (A=80, F=140+F.s3=555, G=50) -- 3c (empty, pins root)     canonical
   * </pre>
   */
  @Test
  void recoversStateAcrossReorg() {
    final DownloadedAccountRangeTracker accountTracker = fullAccountRange();
    final DownloadedStorageRangeTracker storageTracker = new DownloadedStorageRangeTracker();

    // ---- shared base (block 1) applied to both world states ----
    final BlockAccessList baseBal =
        b.merge(
            b.balWithBalances(
                Map.of(
                    ALICE, Wei.of(100),
                    BOB, Wei.of(100),
                    DAVE, Wei.of(75),
                    FRANK, Wei.of(200))),
            b.balWithStorageChanges(FRANK, Map.of(S1, UInt256.valueOf(7))),
            b.balWithCodeChange(CAROL, CAROL_CODE_W));
    final Block block1 = b.appendBlockWithBal(b.header(0), baseBal, 1L);
    applyTo(localCoordinator, 1, 1, accountTracker, storageTracker);
    applyTo(canonicalCoordinator, 1, 1, accountTracker, storageTracker);

    // ---- orphaned fork applied to the local state ----
    final BlockAccessList orphanedBal =
        b.merge(
            b.balWithBalances(Map.of(ALICE, Wei.of(50), DAVE, Wei.of(60))),
            b.balWithStorageChanges(
                FRANK, Map.of(S1, UInt256.valueOf(100), S2, UInt256.valueOf(200))),
            b.balWithBalances(Map.of(NEW_CONTRACT, Wei.ONE)),
            b.balWithCodeChange(NEW_CONTRACT, NC_CODE),
            b.balWithStorageChanges(NEW_CONTRACT, Map.of(SN, UInt256.valueOf(5))),
            b.balWithCodeChange(CAROL, CAROL_CODE_O));
    final Block block2s = b.appendStale(block1.getHeader(), orphanedBal, 2L);
    applyTo(localCoordinator, 2, 2, accountTracker, storageTracker);

    // ---- canonical fork applied to the served state ----
    final BlockAccessList canonicalBal =
        b.merge(
            b.balWithBalances(Map.of(ALICE, Wei.of(80), FRANK, Wei.of(140), GRACE, Wei.of(50))),
            b.balWithStorageChanges(FRANK, Map.of(S3, UInt256.valueOf(555))));
    final Block block2c = b.appendCanonical(block1.getHeader(), canonicalBal, 2L);
    applyTo(canonicalCoordinator, 2, 2, accountTracker, storageTracker);

    final Hash canonicalRoot = worldStateRoot(canonicalCoordinator);
    final Block newPivotBlock =
        b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final AtomicInteger codeFetches = new AtomicInteger();
    final SnapV2ReorgHealer healer =
        healerServing(canonicalRoot, new AtomicInteger(), codeFetches, new AtomicInteger());

    final ReorgRecoveryResult result =
        healer.recoverFromReorg(
            block2s.getHeader(), newPivotBlock.getHeader(), accountTracker, storageTracker);

    // Accounts the canonical fork touched come from the canonical BALs.
    assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(readAccount(FRANK).getBalance()).isEqualTo(Wei.of(140));
    assertThat(readAccount(GRACE).getBalance()).isEqualTo(Wei.of(50));
    // Untouched by either fork.
    assertThat(readAccount(BOB).getBalance()).isEqualTo(Wei.of(100));
    // Orphaned-fork-only scalar change: restored by the re-fetch.
    assertThat(readAccount(DAVE).getBalance()).isEqualTo(Wei.of(75));
    // Orphaned-fork-only code change: record restored; the code was already local.
    assertThat(readAccount(CAROL).getCodeHash()).isEqualTo(Hash.hash(CAROL_CODE_W));
    assertThat(readCode(CAROL)).hasValue(CAROL_CODE_W);
    assertThat(codeFetches).hasValue(0);

    // Frank's storage: overlapping slot s1 restored, orphaned-only slot s2 removed,
    // canonical-only slot s3 created by the BALs.
    assertThat(readStorageSlot(FRANK, S1)).hasValue(UInt256.valueOf(7));
    assertThat(readStorageSlot(FRANK, S2)).isEmpty();
    assertThat(readStorageSlot(FRANK, S3)).hasValue(UInt256.valueOf(555));

    // The contract created only on the orphaned fork is deleted with its storage.
    assertThat(accountExists(NEW_CONTRACT)).isFalse();
    assertThat(readStorageSlot(NEW_CONTRACT, SN)).isEmpty();
    assertThat(result.deletedAccounts()).containsExactly(NEW_CONTRACT.addressHash());

    // Re-fetched surviving accounts report their canonical storage roots.
    assertThat(result.correctedStorageRoots().keySet())
        .containsExactlyInAnyOrder(DAVE.addressHash(), CAROL.addressHash(), FRANK.addressHash());

    // The local world state is now identical to the canonical one.
    assertThat(worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  /**
   * When every orphaned-fork change overlaps with the canonical fork, the plan is clean and
   * recovery must not touch the network at all.
   *
   * <pre>
   * gen -- 1 (A=100) -- 2s (A=50)                     orphaned
   *                  \-- 2c (A=80) -- 3c (empty)       canonical
   * </pre>
   */
  @Test
  void cleanReorgFetchesNothing() {
    final DownloadedAccountRangeTracker accountTracker = fullAccountRange();
    final DownloadedStorageRangeTracker storageTracker = new DownloadedStorageRangeTracker();

    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    applyTo(localCoordinator, 1, 1, accountTracker, storageTracker);
    applyTo(canonicalCoordinator, 1, 1, accountTracker, storageTracker);

    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    applyTo(localCoordinator, 2, 2, accountTracker, storageTracker);

    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);
    applyTo(canonicalCoordinator, 2, 2, accountTracker, storageTracker);

    final Hash canonicalRoot = worldStateRoot(canonicalCoordinator);
    final Block newPivotBlock =
        b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final AtomicInteger accountFetches = new AtomicInteger();
    final AtomicInteger storageFetches = new AtomicInteger();
    final AtomicInteger codeFetches = new AtomicInteger();
    final SnapV2ReorgHealer healer =
        healerServing(canonicalRoot, accountFetches, codeFetches, storageFetches);

    final ReorgRecoveryResult result =
        healer.recoverFromReorg(
            block2s.getHeader(), newPivotBlock.getHeader(), accountTracker, storageTracker);

    assertThat(accountFetches).hasValue(0);
    assertThat(storageFetches).hasValue(0);
    assertThat(codeFetches).hasValue(0);
    assertThat(result.deletedAccounts()).isEmpty();
    assertThat(result.correctedStorageRoots()).isEmpty();
    assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  /**
   * A pending account (range still downloading) keeps a partial storage trie: the diverged
   * downloaded slot is fixed from the re-fetch and the storage root is taken from the fetched
   * canonical account record rather than recomputed locally.
   *
   * <pre>
   * gen -- 1 (P=100+sp1:1+sp2:2) -- 2s (P.sp1=10, P.sp2=20)      orphaned
   *                              \-- 2c (P=300) -- 3c (empty)     canonical
   * </pre>
   *
   * Locally only slot sp1 was ever downloaded, so sp2's orphaned change never reached the flat db
   * and only sp1 is re-fetched.
   */
  @Test
  void pendingAccountGetsSlotFixAndCanonicalStorageRoot() {
    final DownloadedAccountRangeTracker accountTracker = new DownloadedAccountRangeTracker();
    accountTracker.registerPending(Bytes32.ZERO, MAX_KEY, 1);
    final DownloadedStorageRangeTracker storageTracker = new DownloadedStorageRangeTracker();
    storageTracker.registerSlotRange(
        Bytes32.wrap(PETE.addressHash().getBytes()),
        Bytes32.wrap(ReorgBlockchainBuilder.slotHash(SP1).getBytes()),
        Bytes32.wrap(ReorgBlockchainBuilder.slotHash(SP1).getBytes()));

    final BlockAccessList baseBal =
        b.merge(
            b.balWithBalances(Map.of(PETE, Wei.of(100))),
            b.balWithStorageChanges(
                PETE, Map.of(SP1, UInt256.valueOf(1), SP2, UInt256.valueOf(2))));
    final Block block1 = b.appendBlockWithBal(b.header(0), baseBal, 1L);
    applyTo(localCoordinator, 1, 1, accountTracker, storageTracker);
    applyTo(canonicalCoordinator, 1, 1, fullAccountRange(), new DownloadedStorageRangeTracker());

    final Block block2s =
        b.appendStale(
            block1.getHeader(),
            b.balWithStorageChanges(
                PETE, Map.of(SP1, UInt256.valueOf(10), SP2, UInt256.valueOf(20))),
            2L);
    applyTo(localCoordinator, 2, 2, accountTracker, storageTracker);
    assertThat(readStorageSlot(PETE, SP1)).hasValue(UInt256.valueOf(10));

    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(PETE, Wei.of(300))), 2L);
    applyTo(canonicalCoordinator, 2, 2, fullAccountRange(), new DownloadedStorageRangeTracker());

    final Hash canonicalRoot = worldStateRoot(canonicalCoordinator);
    final Block newPivotBlock =
        b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2ReorgHealer healer =
        healerServing(canonicalRoot, new AtomicInteger(), new AtomicInteger(), new AtomicInteger());

    final ReorgRecoveryResult result =
        healer.recoverFromReorg(
            block2s.getHeader(), newPivotBlock.getHeader(), accountTracker, storageTracker);

    // Balance from the canonical BAL, downloaded slot restored, root taken from the fetched record.
    assertThat(readAccount(PETE).getBalance()).isEqualTo(Wei.of(300));
    assertThat(readStorageSlot(PETE, SP1)).hasValue(UInt256.valueOf(1));
    // sp2 was never downloaded locally; the orphaned change never landed and nothing was fetched.
    assertThat(readStorageSlot(PETE, SP2)).isEmpty();

    final PmtStateTrieAccountValue canonicalPete = readAccount(canonicalCoordinator, PETE);
    assertThat(readAccount(PETE).getStorageRoot()).isEqualTo(canonicalPete.getStorageRoot());
    assertThat(result.deletedAccounts()).isEmpty();
    assertThat(result.correctedStorageRoots())
        .containsEntry(PETE.addressHash(), Bytes32.wrap(canonicalPete.getStorageRoot().getBytes()));
  }

  /**
   * An orphaned-fork-only contract whose account range is still PENDING (leaves persisted, storage
   * download still in progress) is deleted atomically via flat-db prefix scan, despite its
   * incomplete storage trie. Contrast with the full-range {@link #recoversStateAcrossReorg} which
   * deletes a completed account.
   *
   * <pre>
   * gen -- 1 (A=50) -- 2s (NC deployed + code + NC.s1=5)   orphaned
   *                 \-- 2c (A=80) -- 3c (empty)            canonical
   * NC range: pending (child count=1), storage range: NC.s1 only
   * </pre>
   */
  @Test
  void deletesPendingOrphanedContractDuringRecovery() {
    final DownloadedAccountRangeTracker accountTracker = new DownloadedAccountRangeTracker();
    accountTracker.registerPending(Bytes32.ZERO, MAX_KEY, 1);
    final DownloadedStorageRangeTracker storageTracker = new DownloadedStorageRangeTracker();
    storageTracker.registerSlotRange(
        Bytes32.wrap(NEW_CONTRACT.addressHash().getBytes()),
        Bytes32.wrap(ReorgBlockchainBuilder.slotHash(SN).getBytes()),
        Bytes32.wrap(ReorgBlockchainBuilder.slotHash(SN).getBytes()));

    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 1L);
    applyTo(localCoordinator, 1, 1, accountTracker, storageTracker);
    applyTo(canonicalCoordinator, 1, 1, fullAccountRange(), new DownloadedStorageRangeTracker());

    final BlockAccessList orphanedBal =
        b.merge(
            b.balWithBalances(Map.of(NEW_CONTRACT, Wei.ONE)),
            b.balWithCodeChange(NEW_CONTRACT, NC_CODE),
            b.balWithStorageChanges(NEW_CONTRACT, Map.of(SN, UInt256.valueOf(5))));
    final Block block2s = b.appendStale(block1.getHeader(), orphanedBal, 2L);
    applyTo(localCoordinator, 2, 2, accountTracker, storageTracker);
    assertThat(readStorageSlot(NEW_CONTRACT, SN)).hasValue(UInt256.valueOf(5));

    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);
    applyTo(canonicalCoordinator, 2, 2, fullAccountRange(), new DownloadedStorageRangeTracker());

    final Hash canonicalRoot = worldStateRoot(canonicalCoordinator);
    final Block newPivotBlock =
        b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2ReorgHealer healer =
        healerServing(canonicalRoot, new AtomicInteger(), new AtomicInteger(), new AtomicInteger());

    final ReorgRecoveryResult result =
        healer.recoverFromReorg(
            block2s.getHeader(), newPivotBlock.getHeader(), accountTracker, storageTracker);

    assertThat(accountExists(NEW_CONTRACT)).isFalse();
    assertThat(readStorageSlot(NEW_CONTRACT, SN)).isEmpty();
    assertThat(result.deletedAccounts()).containsExactly(NEW_CONTRACT.addressHash());
    assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  /**
   * A reorg whose orphaned BAL was pruned locally cannot be recovered and surfaces as {@link
   * ReorgUnrecoverableException} — the caller restarts the sync, as before.
   */
  @Test
  void propagatesUnrecoverableReorg() {
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2s =
        b.appendStaleWithoutStoringBal(
            block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    final SnapV2ReorgHealer healer =
        new SnapV2ReorgHealer(
            b.blockchain(),
            localCoordinator,
            ReorgBlockchainBuilder.balEnabledSchedule(),
            ReorgBlockchainBuilder.neverCalledFetcher());

    assertThatThrownBy(
            () ->
                healer.recoverFromReorg(
                    block2s.getHeader(),
                    block2c.getHeader(),
                    fullAccountRange(),
                    new DownloadedStorageRangeTracker()))
        .isInstanceOf(ReorgUnrecoverableException.class)
        .hasMessageContaining("orphaned BAL");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** A healer whose fetcher serves from the canonical world state, counting calls per seam. */
  private SnapV2ReorgHealer healerServing(
      final Hash canonicalRoot,
      final AtomicInteger accountFetches,
      final AtomicInteger codeFetches,
      final AtomicInteger storageFetches) {
    final SnapTestServing serving = new SnapTestServing(canonicalStorage, canonicalRoot);
    final SnapV2ReorgStateFetcher fetcher =
        new SnapV2ReorgStateFetcher(
            (start, end, pivot) -> {
              accountFetches.incrementAndGet();
              return serving.accountRange(start, end, pivot);
            },
            (accounts, start, end, pivot) -> {
              storageFetches.incrementAndGet();
              return serving.storageRange(accounts, start, end, pivot);
            },
            (codeHashes, pivot) -> {
              codeFetches.incrementAndGet();
              return serving.byteCodes(codeHashes, pivot);
            },
            localCoordinator);
    return new SnapV2ReorgHealer(
        b.blockchain(), localCoordinator, ReorgBlockchainBuilder.balEnabledSchedule(), fetcher);
  }

  private void applyTo(
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

  private static BonsaiWorldStateKeyValueStorage newBonsaiStorage() {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(),
        new NoOpMetricsSystem(),
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  private static Hash worldStateRoot(final WorldStateStorageCoordinator coordinator) {
    return coordinator.getTrieNodeUnsafe(Bytes.EMPTY).map(Hash::hash).orElse(Hash.EMPTY_TRIE_HASH);
  }

  private static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }

  private PmtStateTrieAccountValue readAccount(final Address address) {
    return readAccount(localCoordinator, address);
  }

  private static PmtStateTrieAccountValue readAccount(
      final WorldStateStorageCoordinator coordinator, final Address address) {
    return PmtStateTrieAccountValue.readFrom(
        RLP.input(readAccountBytes(coordinator, address).orElseThrow()));
  }

  private boolean accountExists(final Address address) {
    return readAccountBytes(localCoordinator, address).isPresent();
  }

  private static Optional<Bytes> readAccountBytes(
      final WorldStateStorageCoordinator coordinator, final Address address) {
    return coordinator.applyForStrategy(
        bonsai -> bonsai.getAccount(address.addressHash()), forest -> Optional.<Bytes>empty());
  }

  private Optional<UInt256> readStorageSlot(final Address address, final UInt256 slotKey) {
    return localCoordinator
        .applyForStrategy(
            bonsai ->
                bonsai.getStorageValueByStorageSlotKey(
                    address.addressHash(), new StorageSlotKey(slotKey)),
            forest -> Optional.<Bytes>empty())
        .map(UInt256::fromBytes);
  }

  private Optional<Bytes> readCode(final Address address) {
    final PmtStateTrieAccountValue account = readAccount(address);
    return localCoordinator.applyForStrategy(
        bonsai -> bonsai.getCode(account.getCodeHash(), address.addressHash()),
        forest -> Optional.<Bytes>empty());
  }
}
