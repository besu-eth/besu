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
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.StoredNode;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Focused tests for {@link SnapV2BlockAccessListApplier#applyReorgCorrections} with hand-built
 * plans and fetched state: canonical code storage for restored accounts, fetch-coverage validation,
 * and the storage-root consistency check on completed accounts.
 */
class SnapV2BlockAccessListApplierReorgCorrectionTest {

  private static final Address CAROL =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address FRANK =
      Address.fromHexString("0x6666666666666666666666666666666666666666");

  private static final Bytes CAROL_CODE_W = Bytes.fromHexString("0x6080604052348015600e");
  private static final Bytes CAROL_CODE_O = Bytes.fromHexString("0x6080604052348015600f");
  private static final UInt256 S1 = UInt256.valueOf(1);

  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  static Stream<WorldStateStorageHarness> storage() {
    return Stream.of(new BonsaiWorldStateStorageHarness(), new ForestWorldStateStorageHarness());
  }

  /**
   * A restored account whose canonical code was never downloaded locally (its code changed only on
   * the orphaned fork) gets the fetched canonical code stored.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("storage")
  void storesFetchedCanonicalCodeForRestoredAccount(final WorldStateStorageHarness storage) {
    final ReorgBlockchainBuilder reorgBuilder = new ReorgBlockchainBuilder();

    // Local state: Carol with the orphaned code only.
    storage.seedAccount(CAROL, 0L, Wei.of(50), Hash.EMPTY_TRIE_HASH, Hash.hash(CAROL_CODE_O));
    storage.seedCode(CAROL, CAROL_CODE_O);

    final ReorgPlan plan = planWithDivergedAccounts(Set.of(CAROL.addressHash()), Map.of());
    final FetchedReorgState fetched =
        new FetchedReorgState(
            Map.of(
                CAROL.addressHash(),
                Optional.of(accountValue(Wei.of(50), Hash.hash(CAROL_CODE_W)))),
            Map.of(),
            Map.of(Hash.hash(CAROL_CODE_W), CAROL_CODE_W));

    applier(storage, reorgBuilder)
        .applyReorgCorrections(
            plan, fetched, fullAccountRange(), new DownloadedStorageRangeTracker());

    final PmtStateTrieAccountValue carol = storage.readAccount(CAROL).orElseThrow();
    assertThat(carol.getCodeHash()).isEqualTo(Hash.hash(CAROL_CODE_W));
    assertThat(storage.readCode(CAROL)).hasValue(CAROL_CODE_W);
  }

  /** A restored account whose canonical code is neither local nor fetched aborts the recovery. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("storage")
  void failsWhenCanonicalCodeWasNotFetched(final WorldStateStorageHarness storage) {
    final ReorgBlockchainBuilder reorgBuilder = new ReorgBlockchainBuilder();

    storage.seedAccount(CAROL, 0L, Wei.of(50), Hash.EMPTY_TRIE_HASH, Hash.hash(CAROL_CODE_O));
    storage.seedCode(CAROL, CAROL_CODE_O);

    final ReorgPlan plan = planWithDivergedAccounts(Set.of(CAROL.addressHash()), Map.of());
    final FetchedReorgState fetched =
        new FetchedReorgState(
            Map.of(
                CAROL.addressHash(),
                Optional.of(accountValue(Wei.of(50), Hash.hash(CAROL_CODE_W)))),
            Map.of(),
            Map.of());

    assertThatThrownBy(
            () ->
                applier(storage, reorgBuilder)
                    .applyReorgCorrections(
                        plan, fetched, fullAccountRange(), new DownloadedStorageRangeTracker()))
        .isInstanceOf(WorldStateDownloaderException.class)
        .hasMessageContaining("canonical code");
  }

  /** Every account the plan asks to re-fetch must be covered by the fetched state. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("storage")
  void failsWhenFetchDidNotCoverDivergedAccount(final WorldStateStorageHarness storage) {
    final ReorgBlockchainBuilder reorgBuilder = new ReorgBlockchainBuilder();

    storage.seedAccount(CAROL, 0L, Wei.of(50), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    final ReorgPlan plan = planWithDivergedAccounts(Set.of(CAROL.addressHash()), Map.of());
    final FetchedReorgState fetched = FetchedReorgState.empty();

    assertThatThrownBy(
            () ->
                applier(storage, reorgBuilder)
                    .applyReorgCorrections(
                        plan, fetched, fullAccountRange(), new DownloadedStorageRangeTracker()))
        .isInstanceOf(WorldStateDownloaderException.class)
        .hasMessageContaining("did not cover account");
  }

  /**
   * For accounts in completed ranges the locally recomputed storage root after slot fixes must
   * equal the fetched canonical root — here the fetched record claims an empty storage root while
   * the account clearly has storage, so recovery must abort.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("storage")
  void failsOnStorageRootMismatchForCompletedAccount(final WorldStateStorageHarness storage) {
    final ReorgBlockchainBuilder reorgBuilder = new ReorgBlockchainBuilder();

    // Local state built through the applier so Frank has a real storage trie with s1=100.
    final BlockAccessList baseBal =
        reorgBuilder.merge(
            reorgBuilder.balWithBalances(Map.of(FRANK, Wei.of(200))),
            reorgBuilder.balWithStorageChanges(FRANK, Map.of(S1, UInt256.valueOf(100))));
    reorgBuilder.appendBlockWithBal(reorgBuilder.header(0), baseBal, 1L);
    applier(storage, reorgBuilder)
        .applyBlockAccessLists(1L, 1L, fullAccountRange(), new DownloadedStorageRangeTracker())
        .commit();

    final ReorgPlan plan =
        planWithDivergedAccounts(
            Set.of(), Map.of(FRANK.addressHash(), Set.of(ReorgBlockchainBuilder.slotHash(S1))));
    final PmtStateTrieAccountValue canonicalFrank =
        new PmtStateTrieAccountValue(0L, Wei.of(200), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final FetchedReorgState fetched =
        new FetchedReorgState(
            Map.of(FRANK.addressHash(), Optional.of(canonicalFrank)),
            Map.of(
                FRANK.addressHash(),
                Map.of(ReorgBlockchainBuilder.slotHash(S1), Optional.of(UInt256.valueOf(7)))),
            Map.of());

    assertThatThrownBy(
            () ->
                applier(storage, reorgBuilder)
                    .applyReorgCorrections(
                        plan, fetched, fullAccountRange(), new DownloadedStorageRangeTracker()))
        .isInstanceOf(WorldStateDownloaderException.class)
        .hasMessageContaining("storage root mismatch");
  }

  /**
   * A pending account whose storage trie is partial (root branch with one present leaf child at
   * nibble 0 and one child at nibble 1 referencing a never-persisted node — exactly what a
   * partially range-downloaded snap/2 local state looks like) is still deleted correctly: every
   * flat storage slot is removed via a flat-db prefix scan, bypassing the incomplete trie entirely.
   */
  @Test
  void bonsaiDeletesPendingAccountWithPartialStorageTrie() {
    final BonsaiWorldStateKeyValueStorage bonsaiStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    final WorldStateStorageCoordinator coordinator =
        new WorldStateStorageCoordinator(bonsaiStorage);
    final ReorgBlockchainBuilder reorgBuilder = new ReorgBlockchainBuilder();

    final StoredNodeFactory<Bytes> factory =
        new StoredNodeFactory<>(
            (location, hash) -> Optional.empty(), Function.identity(), Function.identity());

    // Leaf at branch child 0: slot hash 0x00…00 (63 zero nibbles + leaf terminator).
    final MutableBytes leafPath = MutableBytes.create(64);
    leafPath.set(63, CompactEncoding.LEAF_TERMINATOR);
    final Node<Bytes> leaf = factory.createLeaf(leafPath, UInt256.ONE.toMinimalBytes());
    final Bytes32 leafHash = leaf.getHash();
    final Bytes leafRlp = leaf.getEncodedBytes();

    // Missing child at nibble 1: references an undownloaded subtree.
    final Bytes32 missingHash =
        Bytes32.fromHexString("0x0101010101010101010101010101010101010101010101010101010101010101");
    final Node<Bytes> missingChild = new StoredNode<>(factory, Bytes.of((byte) 1), missingHash);

    // Root branch: child 0 = leaf, child 1 = missing, rest null.
    final List<Node<Bytes>> branchChildren =
        new ArrayList<>(Collections.nCopies(16, NullNode.<Bytes>instance()));
    branchChildren.set(0, leaf);
    branchChildren.set(1, missingChild);
    final Node<Bytes> root = factory.createBranch(branchChildren, Optional.empty());
    final Bytes32 rootHash = root.getHash();
    final Bytes rootRlp = root.getEncodedBytes();

    // Persist the partial trie + flat account + flat slot into Bonsai.
    final Hash accountHash = CAROL.addressHash();
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    applyForStrategy(
        updater,
        bonsai -> {
          bonsai.putAccountStorageTrieNode(accountHash, Bytes.EMPTY, rootHash, rootRlp);
          bonsai.putAccountStorageTrieNode(accountHash, Bytes.of((byte) 0), leafHash, leafRlp);
          // child at nibble 1 deliberately NOT persisted
        },
        forest -> {});

    final PmtStateTrieAccountValue carolAccount =
        new PmtStateTrieAccountValue(0L, Wei.of(50), Hash.wrap(rootHash), Hash.EMPTY);
    final Bytes encodedAccount = RLP.encode(carolAccount::writeTo);
    applyForStrategy(
        updater, bonsai -> bonsai.putAccountInfoState(accountHash, encodedAccount), forest -> {});
    // Flat slot matching the leaf keyHash (Bytes32.ZERO).
    applyForStrategy(
        updater,
        bonsai ->
            bonsai.putStorageValueBySlotHash(
                accountHash, Hash.wrap(Bytes32.ZERO), UInt256.ONE.toBytes()),
        forest -> {});
    updater.commit();

    final ReorgPlan plan = planWithDivergedAccounts(Set.of(accountHash), Map.of());
    final FetchedReorgState fetched =
        new FetchedReorgState(Map.of(accountHash, Optional.empty()), Map.of(), Map.of());

    final DownloadedAccountRangeTracker pendingRange = new DownloadedAccountRangeTracker();
    pendingRange.registerPending(Bytes32.ZERO, MAX_KEY, 1);

    final ReorgRecoveryResult result =
        new SnapV2BlockAccessListApplier(
                coordinator, reorgBuilder.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule())
            .applyReorgCorrections(
                plan, fetched, pendingRange, new DownloadedStorageRangeTracker());

    // Account is deleted.
    assertThat(result.deletedAccounts()).containsExactly(accountHash);
    final Optional<Bytes> accountAfter =
        coordinator.applyForStrategy(
            bonsai -> bonsai.getAccount(accountHash), forest -> Optional.<Bytes>empty());
    assertThat(accountAfter).isEmpty();

    // Every flat storage slot is removed despite the partial trie.
    final NavigableMap<Bytes32, Bytes> remainingSlots =
        coordinator.applyForStrategy(
            bonsai -> bonsai.streamFlatStorages(accountHash, Bytes32.ZERO, slotEntry -> true),
            forest -> new TreeMap<>());
    assertThat(remainingSlots).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static SnapV2BlockAccessListApplier applier(
      final WorldStateStorageHarness storage, final ReorgBlockchainBuilder reorgBuilder) {
    return new SnapV2BlockAccessListApplier(
        storage.coordinator(),
        reorgBuilder.blockchain(),
        ReorgBlockchainBuilder.balEnabledSchedule());
  }

  private static ReorgPlan planWithDivergedAccounts(
      final Set<Hash> divergedAccounts, final Map<Hash, Set<Hash>> divergedSlotsByAccount) {
    final BlockHeader ancestor = new BlockHeaderTestFixture().number(1).buildHeader();
    final BlockHeader oldPivot = new BlockHeaderTestFixture().number(2).buildHeader();
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(3).buildHeader();
    return new ReorgPlan(ancestor, oldPivot, newPivot, divergedAccounts, divergedSlotsByAccount);
  }

  private static PmtStateTrieAccountValue accountValue(final Wei balance, final Hash codeHash) {
    return new PmtStateTrieAccountValue(0L, balance, Hash.EMPTY_TRIE_HASH, codeHash);
  }

  private static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }
}
