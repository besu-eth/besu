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
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.manager.snap.SnapTestServing;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SnapV2ReorgStateFetcher} against a real {@link
 * org.hyperledger.besu.ethereum.eth.manager.snap.SnapServer} backed by a real in-memory world
 * state: present and absent accounts, present and absent slots, code retrieval, and rejection of
 * responses that do not prove against the pivot state root.
 */
class SnapV2ReorgStateFetcherTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address FRANK =
      Address.fromHexString("0x6666666666666666666666666666666666666666");
  private static final Address CAROL =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address UNKNOWN =
      Address.fromHexString("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

  private static final UInt256 S1 = UInt256.valueOf(1);
  private static final UInt256 S2 = UInt256.valueOf(2);
  private static final UInt256 UNKNOWN_SLOT = UInt256.valueOf(9999);
  private static final Bytes CAROL_CODE = Bytes.fromHexString("0x6080604052348015600e575f5ffd5b50");

  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  private final BonsaiWorldStateKeyValueStorage canonicalStorage = newBonsaiStorage();
  private final WorldStateStorageCoordinator canonicalCoordinator =
      new WorldStateStorageCoordinator(canonicalStorage);

  private ReorgBlockchainBuilder b;
  private SnapTestServing serving;
  private SnapV2ReorgStateFetcher fetcher;
  private BlockHeader pivotHeader;
  private Hash canonicalRoot;

  private static BonsaiWorldStateKeyValueStorage newBonsaiStorage() {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(),
        new NoOpMetricsSystem(),
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  @BeforeEach
  void setup() {
    b = new ReorgBlockchainBuilder();

    // Single canonical block creating the served state: Alice (balance), Frank (balance and two
    // storage slots), Carol (code).
    final BlockAccessList bal =
        b.merge(
            b.balWithBalances(Map.of(ALICE, Wei.of(100), FRANK, Wei.of(200))),
            b.balWithStorageChanges(FRANK, Map.of(S1, UInt256.valueOf(7), S2, UInt256.valueOf(8))),
            b.balWithCodeChange(CAROL, CAROL_CODE));
    b.appendBlockWithBal(b.header(0), bal, 1L);

    new SnapV2BlockAccessListApplier(
            canonicalCoordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule())
        .applyBlockAccessLists(1L, 1L, fullAccountRange(), new DownloadedStorageRangeTracker())
        .commit();

    canonicalRoot = worldStateRoot(canonicalCoordinator);
    serving = new SnapTestServing(canonicalStorage, canonicalRoot);
    fetcher =
        new SnapV2ReorgStateFetcher(
            serving::accountRange,
            serving::storageRange,
            serving::byteCodes,
            new WorldStateStorageCoordinator(newBonsaiStorage()));
    pivotHeader = new BlockHeaderTestFixture().stateRoot(canonicalRoot).buildHeader();
  }

  @Test
  void fetchesPresentAccounts() {
    final Map<Hash, Optional<PmtStateTrieAccountValue>> fetched =
        fetcher.fetchAccounts(Set.of(ALICE.addressHash(), FRANK.addressHash()), pivotHeader).join();

    assertThat(fetched.get(ALICE.addressHash())).isPresent();
    assertThat(fetched.get(ALICE.addressHash()).get().getBalance()).isEqualTo(Wei.of(100));

    assertThat(fetched.get(FRANK.addressHash())).isPresent();
    final PmtStateTrieAccountValue frank = fetched.get(FRANK.addressHash()).get();
    assertThat(frank.getBalance()).isEqualTo(Wei.of(200));
    assertThat(frank.getStorageRoot()).isNotEqualTo(Hash.EMPTY_TRIE_HASH);
  }

  @Test
  void fetchesAbsentAccountAsEmpty() {
    final Map<Hash, Optional<PmtStateTrieAccountValue>> fetched =
        fetcher.fetchAccounts(Set.of(UNKNOWN.addressHash()), pivotHeader).join();

    assertThat(fetched.get(UNKNOWN.addressHash())).isEmpty();
  }

  @Test
  void fetchesAbsentAccountAlongsidePresentOnes() {
    final Map<Hash, Optional<PmtStateTrieAccountValue>> fetched =
        fetcher
            .fetchAccounts(Set.of(ALICE.addressHash(), UNKNOWN.addressHash()), pivotHeader)
            .join();

    assertThat(fetched.get(ALICE.addressHash())).isPresent();
    assertThat(fetched.get(UNKNOWN.addressHash())).isEmpty();
  }

  @Test
  void fetchesSlots() {
    final Hash frankStorageRoot = frankStorageRoot();
    final Map<Hash, Optional<UInt256>> fetched =
        fetcher
            .fetchSlots(
                FRANK.addressHash(),
                frankStorageRoot,
                Set.of(ReorgBlockchainBuilder.slotHash(S1), ReorgBlockchainBuilder.slotHash(S2)),
                pivotHeader)
            .join();

    assertThat(fetched.get(ReorgBlockchainBuilder.slotHash(S1))).hasValue(UInt256.valueOf(7));
    assertThat(fetched.get(ReorgBlockchainBuilder.slotHash(S2))).hasValue(UInt256.valueOf(8));
  }

  @Test
  void fetchesAbsentSlotAsEmpty() {
    final Map<Hash, Optional<UInt256>> fetched =
        fetcher
            .fetchSlots(
                FRANK.addressHash(),
                frankStorageRoot(),
                Set.of(ReorgBlockchainBuilder.slotHash(UNKNOWN_SLOT)),
                pivotHeader)
            .join();

    assertThat(fetched.get(ReorgBlockchainBuilder.slotHash(UNKNOWN_SLOT))).isEmpty();
  }

  @Test
  void fetchesCode() {
    final Map<Hash, Bytes> fetched =
        fetcher.fetchCodes(Set.of(Hash.hash(CAROL_CODE)), pivotHeader).join();

    assertThat(fetched).containsEntry(Hash.hash(CAROL_CODE), CAROL_CODE);
  }

  @Test
  void failsWhenCodeIsNotServed() {
    assertThatThrownBy(
            () ->
                fetcher
                    .fetchCodes(
                        Set.of(
                            Hash.fromHexString(
                                "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")),
                        pivotHeader)
                    .join())
        .hasCauseInstanceOf(WorldStateDownloaderException.class)
        .hasMessageContaining("missing or hash-mismatched");
  }

  @Test
  void failsWhenResponseDoesNotProveAgainstPivotStateRoot() {
    // Serve from the same state but answer for a root the server does not know: the response
    // carries no proofs and must be rejected.
    final SnapTestServing unserving =
        new SnapTestServing(
            canonicalStorage,
            Hash.fromHexString(
                "0x1234000000000000000000000000000000000000000000000000000000000000"));
    final SnapV2ReorgStateFetcher unservedFetcher =
        new SnapV2ReorgStateFetcher(
            unserving::accountRange,
            unserving::storageRange,
            unserving::byteCodes,
            new WorldStateStorageCoordinator(newBonsaiStorage()));

    assertThatThrownBy(
            () -> unservedFetcher.fetchAccounts(Set.of(ALICE.addressHash()), pivotHeader).join())
        .hasCauseInstanceOf(WorldStateDownloaderException.class)
        .hasMessageContaining("Invalid account range proof");
  }

  private Hash frankStorageRoot() {
    final PmtStateTrieAccountValue frank =
        fetcher
            .fetchAccounts(Set.of(FRANK.addressHash()), pivotHeader)
            .join()
            .get(FRANK.addressHash())
            .orElseThrow();
    return frank.getStorageRoot();
  }

  private static Hash worldStateRoot(final WorldStateStorageCoordinator coordinator) {
    return coordinator.getTrieNodeUnsafe(Bytes.EMPTY).map(Hash::hash).orElse(Hash.EMPTY_TRIE_HASH);
  }

  private static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }
}
