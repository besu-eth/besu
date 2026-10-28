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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests repair through the archive seam using the real Bonsai storage implementation. */
class BonsaiWorldStateRepairTest {
  private static final String GENESIS =
      "/org/hyperledger/besu/ethereum/mainnet/parallelization/genesis-it.json";
  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000000fffff");

  private ExecutionContextTestFixture context;
  private BonsaiWorldStateProvider archive;
  private BonsaiWorldStateKeyValueStorage storage;
  private BlockHeader head;
  private Bytes root;
  private Optional<Bytes> account;
  private Map<String, Map<Bytes, Bytes>> protectedData;

  @BeforeEach
  void setUp() {
    context =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .dataStorageConfiguration(
                ImmutableDataStorageConfiguration.builder()
                    .dataStorageFormat(DataStorageFormat.BONSAI)
                    .extraStorageConfiguration(
                        ImmutableExtraStorageConfiguration.builder()
                            .unstable(
                                ImmutableExtraStorageConfiguration.Unstable.builder()
                                    .bonsaiCrossBlockCacheEnabled(true)
                                    .build())
                            .build())
                    .build())
            .build();
    archive = (BonsaiWorldStateProvider) context.getStateArchive();
    storage = archive.getWorldStateKeyValueStorage();
    head = context.getBlockchain().getChainHeadHeader();
    final BonsaiWorldState worldState = (BonsaiWorldState) archive.getWorldState();
    worldState.disableCacheMerkleTrieLoader();
    root =
        storage
            .getTrieNode(Bytes.EMPTY, Bytes32.wrap(head.getStateRoot().getBytes()))
            .orElseThrow();
    account = worldState.createAccountStateTrie().get(CONTRACT.addressHash().getBytes());
    assertThat(account).isPresent();
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater.removeTrieNode(Bytes.EMPTY);
    updater.commitComposedOnly();
    storage.clearCrossBlockCache();
    archive.getWorldStateCacheManager().reset();
    assertThat(storage.getTrieNode(Bytes.EMPTY, Bytes32.wrap(head.getStateRoot().getBytes())))
        .isEmpty();
    protectedData = protectedData();
  }

  @Test
  void repairsMissingLiveRootWithoutMovingWorkingHead() {
    final WorldStateRepairBudget budget = WorldStateRepairBudget.defaults();
    archive.setMissingTrieNodeFetcher(
        (request, allowance) -> {
          assertThat(request.stateRoot()).isEqualTo(head.getStateRoot());
          assertThat(request.accountHash()).isEmpty();
          assertThat(request.location()).isEqualTo(Bytes.EMPTY);
          assertThat(request.expectedNodeHash()).isEqualTo(head.getStateRoot());
          assertThat(
                  storage.getTrieNode(
                      Bytes.EMPTY, Bytes32.wrap(request.expectedNodeHash().getBytes())))
              .isEmpty();
          assertThat(protectedData()).isEqualTo(protectedData);
          assertThat(allowance.consumeReceivedBytes(root.size())).isTrue();
          return Optional.of(root);
        });

    assertThat(archive.repairWorldState(head.getHash(), head.getStateRoot(), budget))
        .isEqualTo(WorldStateRepairResult.REPAIRED);

    assertWorkingHeadUnchanged();
    assertThat(protectedData()).isEqualTo(protectedData);
    assertThat(storage.getTrieNode(Bytes.EMPTY, Bytes32.wrap(head.getStateRoot().getBytes())))
        .contains(root);
    assertThat(
            ((BonsaiWorldState) archive.getWorldState())
                .createAccountStateTrie()
                .get(CONTRACT.addressHash().getBytes()))
        .isEqualTo(account);
    assertThat(
            archive.repairWorldState(
                head.getHash(), head.getStateRoot(), WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.ALREADY_AVAILABLE);
  }

  @Test
  void unsupportedFlatOrStatelessModeDoesNotFetchOrPublish() {
    archive.setMissingTrieNodeFetcher(
        (request, budget) -> {
          throw new AssertionError("Unsupported modes must decline before fetching");
        });
    archive.getWorldStateSharedSpec().setTrieDisabled(true);
    assertThat(
            archive.repairWorldState(
                head.getHash(), head.getStateRoot(), WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.UNSUPPORTED);
    archive.getWorldStateSharedSpec().setTrieDisabled(false);
    archive.getWorldStateSharedSpec().setStateful(false);
    assertThat(
            archive.repairWorldState(
                head.getHash(), head.getStateRoot(), WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.UNSUPPORTED);
    assertThat(protectedData()).isEqualTo(protectedData);
  }

  @Test
  void invalidReplyDoesNotPublishAnyNode() {
    archive.setMissingTrieNodeFetcher(
        (request, budget) -> {
          budget.consumeReceivedBytes(1);
          return Optional.of(Bytes.of(1));
        });

    assertThat(
            archive.repairWorldState(
                head.getHash(), head.getStateRoot(), WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.UNAVAILABLE);

    assertWorkingHeadUnchanged();
    assertThat(protectedData()).isEqualTo(protectedData);
    assertThat(storage.getTrieNode(Bytes.EMPTY, Bytes32.wrap(head.getStateRoot().getBytes())))
        .isEmpty();
  }

  @Test
  void cancellationDuringFetchDoesNotPublishVerifiedReply() {
    final WorldStateRepairBudget budget = WorldStateRepairBudget.defaults();
    archive.setMissingTrieNodeFetcher(
        (request, allowance) -> {
          allowance.consumeReceivedBytes(root.size());
          allowance.cancel();
          return Optional.of(root);
        });

    assertThat(archive.repairWorldState(head.getHash(), head.getStateRoot(), budget))
        .isEqualTo(WorldStateRepairResult.CANCELLED);

    assertWorkingHeadUnchanged();
    assertThat(protectedData()).isEqualTo(protectedData);
    assertThat(storage.getTrieNode(Bytes.EMPTY, Bytes32.wrap(head.getStateRoot().getBytes())))
        .isEmpty();
  }

  @Test
  void movedWorkingHeadPreventsPublicationOfStagedNode() {
    final BlockHeader replacement =
        new BlockHeaderTestFixture()
            .number(head.getNumber() + 1)
            .parentHash(head.getHash())
            .stateRoot(head.getStateRoot())
            .buildHeader();
    archive.setMissingTrieNodeFetcher(
        (request, allowance) -> {
          allowance.consumeReceivedBytes(root.size());
          // Simulate a concurrent writer changing the working state while the fetch is in flight.
          archive.withHeadWorldStateLock(
              () -> {
                ((BonsaiWorldState) archive.getWorldState()).resetWorldStateTo(replacement);
                return null;
              });
          return Optional.of(root);
        });

    assertThat(
            archive.repairWorldState(
                head.getHash(), head.getStateRoot(), WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.SUPERSEDED);

    assertThat(((BonsaiWorldState) archive.getWorldState()).blockHash())
        .isEqualTo(replacement.getHash());
    assertThat(context.getBlockchain().getChainHeadHash()).isEqualTo(head.getHash());
    assertThat(protectedData()).isEqualTo(protectedData);
    assertThat(storage.getTrieNode(Bytes.EMPTY, Bytes32.wrap(head.getStateRoot().getBytes())))
        .isEmpty();
  }

  @Test
  void repairsFromLocalHashCacheWithoutContactingPeers() {
    storage
        .getCacheManager()
        .putTrieNodes(
            List.of(
                new FlatDbCacheManager.TrieNode(
                    Bytes32.wrap(head.getStateRoot().getBytes()), root)));
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertThat(
                        storage
                            .getCacheManager()
                            .getTrieNode(Bytes32.wrap(head.getStateRoot().getBytes())))
                    .contains(root));
    archive.setMissingTrieNodeFetcher(
        (request, allowance) -> {
          throw new AssertionError("A verified local cache hit must precede the network");
        });

    assertThat(
            archive.repairWorldState(
                head.getHash(), head.getStateRoot(), WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.REPAIRED);

    assertWorkingHeadUnchanged();
    assertThat(protectedData()).isEqualTo(protectedData);
    assertThat(storage.getTrieNode(Bytes.EMPTY, Bytes32.wrap(head.getStateRoot().getBytes())))
        .contains(root);
  }

  @Test
  void oversizedLocalLogIsDeclinedBeforeDecodingOrFetching() {
    final BlockHeader target =
        new BlockHeaderTestFixture()
            .number(head.getNumber() + 1)
            .parentHash(head.getHash())
            .stateRoot(head.getStateRoot())
            .buildHeader();
    final var headers = spy(context.getBlockchain());
    doReturn(Optional.of(target)).when(headers).getBlockHeader(target.getHash());
    final var transaction = storage.getTrieLogStorage().startTransaction();
    // Deliberately invalid encoded data: budget refusal must happen before any RLP decoding.
    transaction.put(target.getHash().getBytes().toArrayUnsafe(), new byte[2 * 1024 * 1024]);
    transaction.commit();
    protectedData = protectedData();
    final BonsaiWorldStateProvider repairArchive =
        new BonsaiWorldStateProvider(
            storage,
            headers,
            ImmutableExtraStorageConfiguration.builder().maxLayersToLoad(64L).build(),
            NoOpBonsaiCachedMerkleTrieLoader.INSTANCE,
            null,
            EvmConfiguration.DEFAULT,
            new BonsaiCodeCache());
    repairArchive.setMissingTrieNodeFetcher(
        (request, allowance) -> {
          throw new AssertionError("Oversized local history must be refused before fetching");
        });

    assertThat(
            repairArchive.repairWorldState(
                target.getHash(), target.getStateRoot(), WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.BUDGET_EXHAUSTED);

    assertWorkingHeadUnchanged();
    assertThat(protectedData()).isEqualTo(protectedData);
    assertThat(storage.getTrieNode(Bytes.EMPTY, Bytes32.wrap(head.getStateRoot().getBytes())))
        .isEmpty();
  }

  @Test
  void elapsedBudgetPreventsPublicationOfVerifiedReply() {
    final AtomicLong now = new AtomicLong();
    final WorldStateRepairBudget budget = new WorldStateRepairBudget(now::get);
    archive.setMissingTrieNodeFetcher(
        (request, allowance) -> {
          allowance.consumeReceivedBytes(root.size());
          now.set(java.time.Duration.ofSeconds(11).toNanos());
          return Optional.of(root);
        });

    assertThat(archive.repairWorldState(head.getHash(), head.getStateRoot(), budget))
        .isEqualTo(WorldStateRepairResult.BUDGET_EXHAUSTED);

    assertWorkingHeadUnchanged();
    assertThat(protectedData()).isEqualTo(protectedData);
    assertThat(storage.getTrieNode(Bytes.EMPTY, Bytes32.wrap(head.getStateRoot().getBytes())))
        .isEmpty();
  }

  private Map<String, Map<Bytes, Bytes>> protectedData() {
    final Map<String, Map<Bytes, Bytes>> data = new HashMap<>();
    for (final SegmentIdentifier segment :
        List.of(ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE, CODE_STORAGE, TRIE_BRANCH_STORAGE)) {
      try (Stream<Pair<byte[], byte[]>> entries =
          storage.getComposedWorldStateStorage().stream(segment)) {
        data.put(
            segment.toString(),
            entries
                .filter(entry -> segment != TRIE_BRANCH_STORAGE || entry.getKey().length != 0)
                .collect(
                    Collectors.toMap(
                        entry -> Bytes.wrap(entry.getKey()).copy(),
                        entry -> Bytes.wrap(entry.getValue()).copy())));
      }
    }
    try (Stream<Pair<byte[], byte[]>> entries = storage.getTrieLogStorage().stream()) {
      data.put(
          "trie-logs",
          entries.collect(
              Collectors.toMap(
                  entry -> Bytes.wrap(entry.getKey()).copy(),
                  entry -> Bytes.wrap(entry.getValue()).copy())));
    }
    return data;
  }

  private void assertWorkingHeadUnchanged() {
    assertThat(context.getBlockchain().getChainHeadHash()).isEqualTo(head.getHash());
    assertThat(((BonsaiWorldState) archive.getWorldState()).blockHash()).isEqualTo(head.getHash());
    assertThat(archive.getWorldState().rootHash()).isEqualTo(head.getStateRoot());
  }
}
