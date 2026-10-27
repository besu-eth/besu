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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeCoordinator;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ForkchoiceUpdatedResultV1;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.WorldStateRecoveryCoordinator;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContext;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Live FCU rolling through the real asynchronous Bonsai storage-root committer. */
class EngineForkchoiceUpdatedStateRepairIntegrationTest {
  @Test
  void wrappedStorageTrieFailureStartsRepairAndLaterForkchoiceSucceeds() throws Exception {
    final Address contract = Address.fromHexString("0x1234");
    final var fixture =
        ExecutionContextTestFixture.builder(
                GenesisConfig.fromConfig(
                    """
        {"config":{"chainId":42,"londonBlock":0},"gasLimit":"0x1c9c380",
         "difficulty":"0x1","alloc":{"0000000000000000000000000000000000001234":
         {"balance":"0x1","storage":{"0x0":"0x1"}}}}
        """))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    final var archive = (BonsaiWorldStateProvider) fixture.getStateArchive();
    final var state = (BonsaiWorldState) archive.getWorldState();
    state.disableCacheMerkleTrieLoader();
    final var genesis = fixture.getGenesis().getHeader();
    final var updater = state.updater();
    updater.getAccount(contract).setStorageValue(UInt256.ZERO, UInt256.valueOf(2));
    updater.commit();
    final var child =
        new BlockHeaderTestFixture()
            .number(1)
            .timestamp(1)
            .parentHash(genesis.getHash())
            .stateRoot(state.frontierRootHash())
            .buildHeader();
    state.persist(child);
    fixture.getBlockchain().appendBlock(new Block(child, BlockBody.empty()), List.of());

    final var storage = archive.getWorldStateKeyValueStorage();
    final var storageRoot = ((BonsaiAccount) state.get(contract)).getStorageRoot();
    final Bytes key = TrieNodeKey.of(contract.addressHash(), Bytes.EMPTY);
    final Bytes node = storage.getTrieNode(key, Bytes32.wrap(storageRoot.getBytes())).orElseThrow();
    final var deletion = storage.updater();
    deletion.removeTrieNode(key);
    deletion.commitComposedOnly();
    storage.clearCrossBlockCache();
    archive.getWorldStateCacheManager().reset();
    assertThat(storage.getTrieNode(key, Bytes32.wrap(storageRoot.getBytes()))).isEmpty();
    archive.setMissingTrieNodeFetcher(
        (request, budget) -> {
          assertThat(request.accountHash()).contains(contract.addressHash());
          assertThat(request.expectedNodeHash()).isEqualTo(storageRoot);
          budget.consumeReceivedBytes(node.size());
          return Optional.of(node);
        });

    final MergeContext mergeContext = mock(MergeContext.class);
    when(mergeContext.as(MergeContext.class)).thenReturn(mergeContext);
    when(mergeContext.isInitialSyncDone()).thenReturn(true);
    final var context =
        new ProtocolContext.Builder()
            .withBlockchain(fixture.getBlockchain())
            .withWorldStateArchive(archive)
            .withConsensusContext(mergeContext)
            .build();
    final var pool = mock(TransactionPool.class);
    final var coordinator =
        new MergeCoordinator(
            context,
            fixture.getProtocolSchedule(),
            mock(EthScheduler.class),
            pool,
            MiningConfiguration.newDefault(),
            mock(BackwardSyncContext.class));
    final Synchronizer synchronizer = mock(Synchronizer.class);
    final CompletableFuture<Void> allowRepair = new CompletableFuture<>();
    final AtomicReference<CompletableFuture<WorldStateRepairResult>> worker =
        new AtomicReference<>();
    when(synchronizer.repairWorldState(any(), any()))
        .thenAnswer(
            invocation -> {
              final CompletableFuture<WorldStateRepairResult> repair =
                  allowRepair.thenApplyAsync(
                      ignored ->
                          archive.repairWorldState(
                              invocation.getArgument(0), invocation.getArgument(1)));
              worker.set(repair);
              return repair;
            });
    final var recovery = new WorldStateRecoveryCoordinator(synchronizer);
    final var listener = mock(EngineCallListener.class);
    final Vertx vertx = Vertx.vertx();
    try {
      final var method =
          new EngineForkchoiceUpdatedV1<>(
              new ConstructorArgumentsBuilder()
                  .protocolSchedule(fixture.getProtocolSchedule())
                  .protocolContext(context)
                  .vertx(vertx)
                  .engineCallListener(listener)
                  .mergeCoordinator(coordinator)
                  .ethPeers(mock(EthPeers.class))
                  .metricsSystem(new NoOpMetricsSystem())
                  .transactionPool(pool)
                  .maxRequestBlocks(0)
                  .worldStateRecoveryCoordinator(recovery)
                  .build(),
              null,
              null);
      final var request =
          new JsonRpcRequestContext(
              new JsonRpcRequest(
                  "2.0",
                  method.getName(),
                  new Object[] {new ForkchoiceStateV1(genesis.getHash(), Hash.ZERO, Hash.ZERO)}));

      final var syncingResponse = (JsonRpcSuccessResponse) method.syncResponse(request);
      final var syncingResult = (ForkchoiceUpdatedResultV1) syncingResponse.getResult();
      assertThat(syncingResult.getPayloadStatus().getStatus()).isEqualTo(SYNCING);
      assertThat(syncingResult.getPayloadStatus().getLatestValidHash()).isNull();
      assertThat(syncingResult.getPayloadId()).isNull();
      final var requestCaptor = org.mockito.ArgumentCaptor.forClass(WorldStateRepairRequest.class);
      verify(synchronizer).repairWorldState(requestCaptor.capture(), any());
      assertThat(requestCaptor.getValue().missingNode()).isPresent();
      assertThat(requestCaptor.getValue().missingNode().orElseThrow().expectedHash())
          .isEqualTo(storageRoot);
      assertThat(fixture.getBlockchain().getChainHeadHash()).isEqualTo(child.getHash());
      allowRepair.complete(null);
      final WorldStateRepairResult outcome = worker.get().get(10, TimeUnit.SECONDS);
      assertThat(outcome)
          .withFailMessage("repair request was %s", requestCaptor.getValue())
          .isEqualTo(WorldStateRepairResult.REPAIRED);
      final var response = (JsonRpcSuccessResponse) method.syncResponse(request);
      assertThat(((ForkchoiceUpdatedResultV1) response.getResult()).getPayloadStatus().getStatus())
          .isEqualTo(VALID);
      assertThat(fixture.getBlockchain().getChainHeadHash()).isEqualTo(genesis.getHash());
      verify(listener, times(2)).executionEngineCalled();
      verify(synchronizer).repairWorldState(any(WorldStateRepairRequest.class), any());
      verify(synchronizer, never()).resyncWorldState();
    } finally {
      allowRepair.complete(null);
      vertx.close();
    }
  }
}
