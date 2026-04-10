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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeCoordinator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineUpdateForkchoiceResult;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContext;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for the engine_forkchoiceUpdated "invalid block as ancestor" flow.
 *
 * <ol>
 *   <li>A bad block B is registered in {@link BadBlockManager} (simulating what {@link
 *       org.hyperledger.besu.ethereum.MainnetBlockValidator} does on a failed import).
 *   <li>{@link MergeCoordinator#onBadChain(Block, List, List)} is called with B and a descendant D
 *       simulating the backward-sync code path that fires on {@code
 *       BackwardSyncContext.emitBadChainEvent}. This should propagate B's "bad" marking to every
 *       descendant, and record each descendant's {@code latestValidHash} as B's (valid) parent
 *       hash.
 *   <li>A JSON-RPC {@code engine_forkchoiceUpdated} call is invoked with {@code headBlockHash = D}.
 *       The that should return {@code INVALID} with {@code latestValidHash} equal to B's valid
 *       parent hash.
 * </ol>
 */
public class EngineForkchoiceUpdatedBadAncestorIntegrationTest {

  private static final Vertx vertx = Vertx.vertx();

  private final BlockHeaderTestFixture headerBuilder = new BlockHeaderTestFixture();

  private BadBlockManager badBlockManager;
  private MutableBlockchain blockchain;
  private MergeCoordinator mergeCoordinator;
  private EngineForkchoiceUpdatedV3 method;

  @BeforeEach
  public void setUp() {
    badBlockManager = new BadBlockManager();

    // Blockchain only needs to serve the bad block's parent header lookup for
    // MergeCoordinator.onBadChain; everything else on the fcU short-circuit path avoids it.
    blockchain = mock(MutableBlockchain.class);

    final MergeContext mergeContext = mock(MergeContext.class);
    when(mergeContext.as(MergeContext.class)).thenReturn(mergeContext);

    final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);

    final ProtocolContext protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(worldStateArchive)
            .withConsensusContext(mergeContext)
            .withBadBlockManager(badBlockManager)
            .build();

    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);

    when(protocolSchedule.getChainId()).thenReturn(Optional.empty());
    when(protocolSchedule.milestoneFor(CANCUN)).thenReturn(Optional.empty());
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.empty());

    final EthScheduler ethScheduler = mock(EthScheduler.class);
    final TransactionPool transactionPool = mock(TransactionPool.class);
    final BackwardSyncContext backwardSyncContext = mock(BackwardSyncContext.class);
    final MiningConfiguration miningConfiguration = MiningConfiguration.newDefault();

    mergeCoordinator =
        new MergeCoordinator(
            protocolContext,
            protocolSchedule,
            ethScheduler,
            transactionPool,
            miningConfiguration,
            backwardSyncContext);

    method =
        new EngineForkchoiceUpdatedV3(
            vertx,
            protocolSchedule,
            protocolContext,
            mergeCoordinator,
            mock(EngineCallListener.class));
  }

  @Test
  public void shouldReturnInvalidForForkchoiceUpdateWhenHeadHasBadAncestor() {
    // Chain shape:
    //   validParent (PoS, difficulty=0, known to the blockchain)
    //     └── badBlock B (rejected by validator, in BadBlockManager)
    //           └── descendant D (CL's head — only in BadBlockManager via onBadChain)
    final BlockHeader validParent = headerBuilder.number(100L).buildHeader();
    final BlockHeader badHeader =
        headerBuilder.number(101L).parentHash(validParent.getHash()).buildHeader();
    final Block badBlock = new Block(badHeader, BlockBody.empty());
    final BlockHeader descendantHeader =
        headerBuilder.number(102L).parentHash(badHeader.getHash()).buildHeader();

    // onBadChain looks up the bad block's parent in the blockchain to compute latestValidHash.
    when(blockchain.getBlockHeader(validParent.getHash())).thenReturn(Optional.of(validParent));

    // Simulate MainnetBlockValidator.handleFailedBlockProcessing — add the bad block
    // to the manager directly. (The direct add path intentionally does NOT record a
    // latestValidHash for B itself; only the descendants in stage 2 get one.)
    badBlockManager.addBadBlock(badBlock, BadBlockCause.fromValidationFailure("BAL mismatch"));

    // Simulate BackwardSyncContext.emitBadChainEvent firing — MergeCoordinator is
    // registered as a BadChainListener in its constructor and receives the descendant list.
    mergeCoordinator.onBadChain(badBlock, emptyList(), List.of(descendantHeader));

    // onBadChain propagated "bad" status and latestValidHash to the descendant.
    assertThat(mergeCoordinator.isBadBlock(descendantHeader.getHash())).isTrue();
    assertThat(mergeCoordinator.getLatestValidHashOfBadBlock(descendantHeader.getHash()))
        .contains(validParent.getHash());

    // CL sends engine_forkchoiceUpdated with the descendant as the head, simulating the
    // case where the CL reuses a head whose ancestry was poisoned while we were backward-syncing.
    final JsonRpcResponse response =
        invokeForkchoiceUpdated(
            new EngineForkchoiceUpdatedParameter(
                descendantHeader.getHash(), validParent.getHash(), validParent.getHash()));

    // Must return INVALID with
    // latestValidHash pointing at the last valid ancestor (B's parent), not SYNCING and not
    // VALID, without ever touching the blockchain state.
    final EngineUpdateForkchoiceResult forkchoiceResult =
        (EngineUpdateForkchoiceResult) ((JsonRpcSuccessResponse) response).getResult();
    assertThat(forkchoiceResult.getPayloadStatus().getStatus()).isEqualTo(INVALID);
    assertThat(forkchoiceResult.getPayloadStatus().getLatestValidHashAsString())
        .isEqualTo(validParent.getHash().toHexString());
    assertThat(forkchoiceResult.getPayloadStatus().getError())
        .contains(descendantHeader.getHash() + " is an invalid block");
    assertThat(forkchoiceResult.getPayloadId()).isNull();
  }

  private JsonRpcResponse invokeForkchoiceUpdated(final EngineForkchoiceUpdatedParameter fcu) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "engine_forkchoiceUpdatedV3", new Object[] {fcu})));
  }
}
