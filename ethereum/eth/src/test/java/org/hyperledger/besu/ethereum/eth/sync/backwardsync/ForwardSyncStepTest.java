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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockWithAccessList;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.referencetests.ForestReferenceTestWorldState;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ForwardSyncStepTest {

  public static final int REMOTE_HEIGHT = 50;
  public static final int LOCAL_HEIGHT = 25;
  private static final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BackwardSyncContext context;

  @Mock private SynchronizerConfiguration syncConfig;
  @Mock private PeerTaskExecutor peerTaskExecutor;

  private MutableBlockchain remoteBlockchain;
  private RespondingEthPeer peer;

  private final ProtocolSchedule protocolSchedule =
      MainnetProtocolSchedule.fromConfig(
          new StubGenesisConfigOptions(),
          MiningConfiguration.MINING_DISABLED,
          new BadBlockManager(),
          false,
          BalConfiguration.DEFAULT,
          new NoOpMetricsSystem());
  private MutableBlockchain localBlockchain;
  GenericKeyValueStorageFacade<Hash, BlockHeader> headersStorage;
  GenericKeyValueStorageFacade<Hash, Block> blocksStorage;
  GenericKeyValueStorageFacade<Hash, Hash> chainStorage;
  GenericKeyValueStorageFacade<String, BlockHeader> sessionDataStorage;

  @BeforeEach
  public void setup() {
    headersStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksHeadersConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    blocksStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    chainStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new HashConvertor(),
            new InMemoryKeyValueStorage());
    sessionDataStorage =
        new GenericKeyValueStorageFacade<>(
            key -> key.getBytes(StandardCharsets.UTF_8),
            new BlocksHeadersConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());

    Block genesisBlock = blockDataGenerator.genesisBlock();
    remoteBlockchain = createInMemoryBlockchain(genesisBlock);
    localBlockchain = createInMemoryBlockchain(genesisBlock);

    for (int i = 1; i <= REMOTE_HEIGHT; i++) {
      final BlockDataGenerator.BlockOptions options =
          new BlockDataGenerator.BlockOptions()
              .setBlockNumber(i)
              .setParentHash(remoteBlockchain.getBlockHashByNumber(i - 1).orElseThrow());
      final Block block = blockDataGenerator.block(options);
      final List<TransactionReceipt> receipts = blockDataGenerator.receipts(block);

      remoteBlockchain.appendBlock(block, receipts);
      if (i <= LOCAL_HEIGHT) {
        localBlockchain.appendBlock(block, receipts);
      }
    }
    when(syncConfig.getSyncMode()).thenReturn(SyncMode.FULL);

    when(context.getProtocolContext().getBlockchain()).thenReturn(localBlockchain);
    when(context.getProtocolSchedule()).thenReturn(protocolSchedule);
    when(context.getBatchSize()).thenReturn(2);
    when(context.getSynchronizerConfiguration()).thenReturn(syncConfig);
    EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setSynchronizerConfiguration(syncConfig)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();

    peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager);
    EthContext ethContext = ethProtocolManager.ethContext();
    when(context.getEthContext()).thenReturn(ethContext);

    when(context
            .getBlockValidatorForBlock(any())
            .validateAndProcessBlock(any(), any(), any(), any(), any(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              final Object[] arguments = invocation.getArguments();
              Block block = (Block) arguments[1];
              return new BlockProcessingResult(
                  Optional.of(
                      new BlockProcessingOutputs(
                          ForestReferenceTestWorldState.create(Collections.emptyMap()),
                          blockDataGenerator.receipts(block))));
            });

    when(peerTaskExecutor.execute(any(GetBodiesFromPeerTask.class)))
        .thenAnswer(
            (invocationOnMock) -> {
              GetBodiesFromPeerTask task =
                  invocationOnMock.getArgument(0, GetBodiesFromPeerTask.class);
              List<Block> blocks =
                  task.getBlockHeaders().stream()
                      .map(
                          (bh) ->
                              new Block(bh, remoteBlockchain.getBlockBody(bh.getBlockHash()).get()))
                      .collect(Collectors.toList());
              return new PeerTaskExecutorResult<List<Block>>(
                  Optional.of(blocks),
                  PeerTaskExecutorResponseCode.SUCCESS,
                  List.of(peer.getEthPeer()));
            });
  }

  @Test
  public void shouldExecuteForwardSyncWhenPossible() throws Exception {
    final BackwardChain backwardChain = createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 3);
    ForwardSyncStep step = new ForwardSyncStep(context, backwardChain);

    final CompletableFuture<Void> completableFuture = step.executeAsync();

    completableFuture.get();
  }

  @Test
  public void shouldNotRequestWhenNull() {
    ForwardSyncStep phase = new ForwardSyncStep(context, null);
    final CompletableFuture<Void> completableFuture =
        phase.possibleRequestBodies(Collections.emptyList());
    assertThat(completableFuture.isDone()).isTrue();

    final CompletableFuture<Void> completableFuture1 =
        phase.possibleRequestBodies(Collections.emptyList());
    assertThat(completableFuture1.isDone()).isTrue();
  }

  @Test
  public void shouldFindBlockWhenRequested() throws Exception {
    ForwardSyncStep step =
        new ForwardSyncStep(context, createBackwardChain(LOCAL_HEIGHT + 1, LOCAL_HEIGHT + 3));

    final CompletableFuture<List<Block>> future =
        step.requestBodies(List.of(getBlockByNumber(LOCAL_HEIGHT + 1).getHeader()));
    final List<Block> blocks = future.get();
    assertThat(blocks).hasSize(1).containsExactlyInAnyOrder(getBlockByNumber(LOCAL_HEIGHT + 1));
  }

  @Test
  void requestBlockAccessLists_skipsPeerTaskWhenNoBalHashes() throws Exception {
    final ForwardSyncStep step =
        new ForwardSyncStep(context, createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 1));
    final Block blockWithoutBal = getBlockByNumber(LOCAL_HEIGHT + 1);
    assertThat(blockWithoutBal.getHeader().getBalHash()).isEmpty();

    final Map<Hash, BlockAccessList> bals =
        step.requestBlockAccessLists(List.of(blockWithoutBal)).get();

    assertThat(bals).isEmpty();
    verify(peerTaskExecutor, never()).execute(any(GetBlockAccessListsFromPeerTask.class));
  }

  @Test
  void requestBlockAccessLists_indexesAvailableBalsByBlockHash() throws Exception {
    final BlockWithAccessList first = blockWithBal(LOCAL_HEIGHT + 1);
    final BlockWithAccessList second = blockWithBal(LOCAL_HEIGHT + 2);
    stubSuccessfulBalDownload(List.of(first.getBlockAccessList(), second.getBlockAccessList()));

    final ForwardSyncStep step =
        new ForwardSyncStep(context, createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 1));
    final Map<Hash, BlockAccessList> bals =
        step.requestBlockAccessLists(List.of(first.getBlock(), second.getBlock())).get();

    assertThat(bals)
        .hasSize(2)
        .containsEntry(first.getBlock().getHash(), first.getBlockAccessList().orElseThrow())
        .containsEntry(second.getBlock().getHash(), second.getBlockAccessList().orElseThrow());
    verify(peerTaskExecutor).execute(any(GetBlockAccessListsFromPeerTask.class));
  }

  @Test
  void requestBlockAccessLists_ignoresUnavailableEntriesAndKeepsPresentOnes() throws Exception {
    final BlockWithAccessList first = blockWithBal(LOCAL_HEIGHT + 1);
    final BlockWithAccessList second = blockWithBal(LOCAL_HEIGHT + 2);
    stubSuccessfulBalDownload(List.of(first.getBlockAccessList(), Optional.empty()));

    final ForwardSyncStep step =
        new ForwardSyncStep(context, createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 1));
    final Map<Hash, BlockAccessList> bals =
        step.requestBlockAccessLists(List.of(first.getBlock(), second.getBlock())).get();

    assertThat(bals)
        .hasSize(1)
        .containsEntry(first.getBlock().getHash(), first.getBlockAccessList().orElseThrow())
        .doesNotContainKey(second.getBlock().getHash());
  }

  @Test
  void requestBlockAccessLists_returnsEmptyOnPeerFailure() throws Exception {
    final BlockWithAccessList withBal = blockWithBal(LOCAL_HEIGHT + 1);
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(),
                PeerTaskExecutorResponseCode.INVALID_RESPONSE,
                List.of(peer.getEthPeer())));

    final ForwardSyncStep step =
        new ForwardSyncStep(context, createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 1));
    final Map<Hash, BlockAccessList> bals =
        step.requestBlockAccessLists(List.of(withBal.getBlock())).get();

    assertThat(bals).isEmpty();
  }

  @Test
  void requestBlockAccessLists_returnsEmptyWhenPeerThrows() throws Exception {
    final BlockWithAccessList withBal = blockWithBal(LOCAL_HEIGHT + 1);
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenThrow(new RuntimeException("no eth70 peers"));

    final ForwardSyncStep step =
        new ForwardSyncStep(context, createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 1));
    final Map<Hash, BlockAccessList> bals =
        step.requestBlockAccessLists(List.of(withBal.getBlock())).get();

    assertThat(bals).isEmpty();
  }

  @Test
  void saveBlocks_passesDownloadedBalIntoSaveBlock() {
    final BlockWithAccessList withBal = blockWithBal(LOCAL_HEIGHT + 1);
    assertThat(localBlockchain.contains(withBal.getBlock().getHeader().getParentHash())).isTrue();

    final ForwardSyncStep step =
        new ForwardSyncStep(context, createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 1));
    final BlockAccessList bal = withBal.getBlockAccessList().orElseThrow();
    step.saveBlocks(
        Map.entry(List.of(withBal.getBlock()), Map.of(withBal.getBlock().getHash(), bal)));

    verify(context).saveBlock(withBal.getBlock(), Optional.of(bal));
  }

  @Test
  void saveBlocks_passesEmptyOptionalWhenBalMissing() {
    final Block block = getBlockByNumber(LOCAL_HEIGHT + 1);
    final ForwardSyncStep step =
        new ForwardSyncStep(context, createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 1));

    step.saveBlocks(Map.entry(List.of(block), Map.of()));

    verify(context).saveBlock(block, Optional.empty());
  }

  @Test
  void possibleRequestBodies_downloadsBalsAndPassesThemToSaveBlock() throws Exception {
    final BlockWithAccessList first = blockWithBal(LOCAL_HEIGHT + 1);
    final BlockWithAccessList second =
        blockDataGenerator.blockWithAccessList(
            new BlockDataGenerator.BlockOptions()
                .setBlockNumber(LOCAL_HEIGHT + 2)
                .setParentHash(first.getBlock().getHash())
                .withGeneratedBlockAccessList(2));
    assertThat(first.getBlock().getHeader().getBalHash()).isPresent();
    assertThat(second.getBlock().getHeader().getBalHash()).isPresent();
    when(context.getBatchSize()).thenReturn(2);
    // saveBlock must advance the local chain so the next block's parent is found
    doAnswer(
            invocation -> {
              final Block block = invocation.getArgument(0);
              localBlockchain.appendBlock(block, blockDataGenerator.receipts(block));
              return null;
            })
        .when(context)
        .saveBlock(any(Block.class), any());
    when(peerTaskExecutor.execute(any(GetBodiesFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(new ArrayList<>(List.of(first.getBlock(), second.getBlock()))),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer.getEthPeer())));
    stubSuccessfulBalDownload(List.of(first.getBlockAccessList(), second.getBlockAccessList()));

    final BackwardChain backwardChain =
        new BackwardChain(headersStorage, blocksStorage, chainStorage, sessionDataStorage);
    backwardChain.appendTrustedBlock(second.getBlock());
    backwardChain.prependAncestorsHeader(first.getBlock().getHeader());

    final ForwardSyncStep step = new ForwardSyncStep(context, backwardChain);
    step.possibleRequestBodies(List.of(first.getBlock().getHeader(), second.getBlock().getHeader()))
        .get();

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Optional<BlockAccessList>> balCaptor =
        ArgumentCaptor.forClass(Optional.class);
    verify(context, times(2)).saveBlock(any(Block.class), balCaptor.capture());
    assertThat(balCaptor.getAllValues())
        .containsExactly(first.getBlockAccessList(), second.getBlockAccessList());
  }

  @Test
  void possibleRequestBodies_stillSavesBlocksWhenBalDownloadFails() throws Exception {
    final BlockWithAccessList withBal = blockWithBal(LOCAL_HEIGHT + 1);
    assertThat(withBal.getBlock().getHeader().getBalHash()).isPresent();
    when(context.getBatchSize()).thenReturn(1);
    when(peerTaskExecutor.execute(any(GetBodiesFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(new ArrayList<>(List.of(withBal.getBlock()))),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer.getEthPeer())));
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(),
                PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE,
                List.of(peer.getEthPeer())));

    final ForwardSyncStep step =
        new ForwardSyncStep(context, createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 1));
    step.possibleRequestBodies(List.of(withBal.getBlock().getHeader())).get();

    verify(context).saveBlock(eq(withBal.getBlock()), eq(Optional.empty()));
  }

  private void stubSuccessfulBalDownload(final List<Optional<BlockAccessList>> bals) {
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(bals),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer.getEthPeer())));
  }

  private BlockWithAccessList blockWithBal(final long number) {
    final Hash parentHash = remoteBlockchain.getBlockHashByNumber(number - 1).orElseThrow();
    return blockDataGenerator.blockWithAccessList(
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(number)
            .setParentHash(parentHash)
            .withGeneratedBlockAccessList(2));
  }

  private BackwardChain createBackwardChain(final int from, final int until) {
    BackwardChain chain = backwardChainFromBlock(until);
    for (int i = until; i > from; --i) {
      chain.prependAncestorsHeader(getBlockByNumber(i - 1).getHeader());
    }
    return chain;
  }

  @NotNull
  private BackwardChain backwardChainFromBlock(final int number) {
    final BackwardChain backwardChain =
        new BackwardChain(headersStorage, blocksStorage, chainStorage, sessionDataStorage);
    backwardChain.appendTrustedBlock(remoteBlockchain.getBlockByNumber(number).orElseThrow());
    return backwardChain;
  }

  @NotNull
  private Block getBlockByNumber(final int number) {
    return remoteBlockchain.getBlockByNumber(number).orElseThrow();
  }
}
