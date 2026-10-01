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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockWithAccessList;
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
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
public class BackwardSyncBalImporterTest {

  public static final int REMOTE_HEIGHT = 50;
  public static final int LOCAL_HEIGHT = 25;
  private static final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BackwardSyncContext context;

  @Mock private SynchronizerConfiguration syncConfig;
  @Mock private PeerTaskExecutor peerTaskExecutor;

  private MutableBlockchain remoteBlockchain;
  private MutableBlockchain localBlockchain;
  private RespondingEthPeer peer;

  @BeforeEach
  public void setup() {
    final Block genesisBlock = blockDataGenerator.genesisBlock();
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
    when(context.getBatchSize()).thenReturn(2);
    when(context.getSynchronizerConfiguration()).thenReturn(syncConfig);

    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setSynchronizerConfiguration(syncConfig)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();
    peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager);
    final EthContext ethContext = ethProtocolManager.ethContext();
    when(context.getEthContext()).thenReturn(ethContext);
  }

  @Test
  void importBlocks_skipsPeerTaskWhenNoBalHashes() {
    final Block blockWithoutBal = getBlockByNumber(LOCAL_HEIGHT + 1);
    assertThat(blockWithoutBal.getHeader().getBalHash()).isEmpty();

    importBlocks(List.of(blockWithoutBal));

    verify(peerTaskExecutor, never()).execute(any(GetBlockAccessListsFromPeerTask.class));
    verify(context).saveBlock(blockWithoutBal, Optional.empty());
  }

  @Test
  void importBlocks_passesDownloadedBalsToSaveBlock() {
    final BlockWithAccessList first = blockWithBal(LOCAL_HEIGHT + 1);
    final BlockWithAccessList second =
        blockDataGenerator.blockWithAccessList(
            new BlockDataGenerator.BlockOptions()
                .setBlockNumber(LOCAL_HEIGHT + 2)
                .setParentHash(first.getBlock().getHash())
                .withGeneratedBlockAccessList(2));
    stubSuccessfulBalDownload(List.of(first.getBlockAccessList(), second.getBlockAccessList()));
    when(context.getBatchSize()).thenReturn(2);

    // Advance chain after each save so the next parent is found.
    org.mockito.Mockito.doAnswer(
            invocation -> {
              final Block block = invocation.getArgument(0);
              localBlockchain.appendBlock(block, blockDataGenerator.receipts(block));
              return null;
            })
        .when(context)
        .saveBlock(any(Block.class), any());

    importBlocks(List.of(first.getBlock(), second.getBlock()));

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Optional<BlockAccessList>> balCaptor =
        ArgumentCaptor.forClass(Optional.class);
    verify(context, org.mockito.Mockito.times(2)).saveBlock(any(Block.class), balCaptor.capture());
    assertThat(balCaptor.getAllValues())
        .containsExactly(first.getBlockAccessList(), second.getBlockAccessList());
  }

  @Test
  void importBlocks_continuesWithoutBalWhenDownloadFails() {
    final BlockWithAccessList withBal = blockWithBal(LOCAL_HEIGHT + 1);
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(),
                PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE,
                List.of(peer.getEthPeer())));

    importBlocks(List.of(withBal.getBlock()));

    verify(context).saveBlock(eq(withBal.getBlock()), eq(Optional.empty()));
  }

  @Test
  void importBlocks_usesPartialBalResponseAndImportsRestWithout() {
    final BlockWithAccessList first = blockWithBal(LOCAL_HEIGHT + 1);
    final BlockWithAccessList second =
        blockDataGenerator.blockWithAccessList(
            new BlockDataGenerator.BlockOptions()
                .setBlockNumber(LOCAL_HEIGHT + 2)
                .setParentHash(first.getBlock().getHash())
                .withGeneratedBlockAccessList(2));
    // Soft-limited response: only first BAL returned for the window.
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(first.getBlockAccessList())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer.getEthPeer())))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(Optional.empty())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer.getEthPeer())));
    when(context.getBatchSize()).thenReturn(2);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              final Block block = invocation.getArgument(0);
              localBlockchain.appendBlock(block, blockDataGenerator.receipts(block));
              return null;
            })
        .when(context)
        .saveBlock(any(Block.class), any());

    importBlocks(List.of(first.getBlock(), second.getBlock()));

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Optional<BlockAccessList>> balCaptor =
        ArgumentCaptor.forClass(Optional.class);
    verify(context, org.mockito.Mockito.times(2)).saveBlock(any(Block.class), balCaptor.capture());
    assertThat(balCaptor.getAllValues().get(0)).isEqualTo(first.getBlockAccessList());
    assertThat(balCaptor.getAllValues().get(1)).isEmpty();
  }

  @Test
  void importBlocks_requestsFailedWindowOnlyOnce() {
    final List<BlockWithAccessList> chain = chainWithBals(3);
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(),
                PeerTaskExecutorResponseCode.TIMEOUT,
                List.of(peer.getEthPeer())));
    appendOnSave();

    importBlocks(blocksOf(chain));

    verify(peerTaskExecutor, times(1)).execute(any(GetBlockAccessListsFromPeerTask.class));
    verify(context, times(3)).saveBlock(any(Block.class), eq(Optional.empty()));
  }

  @Test
  void importBlocks_fetchesOneRequestPerWindow() {
    final int nbBlocks = BackwardSyncBalImporter.BAL_REQUEST_WINDOW + 4;
    final List<BlockWithAccessList> chain = chainWithBals(nbBlocks);
    final List<Optional<BlockAccessList>> bals =
        chain.stream().map(BlockWithAccessList::getBlockAccessList).toList();
    final int window = BackwardSyncBalImporter.BAL_REQUEST_WINDOW;
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(bals.subList(0, window)),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer.getEthPeer())))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(bals.subList(window, nbBlocks)),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer.getEthPeer())));
    appendOnSave();

    importBlocks(blocksOf(chain));

    verify(peerTaskExecutor, times(2)).execute(any(GetBlockAccessListsFromPeerTask.class));
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Optional<BlockAccessList>> balCaptor =
        ArgumentCaptor.forClass(Optional.class);
    verify(context, times(nbBlocks)).saveBlock(any(Block.class), balCaptor.capture());
    assertThat(balCaptor.getAllValues()).containsExactlyElementsOf(bals);
  }

  @Test
  void importBlocks_usesPrefetchedFirstWindow() {
    final List<BlockWithAccessList> chain = chainWithBals(2);
    appendOnSave();

    new BackwardSyncBalImporter(context)
        .importBlocks(
            blocksOf(chain),
            CompletableFuture.completedFuture(
                Map.of(
                    chain.get(0).getBlock().getHash(),
                    chain.get(0).getBlockAccessList().orElseThrow())));

    verify(peerTaskExecutor, never()).execute(any(GetBlockAccessListsFromPeerTask.class));
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Optional<BlockAccessList>> balCaptor =
        ArgumentCaptor.forClass(Optional.class);
    verify(context, times(2)).saveBlock(any(Block.class), balCaptor.capture());
    assertThat(balCaptor.getAllValues())
        .containsExactly(chain.get(0).getBlockAccessList(), Optional.empty());
  }

  @Test
  void importBlocks_continuesWithoutBalWhenDownloadTimesOut() {
    final List<BlockWithAccessList> chain = chainWithBals(1);
    final CompletableFuture<Map<Hash, BlockAccessList>> neverCompletingPrefetch =
        new CompletableFuture<>();
    appendOnSave();

    new BackwardSyncBalImporter(context).importBlocks(blocksOf(chain), neverCompletingPrefetch);

    verify(context).saveBlock(eq(chain.get(0).getBlock()), eq(Optional.empty()));
    assertThat(neverCompletingPrefetch).isCancelled();
  }

  @Test
  void importBlocks_cancelsPrefetchWhenNothingToImport() {
    final CompletableFuture<Map<Hash, BlockAccessList>> prefetch = new CompletableFuture<>();

    new BackwardSyncBalImporter(context).importBlocks(List.of(), prefetch);

    assertThat(prefetch).isCancelled();
  }

  @Test
  void lookupStoredBal_skipsStorageWhenNoBalHash() {
    final Block block = getBlockByNumber(LOCAL_HEIGHT + 1);
    assertThat(block.getHeader().getBalHash()).isEmpty();

    assertThat(new BackwardSyncBalImporter(context).lookupStoredBal(block.getHeader())).isEmpty();
  }

  private void importBlocks(final List<Block> blocks) {
    final BackwardSyncBalImporter importer = new BackwardSyncBalImporter(context);
    importer.importBlocks(
        blocks, importer.prefetchFirstWindow(blocks.stream().map(Block::getHeader).toList()));
  }

  private void stubSuccessfulBalDownload(final List<Optional<BlockAccessList>> bals) {
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(bals),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer.getEthPeer())));
  }

  private List<BlockWithAccessList> chainWithBals(final int count) {
    final List<BlockWithAccessList> chain = new java.util.ArrayList<>();
    Hash parentHash = localBlockchain.getChainHeadHash();
    for (int i = 1; i <= count; i++) {
      final BlockWithAccessList block =
          blockDataGenerator.blockWithAccessList(
              new BlockDataGenerator.BlockOptions()
                  .setBlockNumber(LOCAL_HEIGHT + i)
                  .setParentHash(parentHash)
                  .withGeneratedBlockAccessList(2));
      chain.add(block);
      parentHash = block.getBlock().getHash();
    }
    return chain;
  }

  private static List<Block> blocksOf(final List<BlockWithAccessList> chain) {
    return chain.stream().map(BlockWithAccessList::getBlock).toList();
  }

  private void appendOnSave() {
    org.mockito.Mockito.doAnswer(
            invocation -> {
              final Block block = invocation.getArgument(0);
              localBlockchain.appendBlock(block, blockDataGenerator.receipts(block));
              return null;
            })
        .when(context)
        .saveBlock(any(Block.class), any());
  }

  private BlockWithAccessList blockWithBal(final long number) {
    final Hash parentHash = remoteBlockchain.getBlockHashByNumber(number - 1).orElseThrow();
    return blockDataGenerator.blockWithAccessList(
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(number)
            .setParentHash(parentHash)
            .withGeneratedBlockAccessList(2));
  }

  private Block getBlockByNumber(final int number) {
    return remoteBlockchain.getBlockByNumber(number).orElseThrow();
  }
}
