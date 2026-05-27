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
package org.hyperledger.besu.ethereum.eth.sync.tasks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class DetermineCommonAncestorTaskParameterizedTest {
  private final ProtocolSchedule protocolSchedule = ProtocolScheduleFixture.TESTING_NETWORK;
  private static final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
  private final MetricsSystem metricsSystem = new NoOpMetricsSystem();

  private static Block genesisBlock;
  private static MutableBlockchain localBlockchain;
  protected static final int chainHeight = 50;

  private MutableBlockchain remoteBlockchain;
  private PeerTaskExecutor peerTaskExecutor;

  @BeforeAll
  public static void setupClass() {
    genesisBlock = blockDataGenerator.genesisBlock();
    localBlockchain = createInMemoryBlockchain(genesisBlock);

    // Setup local chain
    for (int i = 1; i <= chainHeight; i++) {
      final BlockDataGenerator.BlockOptions options =
          new BlockDataGenerator.BlockOptions()
              .setBlockNumber(i)
              .setParentHash(localBlockchain.getBlockHashByNumber(i - 1).get());
      final Block block = blockDataGenerator.block(options);
      final List<TransactionReceipt> receipts = blockDataGenerator.receipts(block);
      localBlockchain.appendBlock(block, receipts);
    }
  }

  @BeforeEach
  public void setup() {
    remoteBlockchain = createInMemoryBlockchain(genesisBlock);
    peerTaskExecutor = mock(PeerTaskExecutor.class);
  }

  public static Stream<Arguments> parameters() {
    final int[] requestSizes = {5, 12, chainHeight, chainHeight * 2};
    final Stream.Builder<Arguments> builder = Stream.builder();
    for (final int requestSize : requestSizes) {
      // Test boundary-sensitive heights rather than every height 0..chainHeight.
      // The algorithm batches headers in groups of requestSize, so only heights at
      // batch boundaries (and their neighbours) exercise different code paths.
      final Set<Integer> heights = new LinkedHashSet<>();
      heights.add(0);
      heights.add(1);
      heights.add(chainHeight / 2);
      heights.add(chainHeight - 1);
      heights.add(chainHeight);
      for (int boundary = requestSize; boundary < chainHeight; boundary += requestSize) {
        heights.add(boundary - 1);
        heights.add(boundary);
        heights.add(boundary + 1);
      }
      heights.stream()
          .filter(h -> h >= 0 && h <= chainHeight)
          .sorted()
          .forEach(h -> builder.add(Arguments.of(requestSize, h)));
    }
    return builder.build();
  }

  @ParameterizedTest(name = "requestSize={0}, commonAncestor={1}")
  @MethodSource("parameters")
  public void searchesAgainstNetwork(final int headerRequestSize, final int commonAncestorHeight) {
    BlockHeader commonHeader = genesisBlock.getHeader();
    for (long i = 1; i <= commonAncestorHeight; i++) {
      commonHeader = localBlockchain.getBlockHeader(i).get();
      final List<TransactionReceipt> receipts =
          localBlockchain.getTxReceipts(commonHeader.getHash()).get();
      final BlockBody commonBody = localBlockchain.getBlockBody(commonHeader.getHash()).get();
      remoteBlockchain.appendBlock(new Block(commonHeader, commonBody), receipts);
    }

    // Remaining blocks are disparate...
    for (long i = commonAncestorHeight + 1L; i <= chainHeight; i++) {
      final BlockDataGenerator.BlockOptions localOptions =
          new BlockDataGenerator.BlockOptions()
              .setBlockNumber(i)
              .setParentHash(localBlockchain.getBlockHashByNumber(i - 1).get());
      final Block localBlock = blockDataGenerator.block(localOptions);
      final List<TransactionReceipt> localReceipts = blockDataGenerator.receipts(localBlock);
      localBlockchain.appendBlock(localBlock, localReceipts);

      final BlockDataGenerator.BlockOptions remoteOptions =
          new BlockDataGenerator.BlockOptions()
              .setDifficulty(Difficulty.ONE) // differentiator
              .setBlockNumber(i)
              .setParentHash(remoteBlockchain.getBlockHashByNumber(i - 1).get());
      final Block remoteBlock = blockDataGenerator.block(remoteOptions);
      final List<TransactionReceipt> remoteReceipts = blockDataGenerator.receipts(remoteBlock);
      remoteBlockchain.appendBlock(remoteBlock, remoteReceipts);
    }

    final WorldStateArchive worldStateArchive = createInMemoryWorldStateArchive();
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(protocolSchedule)
            .setBlockchain(localBlockchain)
            .setWorldStateArchive(worldStateArchive)
            .setTransactionPool(mock(TransactionPool.class))
            .setEthereumWireProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();
    final RespondingEthPeer.Responder responder =
        RespondingEthPeer.blockchainResponder(remoteBlockchain);
    final RespondingEthPeer respondingEthPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager);

    // Execute task and wait for response
    final AtomicReference<BlockHeader> actualResult = new AtomicReference<>();
    final AtomicBoolean done = new AtomicBoolean(false);

    final EthContext ethContext = ethProtocolManager.ethContext();
    final ProtocolContext protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(localBlockchain)
            .withWorldStateArchive(worldStateArchive)
            .build();

    final EthTask<BlockHeader> task =
        DetermineCommonAncestorTask.create(
            protocolSchedule,
            protocolContext,
            ethContext,
            respondingEthPeer.getEthPeer(),
            headerRequestSize,
            metricsSystem);

    when(peerTaskExecutor.executeAgainstPeer(
            Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(respondingEthPeer.getEthPeer())))
        .thenAnswer(
            (invocationOnMock) -> {
              GetHeadersFromPeerTask getHeadersTask =
                  invocationOnMock.getArgument(0, GetHeadersFromPeerTask.class);
              long blockNumber = getHeadersTask.getBlockNumber();
              int maxHeaders = getHeadersTask.getMaxHeaders();
              int skip = getHeadersTask.getSkip();

              List<BlockHeader> headers = new ArrayList<>();
              long lowerBound = Math.max(0, blockNumber - (maxHeaders - 1) * (skip + 1));
              for (long i = blockNumber; i > lowerBound; i -= skip + 1) {
                headers.add(remoteBlockchain.getBlockHeader(i).get());
              }

              return new PeerTaskExecutorResult<>(
                  Optional.of(headers),
                  PeerTaskExecutorResponseCode.SUCCESS,
                  List.of(respondingEthPeer.getEthPeer()));
            });

    final CompletableFuture<BlockHeader> future = task.run();
    respondingEthPeer.respondWhile(responder, () -> !future.isDone());

    future.whenComplete(
        (response, error) -> {
          actualResult.set(response);
          done.compareAndSet(false, true);
        });

    assertThat(actualResult.get()).isNotNull();
    assertThat(actualResult.get().getHash())
        .isEqualTo(MainnetBlockHeaderFunctions.createHash(commonHeader));
  }
}
