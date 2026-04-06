/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethstats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethstats.request.EthStatsRequest;
import org.hyperledger.besu.ethstats.util.EthStatsConnectOptions;
import org.hyperledger.besu.ethstats.util.ImmutableEthStatsConnectOptions;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.http.WebSocketConnectOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EthStatsServiceTest {

  @Mock private Vertx vertx;
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private EthProtocolManager ethProtocolManager;
  @Mock private TransactionPool transactionPool;
  @Mock private MiningCoordinator miningCoordinator;
  @Mock private SyncState syncState;
  @Mock private GenesisConfigOptions genesisConfigOptions;
  @Mock private P2PNetwork p2PNetwork;
  @Mock private EthContext ethContext;
  @Mock private EthScheduler ethScheduler;
  @Mock private WebSocketClient webSocketClient;
  @Mock private WebSocket webSocket;

  final EthStatsConnectOptions ethStatsConnectOptions =
      ImmutableEthStatsConnectOptions.builder()
          .nodeName("besu-node")
          .secret("secret")
          .host("127.0.0.1")
          .port(1111)
          .contact("contact@test.net")
          .ethStatsReportInterval(5)
          .build();

  final EnodeURLImpl node =
      EnodeURLImpl.builder()
          .nodeId(
              "50203c6bfca6874370e71aecc8958529fd723feb05013dc1abca8fc1fff845c5259faba05852e9dfe5ce172a7d6e7c2a3a5eaa8b541c8af15ea5518bbff5f2fa")
          .useDefaultPorts()
          .ipAddress("127.0.0.1")
          .listeningPort(30304)
          .build();

  private EthStatsService ethStatsService;

  @BeforeEach
  public void initMocks() {
    when(ethProtocolManager.ethContext()).thenReturn(ethContext);
    when(ethContext.getScheduler()).thenReturn(ethScheduler);
    when(vertx.createWebSocketClient(any(WebSocketClientOptions.class)))
        .thenReturn(webSocketClient);
    when(genesisConfigOptions.getChainId()).thenReturn(Optional.of(BigInteger.ONE));
    when(ethProtocolManager.getSupportedCapabilities())
        .thenReturn(List.of(Capability.create("eth64", 1)));
  }

  @Test
  public void shouldRetryWhenLocalEnodeNotAvailable() throws Exception {
    ethStatsService =
        new EthStatsService(
            ethStatsConnectOptions,
            blockchainQueries,
            ethProtocolManager,
            transactionPool,
            miningCoordinator,
            syncState,
            vertx,
            "clientVersion",
            genesisConfigOptions,
            p2PNetwork);
    when(p2PNetwork.getLocalEnode()).thenThrow(new NoSuchElementException());
    ethStatsService.start();
    verify(ethScheduler, times(1)).scheduleFutureTask(any(Runnable.class), any(Duration.class));
  }

  @Test
  public void shouldSendHelloMessage() {
    ethStatsService =
        new EthStatsService(
            ethStatsConnectOptions,
            blockchainQueries,
            ethProtocolManager,
            transactionPool,
            miningCoordinator,
            syncState,
            vertx,
            "clientVersion",
            genesisConfigOptions,
            p2PNetwork);
    when(p2PNetwork.getLocalEnode()).thenReturn(Optional.of(node));

    final ArgumentCaptor<Handler<AsyncResult<WebSocket>>> webSocketCaptor =
        ArgumentCaptor.forClass(Handler.class);

    ethStatsService.start();

    verify(webSocketClient, times(1))
        .connect(any(WebSocketConnectOptions.class), webSocketCaptor.capture());
    webSocketCaptor.getValue().handle(succeededWebSocketEvent(Optional.of(webSocket)));

    final ArgumentCaptor<String> helloMessageCaptor = ArgumentCaptor.forClass(String.class);
    verify(webSocket, times(1)).writeTextMessage(helloMessageCaptor.capture(), any(Handler.class));

    assertThat(helloMessageCaptor.getValue().contains(EthStatsRequest.Type.HELLO.getValue()))
        .isTrue();
  }

  @Test
  public void shouldRetryIfSendHelloMessageFailed() {

    ethStatsService =
        new EthStatsService(
            ethStatsConnectOptions,
            blockchainQueries,
            ethProtocolManager,
            transactionPool,
            miningCoordinator,
            syncState,
            vertx,
            "clientVersion",
            genesisConfigOptions,
            p2PNetwork);
    when(p2PNetwork.getLocalEnode()).thenReturn(Optional.of(node));

    ethStatsService.start();

    final ArgumentCaptor<Handler<AsyncResult<WebSocket>>> webSocketCaptor =
        ArgumentCaptor.forClass(Handler.class);
    verify(webSocketClient, times(1))
        .connect(any(WebSocketConnectOptions.class), webSocketCaptor.capture());
    webSocketCaptor.getValue().handle(succeededWebSocketEvent(Optional.of(webSocket)));

    final ArgumentCaptor<Handler<AsyncResult<Void>>> helloMessageCaptor =
        ArgumentCaptor.forClass(Handler.class);
    verify(webSocket, times(1)).writeTextMessage(anyString(), helloMessageCaptor.capture());
    helloMessageCaptor.getValue().handle(failedWebSocketEvent(Optional.empty()));

    verify(ethScheduler, times(1)).scheduleFutureTask(any(Runnable.class), any(Duration.class));
  }

  @Test
  public void shouldSendFullReportIfHelloMessageSucceeded() {
    ethStatsService =
        new EthStatsService(
            ethStatsConnectOptions,
            blockchainQueries,
            ethProtocolManager,
            transactionPool,
            miningCoordinator,
            syncState,
            vertx,
            "clientVersion",
            genesisConfigOptions,
            p2PNetwork);
    when(p2PNetwork.getLocalEnode()).thenReturn(Optional.of(node));

    final ArgumentCaptor<Handler<AsyncResult<WebSocket>>> webSocketCaptor =
        ArgumentCaptor.forClass(Handler.class);

    ethStatsService.start();

    verify(webSocketClient, times(1))
        .connect(any(WebSocketConnectOptions.class), webSocketCaptor.capture());
    webSocketCaptor.getValue().handle(succeededWebSocketEvent(Optional.of(webSocket)));

    final ArgumentCaptor<Handler<String>> textMessageHandlerCaptor =
        ArgumentCaptor.forClass(Handler.class);
    verify(webSocket, times(1)).textMessageHandler(textMessageHandlerCaptor.capture());

    textMessageHandlerCaptor.getValue().handle("{\"emit\":[\"ready\"]}");

    verify(ethScheduler, times(1)).scheduleFutureTaskWithFixedDelay(any(), any(), any());
  }

  @Test
  public void shouldSendBlockMessage() throws Exception {
    ethStatsService =
        new EthStatsService(
            ethStatsConnectOptions,
            blockchainQueries,
            ethProtocolManager,
            transactionPool,
            miningCoordinator,
            syncState,
            vertx,
            "clientVersion",
            genesisConfigOptions,
            p2PNetwork);

    final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
    final Block testBlock = blockDataGenerator.block();
    final BlockWithMetadata<TransactionWithMetadata, Hash> blockWithMetadata =
        new BlockWithMetadata<>(
            testBlock.getHeader(),
            List.of(),
            List.of(),
            testBlock.getHeader().getDifficulty(),
            testBlock.getSize(),
            Optional.empty());

    when(p2PNetwork.getLocalEnode()).thenReturn(Optional.of(node));
    when(blockchainQueries.latestBlock()).thenReturn(Optional.of(blockWithMetadata));

    final ArgumentCaptor<Handler<AsyncResult<WebSocket>>> webSocketCaptor =
        ArgumentCaptor.forClass(Handler.class);

    ethStatsService.start();

    verify(webSocketClient, times(1))
        .connect(any(WebSocketConnectOptions.class), webSocketCaptor.capture());
    webSocketCaptor.getValue().handle(succeededWebSocketEvent(Optional.of(webSocket)));

    // send block message
    ethStatsService.sendBlockReport();
    final ArgumentCaptor<String> messagesCaptor = ArgumentCaptor.forClass(String.class);
    verify(webSocket, times(2)).writeTextMessage(messagesCaptor.capture(), any(Handler.class));

    final List<String> sentMessages = messagesCaptor.getAllValues();
    assertThat(sentMessages.get(0)).contains("hello");

    final String blockMessage = sentMessages.get(1);
    assertThat(blockMessage).contains("\"block\"");

    // verify block message
    final ObjectMapper objectMapper = new ObjectMapper().registerModule(new Jdk8Module());
    final var jsonNode = objectMapper.readTree(blockMessage);
    final var blockReportData = jsonNode.get("emit").get(1);
    final var blockDataNode = blockReportData.get("block");

    final var expectedBlockResult = new BlockResultFactory().transactionComplete(blockWithMetadata);
    final JsonNode expectedBlockResultNode = objectMapper.valueToTree(expectedBlockResult);

    assertThat(blockDataNode).isEqualTo(expectedBlockResultNode);
  }

  private <T> AsyncResult<T> succeededWebSocketEvent(final Optional<T> object) {
    return new AsyncResult<>() {
      @Override
      public T result() {
        return object.orElse(null);
      }

      @Override
      public Throwable cause() {
        return null;
      }

      @Override
      public boolean succeeded() {
        return true;
      }

      @Override
      public boolean failed() {
        return false;
      }
    };
  }

  private AsyncResult<Void> failedWebSocketEvent(final Optional<Throwable> cause) {
    return new AsyncResult<>() {
      @Override
      public Void result() {
        return null;
      }

      @Override
      public Throwable cause() {
        return cause.orElse(null);
      }

      @Override
      public boolean succeeded() {
        return false;
      }

      @Override
      public boolean failed() {
        return true;
      }
    };
  }

  @Test
  public void shouldUseCustomReportInterval() {
    // Test with custom 10-second interval
    final EthStatsConnectOptions customIntervalOptions =
        ImmutableEthStatsConnectOptions.builder()
            .nodeName("besu-node")
            .secret("secret")
            .host("127.0.0.1")
            .port(1111)
            .contact("contact@test.net")
            .ethStatsReportInterval(10) // Custom 10-second interval
            .build();

    ethStatsService =
        new EthStatsService(
            customIntervalOptions,
            blockchainQueries,
            ethProtocolManager,
            transactionPool,
            miningCoordinator,
            syncState,
            vertx,
            "clientVersion",
            genesisConfigOptions,
            p2PNetwork);
    when(p2PNetwork.getLocalEnode()).thenReturn(Optional.of(node));

    final ArgumentCaptor<Handler<AsyncResult<WebSocket>>> webSocketCaptor =
        ArgumentCaptor.forClass(Handler.class);

    ethStatsService.start();

    verify(webSocketClient, times(1))
        .connect(any(WebSocketConnectOptions.class), webSocketCaptor.capture());
    webSocketCaptor.getValue().handle(succeededWebSocketEvent(Optional.of(webSocket)));

    final ArgumentCaptor<Handler<String>> textMessageHandlerCaptor =
        ArgumentCaptor.forClass(Handler.class);
    verify(webSocket, times(1)).textMessageHandler(textMessageHandlerCaptor.capture());

    textMessageHandlerCaptor.getValue().handle("{\"emit\":[\"ready\"]}");

    // Verify that scheduleFutureTaskWithFixedDelay is called with the custom 10-second interval
    final ArgumentCaptor<Duration> initialDelayCaptor = ArgumentCaptor.forClass(Duration.class);
    final ArgumentCaptor<Duration> intervalCaptor = ArgumentCaptor.forClass(Duration.class);
    verify(ethScheduler, times(1))
        .scheduleFutureTaskWithFixedDelay(
            any(Runnable.class), initialDelayCaptor.capture(), intervalCaptor.capture());

    // Verify the interval is 10 seconds (custom value)
    assertThat(intervalCaptor.getValue()).isEqualTo(Duration.ofSeconds(10));
    // Verify initial delay is 0 seconds
    assertThat(initialDelayCaptor.getValue()).isEqualTo(Duration.ofSeconds(0));
  }

  @Test
  public void shouldUseDefaultReportIntervalWhenNotSpecified() {
    // Test with default 5-second interval (backward compatibility)
    ethStatsService =
        new EthStatsService(
            ethStatsConnectOptions, // This uses the default 5-second interval
            blockchainQueries,
            ethProtocolManager,
            transactionPool,
            miningCoordinator,
            syncState,
            vertx,
            "clientVersion",
            genesisConfigOptions,
            p2PNetwork);
    when(p2PNetwork.getLocalEnode()).thenReturn(Optional.of(node));

    final ArgumentCaptor<Handler<AsyncResult<WebSocket>>> webSocketCaptor =
        ArgumentCaptor.forClass(Handler.class);

    ethStatsService.start();

    verify(webSocketClient, times(1))
        .connect(any(WebSocketConnectOptions.class), webSocketCaptor.capture());
    webSocketCaptor.getValue().handle(succeededWebSocketEvent(Optional.of(webSocket)));

    final ArgumentCaptor<Handler<String>> textMessageHandlerCaptor =
        ArgumentCaptor.forClass(Handler.class);
    verify(webSocket, times(1)).textMessageHandler(textMessageHandlerCaptor.capture());

    textMessageHandlerCaptor.getValue().handle("{\"emit\":[\"ready\"]}");

    // Verify that scheduleFutureTaskWithFixedDelay is called with the default 5-second interval
    final ArgumentCaptor<Duration> initialDelayCaptor = ArgumentCaptor.forClass(Duration.class);
    final ArgumentCaptor<Duration> intervalCaptor = ArgumentCaptor.forClass(Duration.class);
    verify(ethScheduler, times(1))
        .scheduleFutureTaskWithFixedDelay(
            any(Runnable.class), initialDelayCaptor.capture(), intervalCaptor.capture());

    // Verify the interval is 5 seconds (default value)
    assertThat(intervalCaptor.getValue()).isEqualTo(Duration.ofSeconds(5));
    // Verify initial delay is 0 seconds
    assertThat(initialDelayCaptor.getValue()).isEqualTo(Duration.ofSeconds(0));
  }

  @Test
  public void shouldGenerateNonEmptyHistoryBlockRange() {
    final List<Long> blocks = EthStatsService.buildHistoryBlockList(100L);

    assertThat(blocks).hasSize(51);
    assertThat(blocks).startsWith(50L);
    assertThat(blocks).endsWith(100L);
  }

  @Test
  public void shouldHandleChainHeadSmallerThanHistoryRange() {
    final List<Long> blocks = EthStatsService.buildHistoryBlockList(30L);

    assertThat(blocks).hasSize(31);
    assertThat(blocks).startsWith(0L);
    assertThat(blocks).endsWith(30L);
  }
}
