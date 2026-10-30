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
package org.hyperledger.besu.ethstats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethstats.util.ImmutableEthStatsConnectOptions;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.junit.jupiter.api.Test;

/** Exercises EthStatsService against a real Vert.x WebSocket server. */
public class EthStatsServiceConnectionTest {

  @Test
  public void connectionSurvivesGarbageCollection() throws Exception {
    final Vertx vertx = Vertx.vertx();
    try {
      final CompletableFuture<Void> helloReceived = new CompletableFuture<>();
      final CompletableFuture<Void> serverSideClosed = new CompletableFuture<>();
      final HttpServer server =
          vertx
              .createHttpServer()
              .webSocketHandler(
                  ws -> {
                    ws.closeHandler(__ -> serverSideClosed.complete(null));
                    ws.textMessageHandler(
                        msg -> {
                          if (msg.contains("\"hello\"")) {
                            ws.writeTextMessage("{\"emit\":[\"ready\"]}");
                            helloReceived.complete(null);
                          }
                        });
                  })
              .listen(0, "127.0.0.1")
              .await();

      final EthScheduler ethScheduler = mock(EthScheduler.class);
      final EthContext ethContext = mock(EthContext.class);
      when(ethContext.getScheduler()).thenReturn(ethScheduler);
      final EthProtocolManager ethProtocolManager = mock(EthProtocolManager.class);
      when(ethProtocolManager.ethContext()).thenReturn(ethContext);
      when(ethProtocolManager.getSupportedCapabilities())
          .thenReturn(List.of(Capability.create("eth", 68)));
      final GenesisConfigOptions genesisConfigOptions = mock(GenesisConfigOptions.class);
      when(genesisConfigOptions.getChainId()).thenReturn(Optional.of(BigInteger.ONE));
      final P2PNetwork p2PNetwork = mock(P2PNetwork.class);
      when(p2PNetwork.getLocalEnode())
          .thenReturn(
              Optional.of(
                  EnodeURLImpl.builder()
                      .nodeId(
                          "50203c6bfca6874370e71aecc8958529fd723feb05013dc1abca8fc1fff845c5259faba05852e9dfe5ce172a7d6e7c2a3a5eaa8b541c8af15ea5518bbff5f2fa")
                      .useDefaultPorts()
                      .ipAddress("127.0.0.1")
                      .listeningPort(30303)
                      .build()));

      final EthStatsService ethStatsService =
          new EthStatsService(
              ImmutableEthStatsConnectOptions.builder()
                  .scheme("ws")
                  .nodeName("besu-node")
                  .secret("secret")
                  .host("127.0.0.1")
                  .port(server.actualPort())
                  .contact("contact@test.net")
                  .ethStatsReportInterval(5)
                  .build(),
              mock(BlockchainQueries.class),
              ethProtocolManager,
              mock(TransactionPool.class),
              mock(MiningCoordinator.class),
              mock(SyncState.class),
              vertx,
              "clientVersion",
              genesisConfigOptions,
              p2PNetwork);

      ethStatsService.start();
      assertThat(helloReceived)
          .as("hello received before the connection closed")
          .succeedsWithin(Duration.ofSeconds(10));

      // Vert.x closes WebSocket clients that become unreachable, so a GC must not drop the
      // connection while the service still uses it.
      for (int i = 0; i < 10 && !serverSideClosed.isDone(); i++) {
        System.gc();
        Thread.sleep(200);
      }

      assertThat(serverSideClosed).isNotDone();
      verify(ethScheduler, never()).scheduleFutureTask(any(Runnable.class), any(Duration.class));

      ethStatsService.stop();
      serverSideClosed.get(10, TimeUnit.SECONDS);
    } finally {
      vertx.close().await();
    }
  }
}
