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
package org.hyperledger.besu.ethereum.eth.sync.fullsync.ereprepipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.SyncTerminationCondition;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.era1prepipeline.FileImportChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.tasks.exceptions.InvalidBlockException;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EreImportPrepipelineFactoryTest {
  @TempDir Path directory;
  private BlockchainSetupUtil chain;
  private EthProtocolManager protocolManager;
  private EthContext ethContext;
  private List<Block> blocks;
  private Path file;

  @BeforeEach
  void setUp() throws IOException {
    chain = BlockchainSetupUtil.forMainnet();
    protocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(chain.getProtocolSchedule())
            .setBlockchain(chain.getBlockchain())
            .setEthScheduler(new EthScheduler(1, 1, 1, 1, new NoOpMetricsSystem()))
            .setWorldStateArchive(chain.getWorldArchive())
            .setTransactionPool(chain.getTransactionPool())
            .setEthereumWireProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .build();
    ethContext = protocolManager.ethContext();
    blocks = new ArrayList<>();
    blocks.add(new Block(chain.getBlockchain().getBlockHeader(0).orElseThrow(), BlockBody.empty()));
    blocks.addAll(
        chain.getBlocks().stream()
            .filter(
                block -> block.getHeader().getNumber() > 0 && block.getHeader().getNumber() <= 16)
            .toList());
    assertThat(blocks).hasSize(17);
    file = EreTestFixtures.write(directory, blocks);
  }

  @AfterEach
  void tearDown() {
    if (protocolManager != null) {
      protocolManager.stop();
    }
  }

  @Test
  void importsRealMainnetBlockFixturesThroughFullValidation() throws Exception {
    downloader(directory.toUri()).start().get(30, TimeUnit.SECONDS);
    assertImportedToEnd();
  }

  @Test
  void resumesAtAnExistingChainHead() throws Exception {
    chain.importFirstBlocks(4);
    downloader(directory.toUri()).start().get(30, TimeUnit.SECONDS);
    assertImportedToEnd();
  }

  @Test
  void importsFromHttpDirectory() throws Exception {
    final HttpServer server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          final byte[] data =
              exchange.getRequestURI().getPath().equals("/")
                  ? ("<a href='" + file.getFileName() + "'>archive</a>")
                      .getBytes(StandardCharsets.UTF_8)
                  : Files.readAllBytes(file);
          exchange.sendResponseHeaders(200, data.length);
          try (var output = exchange.getResponseBody()) {
            output.write(data);
          }
        });
    server.start();
    try {
      downloader(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/"))
          .start()
          .get(30, TimeUnit.SECONDS);
      assertImportedToEnd();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void rejectsInvalidBlocksInsteadOfTrustingTheArchive() throws Exception {
    Files.delete(file);
    final Block invalid =
        new BlockDataGenerator(11244)
            .block(
                BlockOptions.create().setBlockNumber(1).setParentHash(blocks.getFirst().getHash()));
    EreTestFixtures.write(directory, List.of(blocks.getFirst(), invalid));
    assertThatThrownBy(() -> downloader(directory.toUri()).start().get(30, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(InvalidBlockException.class);
    assertThat(chain.getBlockchain().getChainHeadBlockNumber()).isZero();
  }

  @Test
  void rejectsHeaderWithOversizedExtraDataBeforeImport() throws Exception {
    final Block first = blocks.get(1);
    final Block invalid =
        new Block(
            BlockHeaderBuilder.fromHeader(first.getHeader())
                .extraData(Bytes.wrap(new byte[33]))
                .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
                .buildBlockHeader(),
            first.getBody());
    assertRejectedBeforeImport(invalid);
  }

  @Test
  void rejectsHeaderTooFarInTheFutureBeforeImport() throws Exception {
    final Block first = blocks.get(1);
    final Block invalid =
        new Block(
            BlockHeaderBuilder.fromHeader(first.getHeader())
                .timestamp(Instant.now().getEpochSecond() + 3600)
                .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
                .buildBlockHeader(),
            first.getBody());
    assertRejectedBeforeImport(invalid);
  }

  @Test
  void resumesAfterRepairingACorruptedArchive() throws Exception {
    final byte[] original = Files.readAllBytes(file);
    final byte[] corrupt = original.clone();
    final ByteBuffer records = ByteBuffer.wrap(corrupt).order(ByteOrder.LITTLE_ENDIAN);
    int body = 0;
    for (int position = 8; position < corrupt.length; ) {
      final int type = Short.toUnsignedInt(records.getShort(position));
      final int length = records.getInt(position + 2);
      if (type == 4 && body++ == 8) {
        corrupt[position + 8 + 14] ^= 1;
        break;
      }
      position += length + 8;
    }
    Files.write(file, corrupt);
    assertThatThrownBy(() -> downloader(directory.toUri()).start().get(30, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(IOException.class);
    assertThat(chain.getBlockchain().getChainHeadBlockNumber()).isEqualTo(7);
    Files.write(file, original);
    downloader(directory.toUri()).start().get(30, TimeUnit.SECONDS);
    assertImportedToEnd();
  }

  private FileImportChainDownloader downloader(final URI uri) {
    final var factory =
        new EreImportPrepipelineFactory(
            new NoOpMetricsSystem(),
            uri,
            2,
            chain.getProtocolSchedule(),
            chain.getProtocolContext(),
            ethContext,
            SyncTerminationCondition.never());
    return new FileImportChainDownloader(factory, chain.getBlockchain(), ethContext.getScheduler());
  }

  private void assertRejectedBeforeImport(final Block invalid) throws Exception {
    Files.delete(file);
    EreTestFixtures.write(directory, List.of(blocks.getFirst(), invalid));
    assertThatThrownBy(() -> downloader(directory.toUri()).start().get(30, TimeUnit.SECONDS))
        .hasRootCauseInstanceOf(InvalidBlockException.class);
    assertThat(chain.getBlockchain().getChainHeadBlockNumber()).isZero();
    assertThat(chain.getBlockchain().contains(invalid.getHash())).isFalse();
  }

  private void assertImportedToEnd() {
    assertThat(chain.getBlockchain().getChainHeadBlockNumber()).isEqualTo(16);
    assertThat(chain.getBlockchain().getChainHeadHeader().getHash())
        .isEqualTo(blocks.getLast().getHash());
    assertThat(chain.getBlockchain().getChainHeadHeader().getStateRoot())
        .isEqualTo(blocks.getLast().getHeader().getStateRoot());
  }
}
