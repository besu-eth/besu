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
package org.hyperledger.besu.chainimport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.components.BesuComponent;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.testutil.BlockTestUtil;
import org.hyperledger.besu.testutil.TestClock;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.google.common.io.Resources;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

/** Tests for {@link RlpBlockImporter}. */
@ExtendWith(MockitoExtension.class)
public final class RlpBlockImporterTest {

  @TempDir Path dataDir;

  private final RlpBlockImporter rlpBlockImporter = new RlpBlockImporter();

  @Test
  public void blockImport() throws IOException {
    final Path source = dataDir.resolve("1000.blocks");
    BlockTestUtil.write1000Blocks(source);
    final BesuController targetController =
        new BesuController.Builder()
            .fromEthNetworkConfig(
                EthNetworkConfig.getNetworkConfig(NetworkDefinition.MAINNET), SyncMode.FULL)
            .synchronizerConfiguration(SynchronizerConfiguration.builder().build())
            .ethProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .storageProvider(new InMemoryKeyValueStorageProvider())
            .networkId(BigInteger.ONE)
            .miningParameters(MiningConfiguration.newDefault())
            .nodeKey(NodeKeyUtils.generate())
            .metricsSystem(new NoOpMetricsSystem())
            .dataDirectory(dataDir)
            .clock(TestClock.fixed())
            .transactionPoolConfiguration(TransactionPoolConfiguration.DEFAULT)
            .evmConfiguration(EvmConfiguration.DEFAULT)
            .networkConfiguration(NetworkingConfiguration.DEFAULT)
            .besuComponent(mock(BesuComponent.class))
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .build();
    final RlpBlockImporter.ImportResult result =
        rlpBlockImporter.importBlockchain(source, targetController, false);
    // Don't count the Genesis block
    assertThat(result.count).isEqualTo(999);
    assertThat(result.td).isEqualTo(UInt256.valueOf(21991996248790L));
  }

  @Test
  public void ibftImport() throws IOException {
    final Path source = dataDir.resolve("ibft.blocks");
    final String config =
        Resources.toString(this.getClass().getResource("/ibft-genesis-2.json"), UTF_8);

    try {
      Files.write(
          source,
          Resources.toByteArray(this.getClass().getResource("/ibft.blocks")),
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING);
    } catch (final IOException ex) {
      throw new IllegalStateException(ex);
    }

    final BesuController controller =
        new BesuController.Builder()
            .fromGenesisFile(GenesisConfig.fromConfig(config), SyncMode.FULL)
            .synchronizerConfiguration(SynchronizerConfiguration.builder().build())
            .ethProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .storageProvider(new InMemoryKeyValueStorageProvider())
            .networkId(BigInteger.valueOf(1337))
            .miningParameters(MiningConfiguration.newDefault())
            .nodeKey(NodeKeyUtils.generate())
            .metricsSystem(new NoOpMetricsSystem())
            .dataDirectory(dataDir)
            .clock(TestClock.fixed())
            .transactionPoolConfiguration(TransactionPoolConfiguration.DEFAULT)
            .evmConfiguration(EvmConfiguration.DEFAULT)
            .networkConfiguration(NetworkingConfiguration.DEFAULT)
            .besuComponent(mock(BesuComponent.class))
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .build();
    final RlpBlockImporter.ImportResult result =
        rlpBlockImporter.importBlockchain(source, controller, true);

    // Don't count the Genesis block
    assertThat(result.count).isEqualTo(100);
  }

  /**
   * After the merge, a valid block imported on a side chain becomes the chain head, as it would
   * after the consensus client's fork choice update. The blocks are the Cancun blocks of
   * ethereum/tests BlockchainTests/InvalidBlocks/bcMultiChainTest/UncleFromSideChain.json: chain A
   * has blocks 1 to 3, side chain B blocks 1 to 4, leaving out the invalid block with uncles.
   */
  @Test
  public void posSideChainImportMovesChainHead() throws IOException {
    final String config =
        Resources.toString(this.getClass().getResource("/pos-side-chain-genesis.json"), UTF_8);
    final BesuController controller =
        new BesuController.Builder()
            .fromGenesisFile(GenesisConfig.fromConfig(config), SyncMode.FULL)
            .synchronizerConfiguration(SynchronizerConfiguration.builder().build())
            .ethProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .storageProvider(new InMemoryKeyValueStorageProvider())
            .networkId(BigInteger.ONE)
            .miningParameters(MiningConfiguration.newDefault())
            .nodeKey(NodeKeyUtils.generate())
            .metricsSystem(new NoOpMetricsSystem())
            .dataDirectory(dataDir)
            .clock(TestClock.fixed())
            .transactionPoolConfiguration(TransactionPoolConfiguration.DEFAULT)
            .evmConfiguration(EvmConfiguration.DEFAULT)
            .networkConfiguration(NetworkingConfiguration.DEFAULT)
            .besuComponent(mock(BesuComponent.class))
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .build();
    final MutableBlockchain blockchain = controller.getProtocolContext().getBlockchain();
    assertThat(blockchain.getChainHeadHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xfc96de622c494ad156bddd8953449830246ebf75564ef37aaf9142db066497c0"));

    rlpBlockImporter.importBlockchain(resourceFile("pos-side-chain-a.blocks"), controller, true);
    assertThat(blockchain.getChainHeadHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x3339fd43c6afc5d4ff580df7fe698f6575560b6611421f9656b1e6f1c8b223be"));

    rlpBlockImporter.importBlockchain(resourceFile("pos-side-chain-b.blocks"), controller, true);
    final BlockHeader head = blockchain.getChainHeadHeader();
    assertThat(head.getHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xd73a4a15d2b9f759009538aafd443198d1e8cd0b2509556f596b7f3a4b345343"));
    assertThat(controller.getProtocolContext().getWorldStateArchive().getWorldState().rootHash())
        .isEqualTo(head.getStateRoot());
  }

  private Path resourceFile(final String name) throws IOException {
    final Path file = dataDir.resolve(name);
    Files.write(file, Resources.toByteArray(this.getClass().getResource("/" + name)));
    return file;
  }
}
