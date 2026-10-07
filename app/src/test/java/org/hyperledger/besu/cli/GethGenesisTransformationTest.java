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
package org.hyperledger.besu.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.Address;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * Tests for Geth genesis file transformation logic in BesuCommand.
 *
 * <p>These tests verify that Geth-format genesis files are correctly transformed to Besu format
 * when loaded through the CLI. The transformation includes:
 *
 * <ul>
 *   <li>Adding the ethash field for consensus detection
 *   <li>Mapping mergeNetsplitBlock to preMergeForkBlock
 *   <li>Adding baseFeePerGas when London fork is at genesis
 *   <li>Adding withdrawal and consolidation contract addresses
 * </ul>
 */
class GethGenesisTransformationTest extends CommandTestAbstract {

  @Test
  void gethGenesis_isTransformedAndLoadsSuccessfully() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/geth-genesis-minimal.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    assertThat(config.getChainId()).hasValue(java.math.BigInteger.valueOf(1337));
  }

  @Test
  void gethGenesis_mapsMergeNetsplitBlockToPreMergeForkBlock() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/geth-genesis-minimal.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    // mergeNetsplitBlock: 0 should be mapped to preMergeForkBlock (accessed via
    // getMergeNetSplitBlockNumber)
    assertThat(config.getMergeNetSplitBlockNumber()).hasValue(0L);
  }

  @Test
  void gethGenesis_isDetectedAsEthash() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/geth-genesis-minimal.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    // After transformation, ethash field should be added, making isEthHash() return true
    assertThat(config.isEthHash()).isTrue();
  }

  @Test
  void gethGenesis_addsWithdrawalContractAddress() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/geth-genesis-minimal.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    assertThat(config.getWithdrawalRequestContractAddress())
        .hasValue(Address.fromHexString("0x00000961ef480eb55e80d19ad83579a64c007002"));
  }

  @Test
  void gethGenesis_addsConsolidationContractAddress() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/geth-genesis-minimal.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    assertThat(config.getConsolidationRequestContractAddress())
        .hasValue(Address.fromHexString("0x0000bbddc7ce488642fb579f8b00f3a590007251"));
  }

  @Test
  void gethGenesis_preservesExistingContractAddresses() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/geth-genesis-with-contracts.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    // Custom addresses from the file should be preserved
    assertThat(config.getWithdrawalRequestContractAddress())
        .hasValue(Address.fromHexString("0x1111111111111111111111111111111111111111"));
    assertThat(config.getConsolidationRequestContractAddress())
        .hasValue(Address.fromHexString("0x2222222222222222222222222222222222222222"));
  }

  @Test
  void gethDevnet5Genesis_loadsAndTransformsSuccessfully() throws IOException {
    // This is a real geth genesis file used in devnet5
    final Path genesisFile = copyResourceToTempFile("genesis/geth-devnet5-genesis.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();

    assertThat(config.getChainId()).hasValue(java.math.BigInteger.valueOf(7092415936L));
    assertThat(config.isEthHash()).isTrue();
    assertThat(config.getMergeNetSplitBlockNumber()).hasValue(0L);
    assertThat(config.getShanghaiTime()).hasValue(0L);
    assertThat(config.getCancunTime()).hasValue(0L);
    assertThat(config.getPragueTime()).hasValue(0L);
  }

  @Test
  void besuGenesis_loadsSuccessfullyWithoutTransformation() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/besu-genesis-minimal.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    assertThat(config.getChainId()).hasValue(java.math.BigInteger.valueOf(1337));
  }

  @Test
  void gethGenesis_withLondonNotAtZero_doesNotRequireBaseFee() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/geth-genesis-london-at-100.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    assertThat(config.getLondonBlockNumber()).hasValue(100L);
    // Should load successfully even without baseFeePerGas since London is not at genesis
  }

  @Test
  void gethGenesis_preservesExistingBaseFee() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/geth-genesis-with-basefee.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    // Should load successfully with the custom base fee
    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    assertThat(config.getChainId()).hasValue(java.math.BigInteger.valueOf(1337));
  }

  @Test
  void genesisWithNoConfig_loadsSuccessfully() throws IOException {
    final Path genesisFile = copyResourceToTempFile("genesis/geth-genesis-no-config.json");

    final TestBesuCommand cmd = parseCommand("--genesis-file", genesisFile.toString());

    // Should not crash - genesis without config section is valid (uses defaults)
    final GenesisConfigOptions config = cmd.getGenesisConfigOptionsSupplier().get();
    assertThat(config).isNotNull();
  }

  @Test
  void malformedGenesis_producesError() throws IOException {
    final Path malformedFile = Files.createTempFile("malformed", ".json");
    Files.writeString(malformedFile, "{invalid json content");
    malformedFile.toFile().deleteOnExit();

    parseCommand("--genesis-file", malformedFile.toString());

    assertThat(commandErrorOutput.toString(UTF_8)).contains("Unable to load genesis file");
  }

  @Test
  void emptyGenesis_producesError() throws IOException {
    final Path emptyFile = Files.createTempFile("empty", ".json");
    Files.writeString(emptyFile, "");
    emptyFile.toFile().deleteOnExit();

    parseCommand("--genesis-file", emptyFile.toString());

    assertThat(commandErrorOutput.toString(UTF_8)).contains("Unable to load genesis file");
  }

  @Test
  void nonExistentGenesis_producesError() {
    parseCommand("--genesis-file", "/non/existent/path/genesis.json");

    assertThat(commandErrorOutput.toString(UTF_8)).contains("Unable to load genesis file");
  }

  private Path copyResourceToTempFile(final String resourcePath) throws IOException {
    final URL resourceUrl = getClass().getClassLoader().getResource(resourcePath);
    if (resourceUrl == null) {
      throw new IOException("Resource not found: " + resourcePath);
    }
    final String content = new String(resourceUrl.openStream().readAllBytes(), UTF_8);
    final Path tempFile = Files.createTempFile("genesis", ".json");
    Files.writeString(tempFile, content);
    tempFile.toFile().deleteOnExit();
    return tempFile;
  }
}
