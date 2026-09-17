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
package org.hyperledger.besu.cli.subcommands.operator;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.besu.cli.DefaultCommandValues;
import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.JsonGenesisConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import jakarta.validation.constraints.NotBlank;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(
    name = "generate-blockchain-config",
    description = "Generate node keypairs and genesis file with RLP encoded extra data.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
class GenerateBlockchainConfig implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(GenerateBlockchainConfig.class);

  @NotBlank
  @Option(
      required = true,
      names = "--config-file",
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "Configuration file.",
      arity = "1..1")
  private final File configurationFile = null;

  @NotBlank
  @Option(
      required = true,
      names = "--to",
      paramLabel = DefaultCommandValues.MANDATORY_DIRECTORY_FORMAT_HELP,
      description = "Directory to write output files to.",
      arity = "1..1")
  private final File outputDirectory = null;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @Option(
      names = "--genesis-file-name",
      paramLabel = DefaultCommandValues.MANDATORY_PATH_FORMAT_HELP,
      description = "Name of the genesis file. (default: ${DEFAULT-VALUE})",
      arity = "1..1")
  private String genesisFileName = "genesis.json";

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @Option(
      names = "--private-key-file-name",
      paramLabel = DefaultCommandValues.MANDATORY_PATH_FORMAT_HELP,
      description = "Name of the private key file. (default: ${DEFAULT-VALUE})",
      arity = "1..1")
  private String privateKeyFileName = "key.priv";

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @Option(
      names = "--public-key-file-name",
      paramLabel = DefaultCommandValues.MANDATORY_PATH_FORMAT_HELP,
      description = "Name of the public key file. (default: ${DEFAULT-VALUE})",
      arity = "1..1")
  private String publicKeyFileName = "key.pub";

  @ParentCommand
  private OperatorSubCommand parentCommand; // Picocli injects reference to parent command

  private ObjectNode operatorConfig;
  private ObjectNode genesisConfig;
  private ObjectNode blockchainConfig;
  private ObjectNode nodesConfig;
  private boolean generateNodesKeys;
  private final List<Address> addressesForGenesisExtraData = new ArrayList<>();
  private Path keysDirectory;

  @Override
  public void run() {
    checkPreconditions();
    generateBlockchainConfig();
  }

  private void checkPreconditions() {
    checkNotNull(parentCommand);
    checkNotNull(parentCommand.parentCommand);
    if (isAnyDuplicate(genesisFileName, publicKeyFileName, privateKeyFileName)) {
      throw new IllegalArgumentException("Output file paths must be unique.");
    }
  }

  /** Generates output directory with all required configuration files. */
  private void generateBlockchainConfig() {
    try {
      handleOutputDirectory();
      parseConfig();
      processEcCurve();
      if (generateNodesKeys) {
        generateNodesKeys();
      } else {
        importPublicKeysFromConfig();
      }
      processExtraData();
      writeGenesisFile(outputDirectory, genesisFileName, genesisConfig);
    } catch (final IOException e) {
      LOG.error("An error occurred while trying to generate network configuration.", e);
    }
  }

  /** Imports public keys from input configuration. */
  private void importPublicKeysFromConfig() {
    LOG.info("Importing public keys from configuration.");
    JsonUtil.getArrayNode(nodesConfig, "keys")
        .ifPresent(keys -> keys.forEach(this::importPublicKey));
  }

  /**
   * Imports a single public key.
   *
   * @param publicKeyJson The public key.
   */
  private void importPublicKey(final JsonNode publicKeyJson) {
    if (publicKeyJson.getNodeType() != JsonNodeType.STRING) {
      throw new IllegalArgumentException(
          "Invalid key json of type: " + publicKeyJson.getNodeType());
    }
    final String publicKeyText = publicKeyJson.asText();

    try {
      SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
      final SECPPublicKey publicKey =
          signatureAlgorithm.createPublicKey(Bytes.fromHexString(publicKeyText));

      if (!signatureAlgorithm.isValidPublicKey(publicKey)) {
        throw new IllegalArgumentException(
            publicKeyText
                + " is not a valid public key for elliptic curve "
                + signatureAlgorithm.getCurveName());
      }

      writeKeypair(publicKey, null);
      LOG.info("Public key imported from configuration.({})", publicKey.toString());
    } catch (final IOException e) {
      LOG.error("An error occurred while trying to import node public key.", e);
    }
  }

  /** Generates nodes keypairs. */
  private void generateNodesKeys() {
    final int nodesCount = JsonUtil.getInt(nodesConfig, "count", 0);
    LOG.info("Generating {} nodes keys.", nodesCount);
    IntStream.range(0, nodesCount).forEach(this::generateNodeKeypair);
  }

  /**
   * Generate a keypair for a node.
   *
   * @param node The number of the node.
   */
  private void generateNodeKeypair(final int node) {
    try {
      LOG.info("Generating keypair for node {}.", node);
      final KeyPair keyPair = SignatureAlgorithmFactory.getInstance().generateKeyPair();
      writeKeypair(keyPair.getPublicKey(), keyPair.getPrivateKey());

    } catch (final IOException e) {
      LOG.error("An error occurred while trying to generate node keypair.", e);
    }
  }

  /**
   * Writes public and private keys in separate files. Both are written in the same directory named
   * with the address derived from the public key.
   *
   * @param publicKey The public key.
   * @param privateKey The private key. No file is created if privateKey is NULL.
   * @throws IOException If the file cannot be written or accessed.
   */
  private void writeKeypair(final SECPPublicKey publicKey, final SECPPrivateKey privateKey)
      throws IOException {
    final Address nodeAddress = Util.publicKeyToAddress(publicKey);
    addressesForGenesisExtraData.add(nodeAddress);
    final Path nodeDirectoryPath = keysDirectory.resolve(nodeAddress.toString());
    Files.createDirectory(nodeDirectoryPath);
    createFileAndWrite(nodeDirectoryPath, publicKeyFileName, publicKey.toString());
    if (privateKey != null) {
      createFileAndWrite(nodeDirectoryPath, privateKeyFileName, privateKey.toString());
    }
  }

  /** Computes RLP encoded exta data from pre filled list of addresses. */
  private void processExtraData() {
    final ObjectNode configNode =
        JsonUtil.getObjectNode(genesisConfig, "config")
            .orElseThrow(
                () -> new IllegalArgumentException("Missing config section in config file"));

    final JsonGenesisConfigOptions genesisConfigOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);
    if (genesisConfigOptions.isIbft2()) {
      LOG.info("Generating IBFT extra data.");
      final String extraData =
          IbftExtraDataCodec.encodeFromAddresses(addressesForGenesisExtraData).toString();
      genesisConfig.put("extraData", extraData);
    } else if (genesisConfigOptions.isQbft()) {
      LOG.info("Generating QBFT extra data.");
      final String extraData =
          QbftExtraDataCodec.encodeFromAddresses(addressesForGenesisExtraData).toString();
      genesisConfig.put("extraData", extraData);
    }
  }

  private void createFileAndWrite(final Path directory, final String fileName, final String content)
      throws IOException {
    final Path filePath = directory.resolve(fileName);
    Files.write(filePath, content.getBytes(UTF_8), StandardOpenOption.CREATE_NEW);
  }

  /**
   * Parses the root configuration file and related sub elements.
   *
   * @throws IOException If the file cannot be read or accessed.
   */
  private void parseConfig() throws IOException {
    final String configString =
        Resources.toString(configurationFile.toPath().toUri().toURL(), UTF_8);
    final ObjectNode root = JsonUtil.objectNodeFromString(configString);
    operatorConfig = root;
    genesisConfig =
        JsonUtil.getObjectNode(operatorConfig, "genesis").orElse(JsonUtil.createEmptyObjectNode());
    blockchainConfig =
        JsonUtil.getObjectNode(operatorConfig, "blockchain")
            .orElse(JsonUtil.createEmptyObjectNode());
    nodesConfig =
        JsonUtil.getObjectNode(blockchainConfig, "nodes").orElse(JsonUtil.createEmptyObjectNode());
    generateNodesKeys = JsonUtil.getBoolean(nodesConfig, "generate", false);
  }

  /** Sets the selected signature algorithm instance in SignatureAlgorithmFactory. */
  private void processEcCurve() {
    GenesisConfigOptions options = GenesisConfig.fromConfig(genesisConfig).getConfigOptions();
    options
        .getEcCurve()
        .ifPresent(
            ecCurve -> {
              try {
                SignatureAlgorithmFactory.switchInstance(ecCurve);
              } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Invalid parameter for ecCurve in genesis config: " + e.getMessage());
              }
            });
  }

  /**
   * Checks if the output directory exists.
   *
   * @throws IOException If the cannot be accessed or created.
   */
  private void handleOutputDirectory() throws IOException {
    checkNotNull(outputDirectory);
    final Path outputDirectoryPath = outputDirectory.toPath();
    if (outputDirectory.exists()
        && outputDirectory.isDirectory()
        && outputDirectory.list() != null
        && outputDirectory.list().length > 0) {
      throw new IllegalArgumentException("Output directory already exists.");
    } else if (!outputDirectory.exists()) {
      Files.createDirectory(outputDirectoryPath);
    }
    keysDirectory = outputDirectoryPath.resolve("keys");
    Files.createDirectory(keysDirectory);
  }

  /**
   * Write the content of the genesis to the output file.
   *
   * @param directory The directory to write the file to.
   * @param fileName The name of the output file.
   * @param genesis The genesis content.
   * @throws IOException If the genesis file cannot be written or accessed.
   */
  private void writeGenesisFile(
      final File directory, final String fileName, final ObjectNode genesis) throws IOException {
    LOG.info("Writing genesis file.");
    Files.write(
        directory.toPath().resolve(fileName),
        JsonUtil.getJson(genesis).getBytes(UTF_8),
        StandardOpenOption.CREATE_NEW);
  }

  private static boolean isAnyDuplicate(final String... values) {
    final Set<String> set = new HashSet<>();
    for (final String value : values) {
      if (!set.add(value)) {
        return true;
      }
    }
    return false;
  }
}
