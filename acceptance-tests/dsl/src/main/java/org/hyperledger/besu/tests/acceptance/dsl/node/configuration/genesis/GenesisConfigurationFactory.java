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
package org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis;

import static java.util.stream.Collectors.toList;

import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.tests.acceptance.dsl.node.RunnableNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

public class GenesisConfigurationFactory {

  private GenesisConfigurationFactory() {
    throw new IllegalStateException("Utility class");
  }

  public static Optional<String> createIbft2GenesisConfig(
      final Collection<? extends RunnableNode> validators) {
    return createIbft2GenesisConfig(validators, "/ibft/ibft.json");
  }

  public static Optional<String> createIbft2GenesisConfig(
      final Collection<? extends RunnableNode> validators, final String genesisFile) {
    final String template = readGenesisFile(genesisFile);
    return updateGenesisExtraData(
        validators, template, IbftExtraDataCodec::createGenesisExtraDataString);
  }

  public static Optional<String> createIbft2GenesisConfigFilterBootnode(
      final Collection<? extends RunnableNode> validators, final String genesisFile) {
    final String template = readGenesisFile(genesisFile);
    final List<? extends RunnableNode> filteredList =
        validators.stream()
            .filter(node -> !node.getConfiguration().isBootnodeEligible())
            .collect(toList());
    return updateGenesisExtraData(
        filteredList, template, IbftExtraDataCodec::createGenesisExtraDataString);
  }

  public static Optional<String> createQbftGenesisConfig(
      final Collection<? extends RunnableNode> validators) {
    final String template = readGenesisFile("/qbft/qbft.json");
    return updateGenesisExtraData(
        validators, template, QbftExtraDataCodec::createGenesisExtraDataString);
  }

  public static Optional<String> createQbft256r1GenesisConfig(
      final Collection<? extends RunnableNode> validators) {
    final String template = readGenesisFile("/crypto/secp256r1.json");
    return updateGenesisExtraData(
        validators, template, QbftExtraDataCodec::createGenesisExtraDataString);
  }

  public static Optional<String> createQbftLondonGenesisConfig(
      final Collection<? extends RunnableNode> validators) {
    final String template = readGenesisFile("/qbft/qbft-london.json");
    return updateGenesisExtraData(
        validators, template, QbftExtraDataCodec::createGenesisExtraDataString);
  }

  public static Optional<String> createQbftMigrationGenesisConfig(
      final Collection<? extends RunnableNode> validators) {
    final String template = readGenesisFile("/qbft/migration-ibft1/qbft-migration.json");
    return updateGenesisExtraData(
        validators, template, QbftExtraDataCodec::createGenesisExtraDataString);
  }

  @SuppressWarnings("unchecked")
  public static Optional<String> createQbftValidatorContractGenesisConfig(
      final Collection<? extends RunnableNode> validators) throws UncheckedIOException {
    final String template = readGenesisFile("/qbft/qbft-emptyextradata.json");
    final String contractAddress = "0x0000000000000000000000000000000000008888";

    try {
      // convert genesis json to Map for modification
      final ObjectMapper objectMapper = new ObjectMapper();
      final Map<String, Object> genesisMap =
          objectMapper.readValue(template, new TypeReference<>() {});

      // update config/qbft to add contract address
      final Map<String, Object> configMap = (Map<String, Object>) genesisMap.get("config");
      final Map<String, Object> qbftMap = (Map<String, Object>) configMap.get("qbft");
      qbftMap.put("validatorcontractaddress", contractAddress);

      // update alloc to add contract code and storage
      final Map<String, Object> allocMap = (Map<String, Object>) genesisMap.get("alloc");
      final Map<String, Object> contractConfig =
          new QbftValidatorContractConfigFactory().buildContractConfig(validators);
      allocMap.put(contractAddress, contractConfig);

      // regenerate genesis json again
      final String genesisJson =
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(genesisMap);
      return Optional.of(genesisJson);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Optional<String> updateGenesisExtraData(
      final Collection<? extends RunnableNode> validators,
      final String genesisTemplate,
      final Function<List<Address>, String> extraDataCreator) {
    final List<Address> addresses =
        validators.stream().map(RunnableNode::getAddress).collect(toList());
    final String extraDataString = extraDataCreator.apply(addresses);
    final String genesis = genesisTemplate.replaceAll("%extraData%", extraDataString);
    return Optional.of(genesis);
  }

  public static Optional<String> createFromResource(final String resourceName) {
    return Optional.of(readGenesisFile(resourceName));
  }

  @SuppressWarnings("UnstableApiUsage")
  public static String readGenesisFile(final String filepath) {
    try {
      final URI uri = GenesisConfigurationFactory.class.getResource(filepath).toURI();
      return Resources.toString(uri.toURL(), Charset.defaultCharset());
    } catch (final URISyntaxException | IOException e) {
      throw new IllegalStateException("Unable to get test genesis config " + filepath);
    }
  }
}
