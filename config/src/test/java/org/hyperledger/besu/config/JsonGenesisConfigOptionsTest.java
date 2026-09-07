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
package org.hyperledger.besu.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

public class JsonGenesisConfigOptionsTest {

  private ObjectNode loadCompleteDataSet() {
    try {
      final String configText =
          Resources.toString(
              Resources.getResource("valid_config_with_custom_forks.json"), StandardCharsets.UTF_8);
      return JsonUtil.objectNodeFromString(configText);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to load resource", e);
    }
  }

  private ObjectNode loadConfigWithQbftTransitions() {
    final ObjectNode configNode = loadCompleteDataSet();
    final ObjectNode transitionsNode = JsonUtil.getObjectNode(configNode, "transitions").get();
    final JsonNode transitions = transitionsNode.get("ibft2");
    transitionsNode.remove("ibft2");
    transitionsNode.set("qbft", transitions);
    return configNode;
  }

  private ObjectNode loadConfigWithNoTransitions() {
    final ObjectNode configNode = loadCompleteDataSet();
    configNode.remove("transitions");
    return configNode;
  }

  private ObjectNode loadConfigWithNoIbft2Forks() {
    final ObjectNode configNode = loadCompleteDataSet();
    final ObjectNode transitionsNode = JsonUtil.getObjectNode(configNode, "transitions").get();
    transitionsNode.remove("ibft2");

    return configNode;
  }

  private ObjectNode loadConfigWithAnIbft2ForkWithMissingValidators() {
    final ObjectNode configNode = loadCompleteDataSet();
    final ObjectNode transitionsNode = JsonUtil.getObjectNode(configNode, "transitions").get();
    final ArrayNode ibftNode = JsonUtil.getArrayNode(transitionsNode, "ibft2").get();
    ((ObjectNode) ibftNode.get(0)).remove("validators");

    return configNode;
  }

  @Test
  public void additionalForkActivationsAreMergedIntoTheForkSchedule() {
    final ObjectNode config =
        JsonUtil.objectNodeFromString("{\"homesteadblock\":10,\"shanghaitime\":1000}");

    // no additional activations: just the genesis-declared forks
    assertThat(JsonGenesisConfigOptions.fromJsonObject(config).getForkBlockNumbers())
        .containsExactly(10L);
    assertThat(JsonGenesisConfigOptions.fromJsonObject(config).getForkBlockTimestamps())
        .containsExactly(1000L);

    // additional activations are unioned in, sorted and de-duplicated; genesis forks remain
    final GenesisConfigOptions merged =
        JsonGenesisConfigOptions.fromJsonObject(
            config, new ForkIdActivations(List.of(22L, 11L, 10L), List.of(2000L)));
    assertThat(merged.getForkIdBlockNumbers()).containsExactly(10L, 11L, 22L);
    assertThat(merged.getForkIdBlockTimestamps()).containsExactly(1000L, 2000L);

    // the forks the config itself declares are what schedule construction reads, and additional
    // activations must not appear there
    assertThat(merged.getForkBlockNumbers()).containsExactly(10L);
    assertThat(merged.getForkBlockTimestamps()).containsExactly(1000L);
  }

  @Test
  public void equalsAndHashCodeAccountForAdditionalForkActivations() {
    final ObjectNode config =
        JsonUtil.objectNodeFromString("{\"homesteadblock\":10,\"shanghaitime\":1000}");

    final GenesisConfigOptions noExtras = JsonGenesisConfigOptions.fromJsonObject(config);
    final GenesisConfigOptions withBlocks =
        JsonGenesisConfigOptions.fromJsonObject(
            config, new ForkIdActivations(List.of(11L), List.of()));
    final GenesisConfigOptions withTimestamps =
        JsonGenesisConfigOptions.fromJsonObject(
            config, new ForkIdActivations(List.of(), List.of(2000L)));
    final GenesisConfigOptions sameAsWithBlocks =
        JsonGenesisConfigOptions.fromJsonObject(
            config, new ForkIdActivations(List.of(11L), List.of()));

    // the additional activations are part of the object's identity: differing only by them
    // must not compare equal, otherwise a cache keyed on the options could serve a stale fork ID
    assertThat(withBlocks).isNotEqualTo(noExtras);
    assertThat(withTimestamps).isNotEqualTo(noExtras);
    assertThat(withBlocks).isNotEqualTo(withTimestamps);

    // same config and same activations remain equal, with a consistent hashCode
    assertThat(withBlocks).isEqualTo(sameAsWithBlocks);
    assertThat(withBlocks).hasSameHashCodeAs(sameAsWithBlocks);
  }

  @Test
  public void additionalForkActivationsHaveCanonicalValueSemantics() {
    final ForkIdActivations canonical =
        new ForkIdActivations(List.of(10L, 11L), List.of(1000L, 2000L));
    final ForkIdActivations reorderedWithDuplicates =
        new ForkIdActivations(List.of(11L, 10L, 11L), List.of(2000L, 1000L, 2000L));

    assertThat(reorderedWithDuplicates).isEqualTo(canonical);
    assertThat(reorderedWithDuplicates.blockNumbers()).containsExactly(10L, 11L);
    assertThat(reorderedWithDuplicates.timestamps()).containsExactly(1000L, 2000L);
  }

  @Test
  public void additionalForkActivationsRejectNegativeValues() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ForkIdActivations(List.of(-1L), List.of()))
        .withMessageContaining("negative");
  }

  @Test
  public void repeatedAdditionalForkActivationsAreMerged() {
    final GenesisConfigOptions options =
        GenesisConfig.fromConfig("{\"config\":{}}")
            .withAdditionalForkIdActivations(new ForkIdActivations(List.of(10L), List.of(1000L)))
            .withAdditionalForkIdActivations(new ForkIdActivations(List.of(11L), List.of(2000L)))
            .getConfigOptions();

    assertThat(options.getForkIdBlockNumbers()).containsExactly(10L, 11L);
    assertThat(options.getForkIdBlockTimestamps()).containsExactly(1000L, 2000L);
  }

  @Test
  public void ibftTransitionsDecodesCorrectlyFromFile() {
    final ObjectNode configNode = loadCompleteDataSet();

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getTransitions()).isNotNull();
    assertThat(configOptions.getTransitions().getIbftForks().size()).isEqualTo(2);
    assertThat(configOptions.getTransitions().getIbftForks().get(0).getForkBlock()).isEqualTo(20);
    assertThat(configOptions.getTransitions().getIbftForks().get(0).getValidators()).isNotEmpty();
    assertThat(configOptions.getTransitions().getIbftForks().get(0).getValidators().get())
        .containsExactly(
            "0x1234567890123456789012345678901234567890",
            "0x9876543210987654321098765432109876543210");

    assertThat(configOptions.getTransitions().getIbftForks().get(1).getForkBlock()).isEqualTo(25);
    assertThat(configOptions.getTransitions().getIbftForks().get(1).getValidators()).isNotEmpty();
    assertThat(configOptions.getTransitions().getIbftForks().get(1).getValidators().get())
        .containsExactly("0x1234567890123456789012345678901234567890");
  }

  @Test
  public void qbftTransitionsDecodesCorrectlyFromFile() {
    final ObjectNode configNode = loadConfigWithQbftTransitions();

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getTransitions()).isNotNull();
    assertThat(configOptions.getTransitions().getQbftForks().size()).isEqualTo(2);
    assertThat(configOptions.getTransitions().getQbftForks().get(0).getForkBlock()).isEqualTo(20);
    assertThat(configOptions.getTransitions().getQbftForks().get(0).getValidators()).isNotEmpty();
    assertThat(configOptions.getTransitions().getQbftForks().get(0).getValidators().get())
        .containsExactly(
            "0x1234567890123456789012345678901234567890",
            "0x9876543210987654321098765432109876543210");

    assertThat(configOptions.getTransitions().getQbftForks().get(1).getForkBlock()).isEqualTo(25);
    assertThat(configOptions.getTransitions().getQbftForks().get(1).getValidators()).isNotEmpty();
    assertThat(configOptions.getTransitions().getQbftForks().get(1).getValidators().get())
        .containsExactly("0x1234567890123456789012345678901234567890");
  }

  @Test
  public void configWithMissingTransitionsIsValid() {
    final ObjectNode configNode = loadConfigWithNoTransitions();

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getTransitions()).isNotNull();
    assertThat(configOptions.getTransitions().getIbftForks().size()).isZero();
    assertThat(configOptions.getTransitions().getQbftForks().size()).isZero();
  }

  @Test
  public void configWithNoIbft2ForksIsValid() {
    final ObjectNode configNode = loadConfigWithNoIbft2Forks();

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getTransitions()).isNotNull();
    assertThat(configOptions.getTransitions().getIbftForks().size()).isZero();
  }

  @Test
  public void configWithAnIbftWithNoValidatorsListedIsValid() {
    final ObjectNode configNode = loadConfigWithAnIbft2ForkWithMissingValidators();

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getTransitions().getIbftForks().get(0).getValidators().isPresent())
        .isFalse();
    assertThat(configOptions.getTransitions().getIbftForks().get(1).getValidators().get().size())
        .isEqualTo(1);
  }

  @Test
  public void configWithValidIbftBlockRewardIsParsable() {
    final ObjectNode configNode = loadConfigWithNoTransitions();

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);
    assertThat(
            configOptions
                .getBftConfigOptions()
                .getMiningBeneficiary()
                .map(a -> a.getBytes().toHexString()))
        .contains("0x1234567890123456789012345678901234567890");
    assertThat(configOptions.getBftConfigOptions().getBlockRewardWei()).isEqualTo(21);
  }

  @Test
  public void ibftConfigWithoutMiningBeneficiaryDefaultsToEmpty() {
    final ObjectNode configNode = loadConfigWithNoTransitions();
    final ObjectNode ibftNode = (ObjectNode) configNode.get("ibft2");
    ibftNode.remove("miningbeneficiary");

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getBftConfigOptions().getMiningBeneficiary()).isEmpty();
  }

  @Test
  public void ibftConfigWithoutBlockRewardsDefaultsToZero() {
    final ObjectNode configNode = loadConfigWithNoTransitions();
    final ObjectNode ibftNode = (ObjectNode) configNode.get("ibft2");
    ibftNode.remove("blockreward");

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getBftConfigOptions().getBlockRewardWei()).isEqualTo(0);
  }

  @Test
  public void ibftBlockRewardAsDecimalNumberCorrectlyDecodes() {
    final ObjectNode configNode = loadConfigWithNoTransitions();
    final ObjectNode ibftNode = (ObjectNode) configNode.get("ibft2");
    ibftNode.put("blockreward", "12");

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getBftConfigOptions().getBlockRewardWei()).isEqualTo(12);
  }

  @Test
  public void configWithoutEcCurveReturnsEmptyOptional() {
    final ObjectNode configNode = loadCompleteDataSet();

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getEcCurve().isEmpty()).isTrue();
  }

  @Test
  public void configWithEcCurveIsCorrectlySet() {
    final ObjectNode configNode = loadCompleteDataSet();
    configNode.put("eccurve", "secp256k1");

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.getEcCurve().isPresent()).isTrue();
    assertThat(configOptions.getEcCurve().get()).isEqualTo("secp256k1");
  }

  @Test
  public void configWithMigrationFromIbft2ToQbft() {
    final ObjectNode configNode = loadConfigWithMigrationFromIbft2ToQbft();

    final JsonGenesisConfigOptions configOptions =
        JsonGenesisConfigOptions.fromJsonObject(configNode);

    assertThat(configOptions.isIbft2()).isTrue();
    assertThat(configOptions.isQbft()).isTrue();
    assertThat(configOptions.isConsensusMigration()).isTrue();
  }

  private ObjectNode loadConfigWithMigrationFromIbft2ToQbft() {
    try {
      final String configText =
          Resources.toString(
              Resources.getResource("valid_config_with_migration_to_qbft.json"),
              StandardCharsets.UTF_8);
      return JsonUtil.objectNodeFromString(configText);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to load resource", e);
    }
  }
}
