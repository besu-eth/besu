/*
 * Copyright 2020 ConsenSys AG.
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
package org.hyperledger.besu.tests.acceptance.bft;

import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.provider.Arguments;

@Disabled("This is not a test class, it offers BFT parameterization only.")
public abstract class ParameterizedBftTestBase extends AcceptanceTestBase {
  protected String bftType;
  protected BftAcceptanceTestParameterization nodeFactory;

  public static Stream<Arguments> factoryFunctions() {
    final String consensusType = System.getProperty("acctests.consensusType");
    if (consensusType != null && !consensusType.isEmpty() && !consensusType.equals("all")) {
      return BftAcceptanceTestParameterization.getFactories()
          .filter(args -> consensusType.equals(args.get()[0]));
    }
    return BftAcceptanceTestParameterization.getFactories();
  }

  protected static void updateGenesisConfigToLondon(
      final BesuNode minerNode, final boolean zeroBaseFeeEnabled) {
    final Optional<String> genesisConfig =
        minerNode.getGenesisConfigProvider().create(List.of(minerNode));
    final ObjectNode genesisConfigNode = JsonUtil.objectNodeFromString(genesisConfig.orElseThrow());
    final ObjectNode config = (ObjectNode) genesisConfigNode.get("config");
    config.remove("berlinBlock");
    config.put("londonBlock", 0);
    config.put("zeroBaseFee", zeroBaseFeeEnabled);
    minerNode.setGenesisConfig(genesisConfigNode.toString());
  }

  static void updateGenesisConfigToShanghai(
      final BesuNode minerNode, final boolean zeroBaseFeeEnabled) {
    final Optional<String> genesisConfig =
        minerNode.getGenesisConfigProvider().create(List.of(minerNode));
    final ObjectNode genesisConfigNode = JsonUtil.objectNodeFromString(genesisConfig.orElseThrow());
    final ObjectNode config = (ObjectNode) genesisConfigNode.get("config");
    config.remove("berlinBlock");
    config.put("shanghaiTime", 100);
    config.put("zeroBaseFee", zeroBaseFeeEnabled);
    minerNode.setGenesisConfig(genesisConfigNode.toString());
  }

  protected void setUp(final String bftType, final BftAcceptanceTestParameterization input) {
    this.bftType = bftType;
    this.nodeFactory = input;
  }
}
