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
package org.hyperledger.besu.controller;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.JsonBftConfigOptions;
import org.hyperledger.besu.config.TransitionsConfigOptions;
import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.MutableBftConfigOptions;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IbftBesuControllerBuilderTest extends AbstractBftBesuControllerBuilderTest {

  @Override
  public void setupBftGenesisConfig() throws JsonProcessingException {

    // Ibft prepForBuild setup
    lenient().when(genesisConfigOptions.isIbft2()).thenReturn(true);
    lenient().when(genesisConfigOptions.getConsensusEngine()).thenReturn("ibft2");
    lenient()
        .when(genesisConfigOptions.getBftConfigOptions())
        .thenReturn(new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT));

    final var jsonTransitions =
        (ObjectNode)
            objectMapper.readTree(
                """
                                {"ibft2": [
                                  {
                                            "block": 2,
                                            "blockperiodseconds": 2
                                  }
                                ]}
                                """);

    lenient()
        .when(genesisConfigOptions.getTransitions())
        .thenReturn(new TransitionsConfigOptions(jsonTransitions));

    when(genesisConfig.getExtraData())
        .thenReturn(
            "0xf83ea00000000000000000000000000000000000000000000000000000000000000000d594c2ab482b506de561668e07f04547232a72897daf808400000000c0");
  }

  @Override
  protected BesuControllerBuilder createBftControllerBuilder() {
    return new IbftBesuControllerBuilder();
  }

  @Override
  protected BlockHeaderFunctions getBlockHeaderFunctions() {
    return BftBlockHeaderFunctions.forOnchainBlock(new IbftExtraDataCodec());
  }
}
