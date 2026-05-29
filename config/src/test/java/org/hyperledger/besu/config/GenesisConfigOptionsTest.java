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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class GenesisConfigOptionsTest {

  @Test
  void shouldUseEthHashWhenEthHashInConfig() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("ethash", emptyMap()));
    assertThat(config.isEthHash()).isTrue();
    assertThat(config.getConsensusEngine()).isEqualTo("ethash");
  }

  @Test
  void shouldNotUseEthHashIfEthHashNotPresent() {
    final GenesisConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.isEthHash()).isFalse();
  }

  @Test
  void shouldUseIbft2WhenIbft2InConfig() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("ibft2", emptyMap()));
    assertThat(config.isIbft2()).isTrue();
    assertThat(config.isPoa()).isTrue();
    assertThat(config.getConsensusEngine()).isEqualTo("ibft2");
  }

  @Test
  void shouldUseQbftWhenQbftInConfig() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("qbft", emptyMap()));
    assertThat(config.isQbft()).isTrue();
    assertThat(config.isPoa()).isTrue();
    assertThat(config.getConsensusEngine()).isEqualTo("qbft");
  }

  @Test
  void shouldUseCliqueWhenCliqueInConfig() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("clique", emptyMap()));
    assertThat(config.isClique()).isTrue();
    assertThat(config.isPoa()).isTrue();
    assertThat(config.getCliqueConfigOptions()).isNotSameAs(JsonCliqueConfigOptions.DEFAULT);
    assertThat(config.getConsensusEngine()).isEqualTo("clique");
  }

  @Test
  void shouldNotUseCliqueIfCliqueNotPresent() {
    final GenesisConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.isClique()).isFalse();
    assertThat(config.isPoa()).isFalse();
    assertThat(config.getCliqueConfigOptions()).isSameAs(JsonCliqueConfigOptions.DEFAULT);
  }

  @Test
  void shouldGetHomesteadBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("homesteadBlock", 1000));
    assertThat(config.getHomesteadBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetDaoForkBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("daoForkBlock", 1000));
    assertThat(config.getDaoForkBlock()).hasValue(1000);
  }

  @Test
  void shouldNotHaveDaoForkBlockWhenSetToZero() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("daoForkBlock", 0));
    assertThat(config.getDaoForkBlock()).isEmpty();
  }

  @Test
  void shouldGetTangerineWhistleBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("eip150Block", 1000));
    assertThat(config.getTangerineWhistleBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetSpuriousDragonBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("eip158Block", 1000));
    assertThat(config.getSpuriousDragonBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetByzantiumBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("byzantiumBlock", 1000));
    assertThat(config.getByzantiumBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetConstantinopleBlockNumber() {
    final GenesisConfigOptions config =
        fromConfigOptions(singletonMap("constantinopleBlock", 1000));
    assertThat(config.getConstantinopleBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetConstantinopleFixBlockNumber() {
    final GenesisConfigOptions config =
        fromConfigOptions(singletonMap("constantinopleFixBlock", 1000));
    assertThat(config.getPetersburgBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetPetersburgBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("petersburgBlock", 1000));
    assertThat(config.getPetersburgBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldFailWithBothPetersburgAndConstantinopleFixBlockNumber() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("constantinopleFixBlock", 1000);
    configMap.put("petersburgBlock", 1000);
    final GenesisConfigOptions config = fromConfigOptions(configMap);
    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(config::getPetersburgBlockNumber)
        .withMessage(
            "Genesis files cannot specify both petersburgBlock and constantinopleFixBlock.");
  }

  @Test
  void shouldGetIstanbulBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("istanbulBlock", 1000));
    assertThat(config.getIstanbulBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetMuirGlacierBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("muirGlacierBlock", 1000));
    assertThat(config.getMuirGlacierBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetBerlinBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("berlinBlock", 1000));
    assertThat(config.getBerlinBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetLondonBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("londonblock", 1000));
    assertThat(config.getLondonBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetArrowGlacierBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("arrowGlacierBlock", 1000));
    assertThat(config.getArrowGlacierBlockNumber()).hasValue(1000);
  }

  @Test
  void shouldGetGrayGlacierBlockNumber() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("grayGlacierBlock", 4242));
    assertThat(config.getGrayGlacierBlockNumber()).hasValue(4242);
  }

  @Test
  void shouldGetShanghaiTime() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("shanghaiTime", 1670470141));
    assertThat(config.getShanghaiTime()).hasValue(1670470141);
  }

  @Test
  void shouldGetCancunTime() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("cancunTime", 1670470142));
    assertThat(config.getCancunTime()).hasValue(1670470142);
  }

  @Test
  void shouldGetPragueTime() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("pragueTime", 1670470143));
    assertThat(config.getPragueTime()).hasValue(1670470143);
  }

  @Test
  void shouldGetOsakaTime() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("osakaTime", 1670470143));
    assertThat(config.getOsakaTime()).hasValue(1670470143);
  }

  @Test
  void shouldGetBpo1Time() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("bpo1Time", 1670470144));
    assertThat(config.getBpo1Time()).hasValue(1670470144);
  }

  @Test
  void shouldGetBpo2Time() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("bpo2Time", 1670470144));
    assertThat(config.getBpo2Time()).hasValue(1670470144);
  }

  @Test
  void shouldGetBpo3Time() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("bpo3Time", 1670470144));
    assertThat(config.getBpo3Time()).hasValue(1670470144);
  }

  @Test
  void shouldGetBpo4Time() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("bpo4Time", 1670470144));
    assertThat(config.getBpo4Time()).hasValue(1670470144);
  }

  @Test
  void shouldGetBpo5Time() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("bpo5Time", 1670470144));
    assertThat(config.getBpo5Time()).hasValue(1670470144);
  }

  @Test
  void shouldGetAmsterdamTime() {
    final GenesisConfigOptions config =
        fromConfigOptions(singletonMap("amsterdamTime", 1670470144));
    assertThat(config.getAmsterdamTime()).hasValue(1670470144);
  }

  @Test
  void shouldGetBogotaTime() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("bogotaTime", 1670470144));
    assertThat(config.getBogotaTime()).hasValue(1670470144);
  }

  @Test
  void shouldGetFutureEipsTime() {
    final GenesisConfigOptions config = fromConfigOptions(singletonMap("futureEipsTime", 1337));
    assertThat(config.getFutureEipsTime()).hasValue(1337);
  }

  @Test
  void shouldGetExperimentalEipsTime() {
    final GenesisConfigOptions config =
        fromConfigOptions(singletonMap("experimentalEipsTime", 1337));
    assertThat(config.getExperimentalEipsTime()).hasValue(1337);
  }

  @Test
  void shouldNotReturnEmptyOptionalWhenBlockNumberNotSpecified() {
    final GenesisConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getHomesteadBlockNumber()).isEmpty();
    assertThat(config.getDaoForkBlock()).isEmpty();
    assertThat(config.getTangerineWhistleBlockNumber()).isEmpty();
    assertThat(config.getSpuriousDragonBlockNumber()).isEmpty();
    assertThat(config.getByzantiumBlockNumber()).isEmpty();
    assertThat(config.getConstantinopleBlockNumber()).isEmpty();
    assertThat(config.getPetersburgBlockNumber()).isEmpty();
    assertThat(config.getIstanbulBlockNumber()).isEmpty();
    assertThat(config.getMuirGlacierBlockNumber()).isEmpty();
    assertThat(config.getBerlinBlockNumber()).isEmpty();
    assertThat(config.getLondonBlockNumber()).isEmpty();
    assertThat(config.getArrowGlacierBlockNumber()).isEmpty();
    assertThat(config.getGrayGlacierBlockNumber()).isEmpty();
    assertThat(config.getMergeNetSplitBlockNumber()).isEmpty();
    assertThat(config.getShanghaiTime()).isEmpty();
    assertThat(config.getCancunTime()).isEmpty();
    assertThat(config.getPragueTime()).isEmpty();
    assertThat(config.getOsakaTime()).isEmpty();
    assertThat(config.getBpo1Time()).isEmpty();
    assertThat(config.getBpo2Time()).isEmpty();
    assertThat(config.getBpo3Time()).isEmpty();
    assertThat(config.getBpo4Time()).isEmpty();
    assertThat(config.getBpo5Time()).isEmpty();
    assertThat(config.getAmsterdamTime()).isEmpty();
    assertThat(config.getBogotaTime()).isEmpty();
    assertThat(config.getFutureEipsTime()).isEmpty();
    assertThat(config.getExperimentalEipsTime()).isEmpty();
  }

  @Test
  void shouldGetChainIdWhenSpecified() {
    final GenesisConfigOptions config =
        fromConfigOptions(singletonMap("chainId", BigInteger.valueOf(32)));
    assertThat(config.getChainId()).hasValue(BigInteger.valueOf(32));
  }

  @Test
  void shouldSupportEmptyGenesisConfig() {
    final GenesisConfigOptions config = GenesisConfig.fromConfig("{}").getConfigOptions();
    assertThat(config.isEthHash()).isFalse();
    assertThat(config.isClique()).isFalse();
    assertThat(config.isPoa()).isFalse();
    assertThat(config.getHomesteadBlockNumber()).isEmpty();
  }

  @Test
  void shouldGetTerminalTotalDifficultyWhenSpecified() {
    final GenesisConfigOptions config =
        fromConfigOptions(singletonMap("terminalTotalDifficulty", BigInteger.valueOf(1000)));
    assertThat(config.getTerminalTotalDifficulty()).isPresent();
    assertThat(config.getTerminalTotalDifficulty()).contains(UInt256.valueOf(1000));

    // stubJsonGenesis
    final GenesisConfigOptions stub =
        new StubGenesisConfigOptions().terminalTotalDifficulty(UInt256.valueOf(500));
    assertThat(stub.getTerminalTotalDifficulty()).isPresent();
    assertThat(stub.getTerminalTotalDifficulty()).contains(UInt256.valueOf(500));
  }

  @Test
  void shouldNotReturnTerminalTotalDifficultyWhenNotSpecified() {
    final GenesisConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getTerminalTotalDifficulty()).isNotPresent();
    // stubJsonGenesis
    assertThat(new StubGenesisConfigOptions().getTerminalTotalDifficulty()).isNotPresent();
  }

  @Test
  void isZeroBaseFeeShouldDefaultToFalse() {
    final GenesisConfigOptions config = GenesisConfig.fromConfig("{}").getConfigOptions();

    assertThat(config.isZeroBaseFee()).isFalse();
  }

  @Test
  void isZeroBaseFeeParsedCorrectly() {
    final GenesisConfigOptions config = fromConfigOptions(Map.of("zerobasefee", true));

    assertThat(config.isZeroBaseFee()).isTrue();
  }

  @Test
  void asMapIncludesZeroBaseFee() {
    final GenesisConfigOptions config = fromConfigOptions(Map.of("zerobasefee", true));

    assertThat(config.asMap()).containsOnlyKeys("zeroBaseFee").containsValue(true);
  }

  @Test
  void isFixedBaseFeeShouldDefaultToFalse() {
    final GenesisConfigOptions config = GenesisConfig.fromConfig("{}").getConfigOptions();

    assertThat(config.isFixedBaseFee()).isFalse();
  }

  @Test
  void isFixedBaseFeeParsedCorrectly() {
    final GenesisConfigOptions config = fromConfigOptions(Map.of("fixedbasefee", true));

    assertThat(config.isFixedBaseFee()).isTrue();
  }

  @Test
  void asMapIncludesFixedBaseFee() {
    final GenesisConfigOptions config = fromConfigOptions(Map.of("fixedbasefee", true));

    assertThat(config.asMap()).containsOnlyKeys("fixedBaseFee").containsValue(true);
  }

  @Test
  void asMapIncludesBogotaTime() {
    final GenesisConfigOptions config = fromConfigOptions(Map.of("bogotaTime", 1670470145));

    assertThat(config.asMap()).containsOnlyKeys("bogotaTime").containsValue(1670470145L);
  }

  @Test
  void shouldGetWithdrawalRequestContractAddress() {

    final String address = "0x0000000000000000111111119cbe0530deadbeef";
    final GenesisConfigOptions config =
        fromConfigOptions(singletonMap("withdrawalRequestContractAddress", address));
    assertThat(config.getWithdrawalRequestContractAddress())
        .hasValue(Address.fromHexString(address));
  }

  @Test
  void shouldNotHaveWithdrawalRequestContractAddressWhenEmpty() {
    final GenesisConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getWithdrawalRequestContractAddress()).isEmpty();
  }

  @Test
  void asMapIncludesWithdrawalRequestContractAddress() {
    final GenesisConfigOptions config =
        fromConfigOptions(Map.of("withdrawalRequestContractAddress", "0x0"));

    assertThat(config.asMap())
        .containsOnlyKeys("withdrawalRequestContractAddress")
        .containsValue(Address.ZERO);
  }

  @Test
  void shouldGetDepositContractAddress() {
    final String address = "0x00000000deadbeefdeadbeef0000000011111111";
    final GenesisConfigOptions config =
        fromConfigOptions(singletonMap("depositContractAddress", address));
    assertThat(config.getDepositContractAddress()).hasValue(Address.fromHexString(address));
  }

  @Test
  void shouldNotHaveDepositContractAddressWhenEmpty() {
    final GenesisConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getDepositContractAddress()).isEmpty();
  }

  @Test
  void asMapIncludesDepositContractAddress() {
    final GenesisConfigOptions config = fromConfigOptions(Map.of("depositContractAddress", "0x0"));

    assertThat(config.asMap())
        .containsOnlyKeys("depositContractAddress")
        .containsValue(Address.ZERO);
  }

  @Test
  void shouldGetConsolidationRequestContractAddress() {
    final GenesisConfigOptions config =
        fromConfigOptions(
            singletonMap(
                "consolidationRequestContractAddress",
                "0x00000000219ab540356cbb839cbe05303d7705fa"));
    assertThat(config.getConsolidationRequestContractAddress())
        .hasValue(Address.fromHexString("0x00000000219ab540356cbb839cbe05303d7705fa"));
  }

  @Test
  void shouldNotHaveConsolidationRequestContractAddressWhenEmpty() {
    final GenesisConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getConsolidationRequestContractAddress()).isEmpty();
  }

  @Test
  void asMapIncludesConsolidationRequestContractAddress() {
    final GenesisConfigOptions config =
        fromConfigOptions(Map.of("consolidationRequestContractAddress", "0x0"));

    assertThat(config.asMap())
        .containsOnlyKeys("consolidationRequestContractAddress")
        .containsValue(Address.ZERO);
  }

  @SuppressWarnings("unchecked")
  @Test
  void asMapIncludesBlobFeeSchedule() {
    final GenesisConfigOptions config =
        GenesisConfig.fromConfig(
                """
              {
                "config": {
                  "blobSchedule": {
                    "cancun": {
                      "target": 1,
                      "max": 2,
                      "baseFeeUpdateFraction": 3
                    },
                    "prague": {
                      "target": 4,
                      "max": 5,
                      "baseFeeUpdateFraction": 6
                    },
                    "bpo1": {
                      "target": 7,
                      "max": 9,
                      "baseFeeUpdateFraction": 10
                    },
                    "bpo2": {
                      "target": 11,
                      "max": 12,
                      "baseFeeUpdateFraction": 13
                    },
                    "bpo3": {
                      "target": 14,
                      "max": 15,
                      "baseFeeUpdateFraction": 16
                    },
                    "bpo4": {
                      "target": 17,
                      "max": 18,
                      "baseFeeUpdateFraction": 19
                    },
                    "bpo5": {
                      "target": 20,
                      "max": 21,
                      "baseFeeUpdateFraction": 22
                    }
                  }
                }
              }""")
            .getConfigOptions();

    final Map<String, Object> map = config.asMap();
    assertThat(map).containsOnlyKeys("blobSchedule");
    final Map<String, Object> blobSchedule = (Map<String, Object>) map.get("blobSchedule");
    assertThat(blobSchedule)
        .containsOnlyKeys("cancun", "prague", "bpo1", "bpo2", "bpo3", "bpo4", "bpo5");
    assertBlobDefinition((Map<String, Object>) blobSchedule.get("cancun"), 1, 2, 3);
    assertBlobDefinition((Map<String, Object>) blobSchedule.get("prague"), 4, 5, 6);
    assertBlobDefinition((Map<String, Object>) blobSchedule.get("bpo1"), 7, 9, 10);
    assertBlobDefinition((Map<String, Object>) blobSchedule.get("bpo2"), 11, 12, 13);
    assertBlobDefinition((Map<String, Object>) blobSchedule.get("bpo3"), 14, 15, 16);
    assertBlobDefinition((Map<String, Object>) blobSchedule.get("bpo4"), 17, 18, 19);
    assertBlobDefinition((Map<String, Object>) blobSchedule.get("bpo5"), 20, 21, 22);
  }

  private void assertBlobDefinition(
      final Map<String, Object> blobDefinitionMap,
      final int expectedTarget,
      final int expectedMax,
      final int expectedBaseFeeUpdateFraction) {
    assertThat(blobDefinitionMap)
        .containsOnlyKeys("target", "max", "baseFeeUpdateFraction")
        .containsEntry("target", expectedTarget)
        .containsEntry("max", expectedMax)
        .containsEntry("baseFeeUpdateFraction", expectedBaseFeeUpdateFraction);
  }

  private GenesisConfigOptions fromConfigOptions(final Map<String, Object> configOptions) {
    final ObjectNode rootNode = JsonUtil.createEmptyObjectNode();
    final ObjectNode options = JsonUtil.objectNodeFromMap(configOptions);
    rootNode.set("config", options);
    return GenesisConfig.fromConfig(rootNode).getConfigOptions();
  }
}
