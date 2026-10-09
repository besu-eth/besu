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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.ARROW_GLACIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BERLIN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BYZANTIUM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CONSTANTINOPLE;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.DAO_RECOVERY;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.FRONTIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.GRAY_GLACIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.HOMESTEAD;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.ISTANBUL;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.LONDON;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.MUIR_GLACIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PETERSBURG;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SPURIOUS_DRAGON;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.TANGERINE_WHISTLE;
import static org.hyperledger.besu.ethereum.mainnet.headervalidationrules.DaoExtraDataValidationRule.DAO_EXTRA_DATA;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.mainnet.forkstatechange.ForkStateChangeProcessor;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestContractAddresses;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class MainnetProtocolScheduleTest {

  @Test
  public void shouldReturnMainnetDefaultProtocolSpecsWhenCustomNumbersAreNotUsed() {
    final ProtocolSchedule sched = ProtocolScheduleFixture.MAINNET;
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(1L)).getHardforkId())
        .isEqualTo(FRONTIER);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(1_150_000L)).getHardforkId())
        .isEqualTo(HOMESTEAD);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(1_920_000L)).getHardforkId())
        .isEqualTo(DAO_RECOVERY);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(2_463_000L)).getHardforkId())
        .isEqualTo(TANGERINE_WHISTLE);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(2_675_000L)).getHardforkId())
        .isEqualTo(SPURIOUS_DRAGON);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(4_730_000L)).getHardforkId())
        .isEqualTo(BYZANTIUM);
    // Constantinople was originally scheduled for 7_080_000, but postponed
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(7_080_000L)).getHardforkId())
        .isEqualTo(BYZANTIUM);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(7_280_000L)).getHardforkId())
        .isEqualTo(PETERSBURG);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(9_069_000L)).getHardforkId())
        .isEqualTo(ISTANBUL);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(9_200_000L)).getHardforkId())
        .isEqualTo(MUIR_GLACIER);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(12_244_000L)).getHardforkId())
        .isEqualTo(BERLIN);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(12_965_000L)).getHardforkId())
        .isEqualTo(LONDON);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(13_773_000L)).getHardforkId())
        .isEqualTo(ARROW_GLACIER);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(15_050_000L)).getHardforkId())
        .isEqualTo(GRAY_GLACIER);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(Long.MAX_VALUE)).getHardforkId())
        .isEqualTo(GRAY_GLACIER);
  }

  @Test
  public void shouldOnlyUseFrontierWhenEmptyJsonConfigIsUsed() {
    final ProtocolSchedule sched =
        MainnetProtocolSchedule.fromConfig(
            GenesisConfig.fromConfig("{}").getConfigOptions(),
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(1L)).getHardforkId())
        .isEqualTo(FRONTIER);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(Long.MAX_VALUE)).getHardforkId())
        .isEqualTo(FRONTIER);
  }

  @Test
  public void createFromConfigWithSettings() {
    final String json =
        "{\"config\": {\"homesteadBlock\": 2, \"daoForkBlock\": 3, \"eip150Block\": 14, \"eip158Block\": 15, \"byzantiumBlock\": 16, \"constantinopleBlock\": 18, \"petersburgBlock\": 19, \"chainId\":1234}}";
    final ProtocolSchedule sched =
        MainnetProtocolSchedule.fromConfig(
            GenesisConfig.fromConfig(json).getConfigOptions(),
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(1)).getHardforkId())
        .isEqualTo(FRONTIER);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(2)).getHardforkId())
        .isEqualTo(HOMESTEAD);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(3)).getHardforkId())
        .isEqualTo(DAO_RECOVERY);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(14)).getHardforkId())
        .isEqualTo(TANGERINE_WHISTLE);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(15)).getHardforkId())
        .isEqualTo(SPURIOUS_DRAGON);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(16)).getHardforkId())
        .isEqualTo(BYZANTIUM);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(18)).getHardforkId())
        .isEqualTo(CONSTANTINOPLE);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(19)).getHardforkId())
        .isEqualTo(PETERSBURG);
  }

  @Test
  public void outOfOrderConstantinoplesFail() {
    final String json =
        "{\"config\": {\"homesteadBlock\": 2, \"daoForkBlock\": 3, \"eip150Block\": 14, \"eip158Block\": 15, \"byzantiumBlock\": 16, \"constantinopleBlock\": 18, \"petersburgBlock\": 17, \"chainId\":1234}}";
    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .describedAs(
            "Genesis Config Error: 'Petersburg' is scheduled for block 17 but it must be on or after block 18.")
        .isThrownBy(
            () ->
                MainnetProtocolSchedule.fromConfig(
                    GenesisConfig.fromConfig(json).getConfigOptions(),
                    EvmConfiguration.DEFAULT,
                    MiningConfiguration.MINING_DISABLED,
                    new BadBlockManager(),
                    false,
                    BalConfiguration.DEFAULT,
                    new NoOpMetricsSystem()));
  }

  @Test
  public void shouldDefaultRequestContractAddressesForEthClientsMainnetGenesis() {
    // Config section of https://github.com/eth-clients/mainnet/blob/main/metadata/genesis.json,
    // which sets pragueTime but neither request contract address.
    final String json =
        """
        {
          "config": {
            "chainId": 1,
            "homesteadBlock": 1150000,
            "daoForkBlock": 1920000,
            "daoForkSupport": true,
            "eip150Block": 2463000,
            "eip150Hash": "0x2086799aeebeae135c246c65021c82b4e15a2c451340993aacfd2751886514f0",
            "eip155Block": 2675000,
            "eip158Block": 2675000,
            "byzantiumBlock": 4370000,
            "constantinopleBlock": 7280000,
            "petersburgBlock": 7280000,
            "istanbulBlock": 9069000,
            "muirGlacierBlock": 9200000,
            "berlinBlock": 12244000,
            "londonBlock": 12965000,
            "arrowGlacierBlock": 13773000,
            "grayGlacierBlock": 15050000,
            "terminalTotalDifficulty": 58750000000000000000000,
            "terminalTotalDifficultyPassed": true,
            "shanghaiTime": 1681338455,
            "cancunTime": 1710338135,
            "pragueTime": 1746612311,
            "osakaTime": 1764798551,
            "bpo1Time": 1765290071,
            "bpo2Time": 1767747671,
            "ethash": {},
            "depositContractAddress": "0x00000000219ab540356cBB839Cbe05303d7705Fa",
            "blobSchedule": {
              "cancun": {
                "target": 3,
                "max": 6,
                "baseFeeUpdateFraction": 3338477
              },
              "prague": {
                "target": 6,
                "max": 9,
                "baseFeeUpdateFraction": 5007716
              },
              "bpo1": {
                "target": 10,
                "max": 15,
                "baseFeeUpdateFraction": 8346193
              },
              "bpo2": {
                "target": 14,
                "max": 21,
                "baseFeeUpdateFraction": 11684671
              }
            }
          }
        }
        """;
    final GenesisConfigOptions options = GenesisConfig.fromConfig(json).getConfigOptions();
    final ProtocolSchedule schedule =
        MainnetProtocolSchedule.fromConfig(
            options,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());

    for (final long forkTime :
        List.of(options.getPragueTime().orElseThrow(), options.getOsakaTime().orElseThrow())) {
      final BlockHeader header =
          new BlockHeaderTestFixture().number(22_000_000L).timestamp(forkTime).buildHeader();

      Assertions.assertThat(
              schedule
                  .getByBlockHeader(header)
                  .getRequestProcessorCoordinator()
                  .orElseThrow()
                  .getContractConfigs())
          .containsEntry(
              "WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS",
              RequestContractAddresses.DEFAULT_WITHDRAWAL_REQUEST_CONTRACT_ADDRESS.toHexString())
          .containsEntry(
              "CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS",
              RequestContractAddresses.DEFAULT_CONSOLIDATION_REQUEST_CONTRACT_ADDRESS
                  .toHexString());
    }
  }

  @Test
  public void daoExtraDataIsRequiredOnTheDaoForkBlocks() {
    final ProtocolSchedule sched = scheduleFromConfig("\"homesteadBlock\": 2, \"daoForkBlock\": 3");

    for (final long number : new long[] {3, 12}) {
      Assertions.assertThat(requiresDaoExtraData(sched, number)).as("block %d", number).isTrue();
    }
    for (final long number : new long[] {2, 13}) {
      Assertions.assertThat(requiresDaoExtraData(sched, number)).as("block %d", number).isFalse();
    }
  }

  @Test
  public void daoExtraDataIsRequiredWhenALaterForkActivatesWithinTheDaoForkBlocks() {
    final ProtocolSchedule sched =
        scheduleFromConfig("\"homesteadBlock\": 2, \"daoForkBlock\": 3, \"eip150Block\": 5");

    Assertions.assertThat(sched.getByBlockHeader(blockHeader(5)).getHardforkId())
        .isEqualTo(TANGERINE_WHISTLE);
    Assertions.assertThat(requiresDaoExtraData(sched, 5)).isTrue();
    Assertions.assertThat(requiresDaoExtraData(sched, 12)).isTrue();
    Assertions.assertThat(requiresDaoExtraData(sched, 13)).isFalse();
  }

  @Test
  public void londonDropsTheDaoExtraDataRule() {
    // London sets its own header validator, so the rule ends there, even within the DAO fork
    // blocks.
    final ProtocolSchedule sched =
        scheduleFromConfig("\"homesteadBlock\": 2, \"daoForkBlock\": 3, \"londonBlock\": 5");

    Assertions.assertThat(sched.getByBlockHeader(blockHeader(4)).getHardforkId())
        .isEqualTo(DAO_RECOVERY);
    Assertions.assertThat(requiresDaoExtraData(sched, 4)).isTrue();
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(6)).getHardforkId()).isEqualTo(LONDON);
    Assertions.assertThat(requiresDaoExtraData(sched, 6)).isFalse();
  }

  @Test
  public void forkOnTheDaoForkBlockKeepsTheDaoRefund() {
    final ProtocolSchedule sched =
        scheduleFromConfig("\"homesteadBlock\": 2, \"daoForkBlock\": 5, \"eip150Block\": 5");

    Assertions.assertThat(sched.getByBlockHeader(blockHeader(5)).getHardforkId())
        .isEqualTo(TANGERINE_WHISTLE);
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(5)).getForkStateChangeProcessor())
        .isNotSameAs(ForkStateChangeProcessor.NONE);
  }

  @Test
  public void daoForkAfterTangerineWhistleIsRejected() {
    // Like geth, the DAO fork is ordered between Homestead and Tangerine Whistle.
    Assertions.assertThatThrownBy(
            () ->
                scheduleFromConfig(
                    "\"homesteadBlock\": 2, \"eip150Block\": 5, \"daoForkBlock\": 7"))
        .hasMessageContaining("milestone 5 but it must be on or after milestone 7");
  }

  @Test
  public void daoForkIsAMilestoneOnlyWhereConfigured() {
    Assertions.assertThat(
            scheduleFromConfig("\"homesteadBlock\": 2, \"daoForkBlock\": 3, \"eip150Block\": 5")
                .milestoneFor(DAO_RECOVERY))
        .contains(3L);
    // Unlike other forks, the DAO fork does not activate with the next configured fork.
    Assertions.assertThat(
            scheduleFromConfig("\"homesteadBlock\": 2, \"eip150Block\": 5")
                .milestoneFor(DAO_RECOVERY))
        .isEmpty();
  }

  @Test
  public void chainWithoutADaoForkHasNoDaoRules() {
    final ProtocolSchedule sched = scheduleFromConfig("\"homesteadBlock\": 2");

    Assertions.assertThat(requiresDaoExtraData(sched, 3)).isFalse();
    Assertions.assertThat(sched.getByBlockHeader(blockHeader(3)).getForkStateChangeProcessor())
        .isSameAs(ForkStateChangeProcessor.NONE);
  }

  private static ProtocolSchedule scheduleFromConfig(final String forks) {
    return MainnetProtocolSchedule.fromConfig(
        GenesisConfig.fromConfig("{\"config\": {" + forks + ", \"chainId\":1234}}")
            .getConfigOptions(),
        EvmConfiguration.DEFAULT,
        MiningConfiguration.MINING_DISABLED,
        new BadBlockManager(),
        false,
        BalConfiguration.DEFAULT,
        new NoOpMetricsSystem());
  }

  /**
   * Whether block {@code number}'s header validator rejects a header that lacks the DAO extra data
   * but is otherwise valid.
   */
  private boolean requiresDaoExtraData(final ProtocolSchedule sched, final long number) {
    final ProtocolSpec spec = sched.getByBlockHeader(blockHeader(number));
    // A header valid under the fork: from London, a base fee, kept the same by a parent at its gas
    // target; from Paris, no difficulty or nonce.
    final long gasLimit = 5_000_000;
    final BlockHeaderTestFixture parentFixture =
        new BlockHeaderTestFixture()
            .number(number - 1)
            .gasLimit(gasLimit)
            .gasUsed(gasLimit / 2)
            .timestamp(100);
    final BlockHeaderTestFixture header =
        new BlockHeaderTestFixture().number(number).gasLimit(gasLimit).gasUsed(0).timestamp(110);
    if (spec.getFeeMarket().implementsBaseFee()) {
      final Wei baseFee = Wei.of(1_000_000_000L);
      parentFixture.baseFeePerGas(baseFee);
      header.baseFeePerGas(baseFee);
    }
    if (spec.isPoS()) {
      parentFixture.difficulty(Difficulty.ZERO).nonce(0);
      header.difficulty(Difficulty.ZERO).nonce(0);
    }
    final BlockHeader parent = parentFixture.buildHeader();
    header.parentHash(parent.getHash());
    final BlockHeaderValidator validator = spec.getBlockHeaderValidator();
    Assertions.assertThat(
            validator.validateHeader(
                header.extraData(DAO_EXTRA_DATA).buildHeader(),
                parent,
                null,
                HeaderValidationMode.FULL))
        .isTrue();
    return !validator.validateHeader(
        header.extraData(Bytes.EMPTY).buildHeader(), parent, null, HeaderValidationMode.FULL);
  }

  private BlockHeader blockHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }
}
