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
package org.hyperledger.besu.consensus.merge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PARIS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.blockhash.PraguePreExecutionProcessor;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.operation.PrevRanDaoOperation;
import org.hyperledger.besu.evm.operation.Push0Operation;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

public class MergeProtocolScheduleTest {

  @Test
  public void protocolSpecsAreCreatedAtBlockDefinedInJson() {
    final String jsonInput =
        "{\"config\": "
            + "{\"chainId\": 1,\n"
            + "\"homesteadBlock\": 1,\n"
            + "\"LondonBlock\": 1559}"
            + "}";

    final GenesisConfigOptions config = GenesisConfig.fromConfig(jsonInput).getConfigOptions();
    final ProtocolSchedule protocolSchedule =
        MergeProtocolSchedule.create(
            config,
            false,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem(),
            EvmConfiguration.DEFAULT);

    final ProtocolSpec homesteadSpec = protocolSchedule.getByBlockHeader(blockHeader(1));
    final ProtocolSpec londonSpec = protocolSchedule.getByBlockHeader(blockHeader(1559));

    assertThat(homesteadSpec).isNotEqualTo(londonSpec);
    assertThat(homesteadSpec.getFeeMarket().implementsBaseFee()).isFalse();
    assertThat(londonSpec.getFeeMarket().implementsBaseFee()).isTrue();
  }

  @Test
  public void mergeSpecificModificationsAreUnappliedForShanghai() {

    final GenesisConfigOptions config = GenesisConfig.mainnet().getConfigOptions();
    final ProtocolSchedule protocolSchedule =
        MergeProtocolSchedule.create(
            config,
            false,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem(),
            EvmConfiguration.DEFAULT);

    final long lastParisBlockNumber = 17034869L;
    final ProtocolSpec parisSpec =
        protocolSchedule.getByBlockHeader(blockHeader(lastParisBlockNumber));
    final ProtocolSpec shanghaiSpec =
        protocolSchedule.getByBlockHeader(
            new BlockHeaderTestFixture().timestamp(1681338455).buildHeader());

    assertThat(parisSpec.getHardforkId()).isEqualTo(PARIS);
    assertThat(shanghaiSpec.getHardforkId()).isEqualTo(SHANGHAI);

    // ensure PUSH0 is enabled in Shanghai
    final int PUSH0 = 0x5f;
    assertThat(parisSpec.getEvm().getOperationsUnsafe()[PUSH0])
        .isInstanceOf(InvalidOperation.class);
    assertThat(shanghaiSpec.getEvm().getOperationsUnsafe()[PUSH0])
        .isInstanceOf(Push0Operation.class);

    assertProofOfStakeConfigIsEnabled(parisSpec);
    assertProofOfStakeConfigIsEnabled(shanghaiSpec);
  }

  @Test
  public void mergeSpecificModificationsAreUnappliedForCancun_whenShanghaiNotConfigured() {

    final String jsonInput =
        "{\"config\": "
            + "{\"chainId\": 1,\n"
            + "\"parisBlock\": 0,\n"
            + "\"cancunTime\": 1000}"
            + "}";

    final GenesisConfigOptions config = GenesisConfig.fromConfig(jsonInput).getConfigOptions();
    final ProtocolSchedule protocolSchedule =
        MergeProtocolSchedule.create(
            config,
            false,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem(),
            EvmConfiguration.DEFAULT);

    final ProtocolSpec parisSpec =
        protocolSchedule.getByBlockHeader(
            new BlockHeaderTestFixture().number(9).timestamp(999).buildHeader());
    final ProtocolSpec cancunSpec =
        protocolSchedule.getByBlockHeader(
            new BlockHeaderTestFixture().number(10).timestamp(1000).buildHeader());

    assertThat(parisSpec.getHardforkId()).isEqualTo(PARIS);
    assertThat(cancunSpec.getHardforkId()).isEqualTo(CANCUN);

    // ensure PUSH0 is enabled in Cancun (i.e. it has picked up the Shanghai change rather than been
    // reverted to Paris)
    final int PUSH0 = 0x5f;
    assertThat(parisSpec.getEvm().getOperationsUnsafe()[PUSH0])
        .isInstanceOf(InvalidOperation.class);
    assertThat(cancunSpec.getEvm().getOperationsUnsafe()[PUSH0]).isInstanceOf(Push0Operation.class);

    assertProofOfStakeConfigIsEnabled(parisSpec);
    assertProofOfStakeConfigIsEnabled(cancunSpec);
  }

  @Test
  public void mergeSpecificModificationsAreUnappliedForAllMainnetForksAfterParis() {
    final GenesisConfigOptions config = GenesisConfig.mainnet().getConfigOptions();
    final ProtocolSchedule protocolSchedule =
        MergeProtocolSchedule.create(
            config,
            false,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem(),
            EvmConfiguration.DEFAULT);

    final long lastParisBlockNumber = 17034869L;
    final ProtocolSpec parisSpec =
        protocolSchedule.getByBlockHeader(blockHeader(lastParisBlockNumber));
    assertThat(parisSpec.getHardforkId()).isEqualTo(PARIS);

    for (long forkTimestamp : config.getForkBlockTimestamps()) {
      final ProtocolSpec postParisSpec =
          protocolSchedule.getByBlockHeader(
              new BlockHeaderTestFixture().timestamp(forkTimestamp).buildHeader());

      assertThat(postParisSpec.getHardforkId()).isNotEqualTo(PARIS);
      // ensure PUSH0 is enabled from Shanghai onwards
      final int PUSH0 = 0x5f;
      assertThat(parisSpec.getEvm().getOperationsUnsafe()[PUSH0])
          .isInstanceOf(InvalidOperation.class);
      assertThat(postParisSpec.getEvm().getOperationsUnsafe()[PUSH0])
          .isInstanceOf(Push0Operation.class);

      assertProofOfStakeConfigIsEnabled(parisSpec);
      assertProofOfStakeConfigIsEnabled(postParisSpec);
    }
  }

  @Test
  public void parametersAlignWithMainnetWithAdjustments() {
    final ProtocolSpec london =
        MergeProtocolSchedule.create(
                GenesisConfig.DEFAULT.getConfigOptions(),
                false,
                MiningConfiguration.MINING_DISABLED,
                new BadBlockManager(),
                false,
                BalConfiguration.DEFAULT,
                new NoOpMetricsSystem(),
                EvmConfiguration.DEFAULT)
            .getByBlockHeader(blockHeader(0));

    assertThat(london.getHardforkId()).isEqualTo(PARIS);
    assertProofOfStakeConfigIsEnabled(london);
  }

  private static void assertProofOfStakeConfigIsEnabled(final ProtocolSpec spec) {
    assertThat(spec.isPoS()).isTrue();
    assertThat(spec.getEvm().getOperationsUnsafe()[0x44]).isInstanceOf(PrevRanDaoOperation.class);
    assertThat(spec.getDifficultyCalculator().nextDifficulty(-1, null)).isEqualTo(BigInteger.ZERO);
    assertThat(spec.getBlockReward()).isEqualTo(Wei.ZERO);
    assertThat(spec.isSkipZeroBlockRewards()).isTrue();
    assertThat(spec.getBlockProcessor()).isInstanceOf(MainnetBlockProcessor.class);
  }

  private BlockHeader blockHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }

  @Test
  public void amsterdamHasBlockAccessListFactoryWithForkActivated() {
    final String jsonInput =
        "{\"config\": "
            + "{\"chainId\": 20211,\n"
            + "\"homesteadBlock\": 0,\n"
            + "\"eip150Block\": 0,\n"
            + "\"eip155Block\": 0,\n"
            + "\"eip158Block\": 0,\n"
            + "\"byzantiumBlock\": 0,\n"
            + "\"constantinopleBlock\": 0,\n"
            + "\"petersburgBlock\": 0,\n"
            + "\"istanbulBlock\": 0,\n"
            + "\"muirGlacierBlock\": 0,\n"
            + "\"berlinBlock\": 0,\n"
            + "\"londonBlock\": 0,\n"
            + "\"terminalTotalDifficulty\": 0,\n"
            + "\"cancunTime\": 0,\n"
            + "\"pragueTime\": 0,\n"
            + "\"osakaTime\": 0,\n"
            + "\"amsterdamTime\": 0,\n"
            + "\"depositContractAddress\": \"0x4242424242424242424242424242424242424242\",\n"
            + "\"withdrawalRequestContractAddress\": \"0x00A3ca265EBcb825B45F985A16CEFB49958cE017\",\n"
            + "\"consolidationRequestContractAddress\": \"0x00b42dbF2194e931E80326D950320f7d9Dbeac02\"\n"
            + "}}";

    final GenesisConfigOptions config = GenesisConfig.fromConfig(jsonInput).getConfigOptions();
    final ProtocolSchedule protocolSchedule =
        MergeProtocolSchedule.create(
            config,
            false,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem(),
            EvmConfiguration.DEFAULT);

    // Get the Amsterdam protocol spec for a block at timestamp 1
    final ProtocolSpec amsterdamSpec =
        protocolSchedule.getByBlockHeader(
            new BlockHeaderTestFixture().number(1).timestamp(1).buildHeader());

    assertThat(amsterdamSpec.getHardforkId()).isEqualTo(AMSTERDAM);

    // Verify that BlockAccessListFactory is present and fork-activated
    assertThat(amsterdamSpec.getBlockAccessListFactory())
        .withFailMessage("BlockAccessListFactory should be present for Amsterdam, but it was empty")
        .isPresent();
  }

  /**
   * Verifies that a Clique-to-PoS network (with TTD set) uses PraguePreExecutionProcessor for
   * post-merge Prague blocks, not FrontierPreExecutionProcessor. This is a regression test for a
   * bug where isPoAConsensus() returned true for Clique genesis configs even when TTD was set,
   * causing EIP-2935 and EIP-4788 system calls to be skipped for post-merge blocks.
   */
  @Test
  public void cliqueToPoSNetworkUsesPraguePreExecutionProcessorAfterMerge() {
    final String jsonInput =
        "{\"config\": "
            + "{\"chainId\": 59139,\n"
            + "\"homesteadBlock\": 0,\n"
            + "\"eip150Block\": 0,\n"
            + "\"eip155Block\": 0,\n"
            + "\"eip158Block\": 0,\n"
            + "\"byzantiumBlock\": 0,\n"
            + "\"constantinopleBlock\": 0,\n"
            + "\"petersburgBlock\": 0,\n"
            + "\"istanbulBlock\": 0,\n"
            + "\"berlinBlock\": 0,\n"
            + "\"londonBlock\": 0,\n"
            + "\"terminalTotalDifficulty\": 17628883,\n"
            + "\"shanghaiTime\": 1755165600,\n"
            + "\"pragueTime\": 1755770400,\n"
            + "\"clique\": {\n"
            + "  \"blockperiodseconds\": 1,\n"
            + "  \"epochlength\": 30000,\n"
            + "  \"createemptyblocks\": true\n"
            + "},\n"
            + "\"depositContractAddress\": \"0x45152B0bD93Dc1e4c84d70e24edA3CEb12b1a1D3\",\n"
            + "\"withdrawalRequestContractAddress\": \"0xF7B4391C85B1ad1eAF38cb4B45a235cDF9295a7D\",\n"
            + "\"consolidationRequestContractAddress\": \"0x0bbfd3E844Cc1D63D4D86498Ca91FF6de417Ed73\"\n"
            + "}}";

    final GenesisConfigOptions config = GenesisConfig.fromConfig(jsonInput).getConfigOptions();

    // Verify that the genesis is detected as Clique (this is expected)
    assertThat(config.isClique()).isTrue();
    // Verify TTD is set (this is a Clique-to-PoS network)
    assertThat(config.getTerminalTotalDifficulty()).isPresent();

    final ProtocolSchedule protocolSchedule =
        MergeProtocolSchedule.create(
            config,
            false,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem(),
            EvmConfiguration.DEFAULT);

    // Get Prague spec at post-merge timestamp
    final ProtocolSpec pragueSpec =
        protocolSchedule.getByBlockHeader(
            new BlockHeaderTestFixture().number(18000000).timestamp(1755770400).buildHeader());

    assertThat(pragueSpec.getHardforkId()).isEqualTo(PRAGUE);
    assertProofOfStakeConfigIsEnabled(pragueSpec);

    // The critical assertion: Prague blocks on a Clique-to-PoS network must use
    // PraguePreExecutionProcessor (for EIP-2935 and EIP-4788 system calls),
    // NOT FrontierPreExecutionProcessor
    assertThat(pragueSpec.getPreExecutionProcessor())
        .withFailMessage(
            "Prague blocks on a Clique-to-PoS network should use PraguePreExecutionProcessor, "
                + "but got %s. This means EIP-2935 blockhash storage and EIP-4788 beacon root "
                + "system calls are being skipped, causing stateroot mismatches.",
            pragueSpec.getPreExecutionProcessor().getClass().getSimpleName())
        .isInstanceOf(PraguePreExecutionProcessor.class);
  }
}
