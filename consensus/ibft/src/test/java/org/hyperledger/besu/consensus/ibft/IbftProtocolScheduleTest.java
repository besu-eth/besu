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
package org.hyperledger.besu.consensus.ibft;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hyperledger.besu.consensus.common.bft.BftContextBuilder.setupContextWithBftExtraDataEncoder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.JsonBftConfigOptions;
import org.hyperledger.besu.config.JsonGenesisConfigOptions;
import org.hyperledger.besu.config.JsonQbftConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.consensus.common.bft.MutableBftConfigOptions;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.MilestoneStreamingProtocolSchedule;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.core.components.EthereumCoreComponent;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IbftProtocolScheduleTest {

  private ProtocolContext protocolContext;
  private List<Address> validators;
  private NodeKey proposerNodeKey;
  private TestEthCoreComponent component;

  @BeforeEach
  public void setup() {
    TestEthCoreComponent component =
        DaggerIbftProtocolScheduleTest_TestEthCoreComponent.builder().build();
    this.component = component;
    this.protocolContext = component.protocolContext();
    this.validators = component.validators();
    this.proposerNodeKey = component.nodeKey();
  }

  @Test
  public void blockModeTransitionsCreatesBlockModeHeaderValidators() {
    final MutableBftConfigOptions arbitraryTransition =
        new MutableBftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    arbitraryTransition.setBlockRewardWei(BigInteger.ONE);

    final BlockHeader parentHeader =
        IbftBlockHeaderUtils.createPresetHeaderBuilder(1, proposerNodeKey, validators, null)
            .buildHeader();
    final BlockHeader blockHeader =
        IbftBlockHeaderUtils.createPresetHeaderBuilder(2, proposerNodeKey, validators, parentHeader)
            .buildHeader();

    final BftProtocolSchedule schedule =
        createProtocolSchedule(component.bftExtraDataCodec(), arbitraryTransition);
    assertThat(new MilestoneStreamingProtocolSchedule(schedule).streamMilestoneBlocks().count())
        .isEqualTo(3);
    assertThat(validateHeader(schedule, parentHeader, blockHeader, 0)).isTrue();
    assertThat(validateHeader(schedule, parentHeader, blockHeader, 1)).isTrue();
    assertThat(validateHeader(schedule, parentHeader, blockHeader, 2)).isTrue();
  }

  @Test
  public void enforcesConfiguredTransactionGasLimit() {
    final MutableBftConfigOptions genesisOptions =
        new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT);
    genesisOptions.setTransactionGasLimit(OptionalLong.of(8_000_000L));

    final BftProtocolSchedule schedule =
        createProtocolSchedule(component.bftExtraDataCodec(), genesisOptions);

    // arbitraryTransition is the fork at block 1; look up via that block
    assertThat(
            schedule
                .getByBlockNumberOrTimestamp(1, 0)
                .getGasLimitCalculator()
                .transactionGasLimitCap())
        .isEqualTo(8_000_000L);
  }

  @Test
  public void specIsNotPoSWhenShanghaiOrLaterForkIsConfigured() {
    final long shanghaiTime = 10;
    final ObjectNode genesisConfigNode = JsonUtil.createEmptyObjectNode();
    genesisConfigNode.put("shanghaitime", shanghaiTime);

    final BftProtocolSchedule schedule =
        IbftProtocolScheduleBuilder.create(
            JsonGenesisConfigOptions.fromJsonObject(genesisConfigNode),
            new ForksSchedule<>(List.of(new ForkSpec<>(0, JsonBftConfigOptions.DEFAULT))),
            false,
            mock(BftExtraDataCodec.class),
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem(),
            8_000_000L);

    final ProtocolSpec shanghaiSpec = schedule.getByBlockNumberOrTimestamp(1, shanghaiTime);

    // IBFT is a PoA consensus, so its specs must never be reported as PoS, otherwise the PoS
    // transaction selection timeout is used instead of the configured PoA percentage.
    assertThat(shanghaiSpec.isPoS()).isFalse();
  }

  @Test
  public void poaBlockTxsSelectionMaxTimeAppliesWhenShanghaiOrLaterForkIsConfigured() {
    final long shanghaiTime = 10;
    final ObjectNode genesisConfigNode = JsonUtil.createEmptyObjectNode();
    genesisConfigNode.put("shanghaitime", shanghaiTime);

    final BftProtocolSchedule schedule =
        IbftProtocolScheduleBuilder.create(
            JsonGenesisConfigOptions.fromJsonObject(genesisConfigNode),
            new ForksSchedule<>(List.of(new ForkSpec<>(0, JsonBftConfigOptions.DEFAULT))),
            false,
            mock(BftExtraDataCodec.class),
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem(),
            8_000_000L);

    final ProtocolSpec shanghaiSpec = schedule.getByBlockNumberOrTimestamp(1, shanghaiTime);

    // 2s block period with --poa-block-txs-selection-max-time=75
    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                ImmutableMiningConfiguration.MutableInitValues.builder()
                    .blockPeriodSeconds(2)
                    .build())
            .poaBlockTxsSelectionMaxTime(PositiveNumber.fromInt(75))
            .build();

    // same expression BlockTransactionSelector uses to size the selection timeout: the PoA
    // percentage of the block period must apply, not the PoS timeout (5000 ms by default)
    assertThat(miningConfiguration.getBlockTxsSelectionMaxTime(shanghaiSpec.isPoS()))
        .isEqualTo(Duration.ofMillis(1500));
  }

  private boolean validateHeader(
      final BftProtocolSchedule schedule,
      final BlockHeader parentHeader,
      final BlockHeader blockHeader,
      final int block) {
    return schedule
        .getByBlockNumberOrTimestamp(block, blockHeader.getTimestamp())
        .getBlockHeaderValidator()
        .validateHeader(
            blockHeader, parentHeader, this.protocolContext, HeaderValidationMode.LIGHT);
  }

  private BftProtocolSchedule createProtocolSchedule(
      final BftExtraDataCodec bftExtraDataCodec,
      final MutableBftConfigOptions arbitraryTransition) {

    var genesisConfig = JsonGenesisConfigOptions.fromJsonObject(JsonUtil.createEmptyObjectNode());
    ForksSchedule<BftConfigOptions> forkSched =
        new ForksSchedule<>(
            List.of(
                new ForkSpec<>(0, JsonQbftConfigOptions.DEFAULT),
                new ForkSpec<>(1, arbitraryTransition),
                new ForkSpec<>(2, JsonQbftConfigOptions.DEFAULT)));

    return IbftProtocolScheduleBuilder.create(
        genesisConfig,
        forkSched,
        false,
        bftExtraDataCodec,
        EvmConfiguration.DEFAULT,
        MiningConfiguration.MINING_DISABLED,
        new BadBlockManager(),
        false,
        BalConfiguration.DEFAULT,
        new NoOpMetricsSystem(),
        8_000_000L);
  }

  @Module
  static class IbftProtocolScheduleModule {
    @Provides
    @Singleton
    NodeKey nodeKey() {
      return NodeKeyUtils.generate();
    }

    @Provides
    Address provideProposerAddress(final NodeKey proposerNodeKey) {
      return Util.publicKeyToAddress(proposerNodeKey.getPublicKey());
    }

    @Provides
    List<Address> provideValidators(final Address proposerAddress) {
      return singletonList(proposerAddress);
    }

    @Provides
    public BftExtraData mockBftExtraData(final List<Address> validators) {
      BftExtraData bftExtraData = mock(BftExtraData.class);
      when(bftExtraData.getValidators()).thenReturn(validators);
      return bftExtraData;
    }

    @Provides
    public BftExtraDataCodec mockBftExtraDataCodec(final BftExtraData bftExtraData) {
      BftExtraDataCodec bftExtraDataCodec = mock(BftExtraDataCodec.class);
      when(bftExtraDataCodec.decode(any())).thenReturn(bftExtraData);
      return bftExtraDataCodec;
    }

    @Provides
    ProtocolContext protocolContext(
        final List<Address> validators, final BftExtraDataCodec bftExtraDataCodec) {
      return new ProtocolContext.Builder()
          .withConsensusContext(
              setupContextWithBftExtraDataEncoder(BftContext.class, validators, bftExtraDataCodec))
          .build();
    }
  }

  @Singleton
  @Component(modules = {NoMiningParamters.class, IbftProtocolScheduleModule.class})
  interface TestEthCoreComponent extends EthereumCoreComponent {
    ProtocolContext protocolContext();

    List<Address> validators();

    NodeKey nodeKey();

    BftExtraDataCodec bftExtraDataCodec();
  }

  @Module
  static class NoMiningParamters {
    @Provides
    MiningConfiguration provideMiningParameters() {
      return MiningConfiguration.MINING_DISABLED;
    }
  }
}
