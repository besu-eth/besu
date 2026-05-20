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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BYZANTIUM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.FRONTIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.GRAY_GLACIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.HOMESTEAD;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.LONDON;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultProtocolScheduleTest {

  private static final Optional<BigInteger> CHAIN_ID = Optional.of(BigInteger.ONE);
  private static final BigInteger DEFAULT_CHAIN_ID = BigInteger.ONE;
  private static final EvmConfiguration evmConfiguration = EvmConfiguration.DEFAULT;
  private ProtocolScheduleBuilder builder;
  private StubGenesisConfigOptions config;

  private final Function<ProtocolSpecBuilder, ProtocolSpecBuilder> modifier = Function.identity();

  private final long FIRST_TIMESTAMP_FORK = 9991L;

  @BeforeEach
  public void setup() {
    config = new StubGenesisConfigOptions();
    config.chainId(DEFAULT_CHAIN_ID);
    boolean isRevertReasonEnabled = false;
    builder =
        new ProtocolScheduleBuilder(
            config,
            Optional.of(DEFAULT_CHAIN_ID),
            ProtocolSpecAdapters.create(FIRST_TIMESTAMP_FORK, modifier),
            isRevertReasonEnabled,
            evmConfiguration,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
  }

  @Test
  public void emptySchedule() {
    final BlockHeader blockHeader = new BlockHeaderTestFixture().number(0L).buildHeader();
    Assertions.assertThatThrownBy(
            () -> new DefaultProtocolSchedule(CHAIN_ID).getByBlockHeader(blockHeader))
        .hasMessage("At least 1 milestone must be provided to the protocol schedule");
  }

  @Test
  public void conflictingSchedules() {
    final ProtocolSpec spec1 = mock(ProtocolSpec.class);
    final ProtocolSpec spec2 = mock(ProtocolSpec.class);
    when(spec1.getHardforkId()).thenReturn(FRONTIER);
    when(spec2.getHardforkId()).thenReturn(HOMESTEAD);

    final DefaultProtocolSchedule protocolSchedule = new DefaultProtocolSchedule(CHAIN_ID);
    protocolSchedule.putBlockNumberMilestone(0, spec1);
    protocolSchedule.putBlockNumberMilestone(0, spec2);
    assertThat(protocolSchedule.getByBlockHeader(header(0, 1L))).isSameAs(spec2);
  }

  @Test
  public void given_conflictingTimestampSchedules_lastMilestoneOverwritesPrevious() {
    final ProtocolSpec spec1 = mock(ProtocolSpec.class);
    final ProtocolSpec spec2 = mock(ProtocolSpec.class);
    final ProtocolSpec spec3 = mock(ProtocolSpec.class);

    when(spec1.getHardforkId()).thenReturn(FRONTIER);
    when(spec2.getHardforkId()).thenReturn(HOMESTEAD);
    when(spec3.getHardforkId()).thenReturn(BYZANTIUM);

    final DefaultProtocolSchedule protocolSchedule = new DefaultProtocolSchedule(CHAIN_ID);
    protocolSchedule.putTimestampMilestone(0, spec1);
    protocolSchedule.putTimestampMilestone(10, spec2);
    protocolSchedule.putTimestampMilestone(10, spec3);
    assertThat(protocolSchedule.getByBlockHeader(header(0, 10L))).isSameAs(spec3);
  }

  @Test
  public void given_conflictingBlockNumberAndTimestampSchedules_lastMilestoneOverwritesPrevious() {
    final ProtocolSpec spec1 = mock(ProtocolSpec.class);
    final ProtocolSpec spec2 = mock(ProtocolSpec.class);
    final ProtocolSpec spec3 = mock(ProtocolSpec.class);

    when(spec1.getHardforkId()).thenReturn(FRONTIER);
    when(spec2.getHardforkId()).thenReturn(HOMESTEAD);
    when(spec3.getHardforkId()).thenReturn(BYZANTIUM);

    final DefaultProtocolSchedule protocolSchedule = new DefaultProtocolSchedule(CHAIN_ID);
    protocolSchedule.putBlockNumberMilestone(0, spec1);
    protocolSchedule.putBlockNumberMilestone(10, spec2);
    protocolSchedule.putTimestampMilestone(10, spec3);
    assertThat(protocolSchedule.getByBlockHeader(header(10, 10L))).isSameAs(spec3);
  }

  @Test
  public void getByBlockHeader_getLatestTimestampFork() {
    config.grayGlacierBlock(0);
    config.shanghaiTime(FIRST_TIMESTAMP_FORK);
    config.cancunTime(FIRST_TIMESTAMP_FORK + 2);
    final ProtocolSchedule schedule = builder.createProtocolSchedule();

    assertThat(schedule.getByBlockHeader(header(2, FIRST_TIMESTAMP_FORK + 2)).getHardforkId())
        .isEqualTo(CANCUN);
    assertThat(schedule.getByBlockHeader(header(3, FIRST_TIMESTAMP_FORK + 3)).getHardforkId())
        .isEqualTo(CANCUN);
  }

  @Test
  public void getLatestProtocolSpec() {
    config.grayGlacierBlock(0);
    config.shanghaiTime(FIRST_TIMESTAMP_FORK);
    config.cancunTime(FIRST_TIMESTAMP_FORK + 2);
    final ProtocolSchedule schedule = builder.createProtocolSchedule();
    assertThat(schedule.getLatestProtocolSpec()).isNotEmpty();
    assertThat(schedule.getLatestProtocolSpec().get().fork().milestone())
        .isEqualTo(FIRST_TIMESTAMP_FORK + 2);
  }

  @Test
  public void getByBlockHeader_getIntermediateTimestampFork() {
    config.grayGlacierBlock(0);
    config.shanghaiTime(FIRST_TIMESTAMP_FORK);
    config.cancunTime(FIRST_TIMESTAMP_FORK + 2);
    final ProtocolSchedule schedule = builder.createProtocolSchedule();

    assertThat(schedule.getByBlockHeader(header(2, FIRST_TIMESTAMP_FORK)).getHardforkId())
        .isEqualTo(SHANGHAI);
    assertThat(schedule.getByBlockHeader(header(3, FIRST_TIMESTAMP_FORK + 1)).getHardforkId())
        .isEqualTo(SHANGHAI);
  }

  @Test
  public void getByBlockHeader_getLatestBlockNumberFork() {
    config.londonBlock(0);
    config.grayGlacierBlock(100);
    config.shanghaiTime(9992L);
    final ProtocolSchedule schedule = builder.createProtocolSchedule();

    assertThat(schedule.getByBlockHeader(header(100, 8881L)).getHardforkId())
        .isEqualTo(GRAY_GLACIER);
    assertThat(schedule.getByBlockHeader(header(200, 9991L)).getHardforkId())
        .isEqualTo(GRAY_GLACIER);
  }

  @Test
  public void getByBlockHeader_getIntermediateBlockNumberFork() {
    config.berlinBlock(0);
    config.londonBlock(50);
    config.grayGlacierBlock(100);
    config.shanghaiTime(9992L);
    final ProtocolSchedule schedule = builder.createProtocolSchedule();

    assertThat(schedule.getByBlockHeader(header(99, 8881L)).getHardforkId()).isEqualTo(LONDON);
  }

  @Test
  public void getForNextBlockHeader_shouldGetHeaderForNextBlockNumber() {
    final ProtocolSpec spec1 = mock(ProtocolSpec.class);
    final ProtocolSpec spec2 = mock(ProtocolSpec.class);

    when(spec1.getHardforkId()).thenReturn(FRONTIER);
    when(spec2.getHardforkId()).thenReturn(HOMESTEAD);

    final DefaultProtocolSchedule protocolSchedule = new DefaultProtocolSchedule(CHAIN_ID);
    protocolSchedule.putBlockNumberMilestone(0, spec1);
    protocolSchedule.putBlockNumberMilestone(10, spec2);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture().number(10L).timestamp(1L).buildHeader();
    final BlockHeader blockHeaderSpy = spy(blockHeader);
    final ProtocolSpec spec = protocolSchedule.getForNextBlockHeader(blockHeaderSpy, 2L);

    assertThat(spec).isEqualTo(spec2);
  }

  @Test
  public void getForNextBlockHeader_shouldGetHeaderForNextTimestamp() {
    final ProtocolSpec spec1 = mock(ProtocolSpec.class);
    final ProtocolSpec spec2 = mock(ProtocolSpec.class);

    when(spec1.getHardforkId()).thenReturn(FRONTIER);
    when(spec2.getHardforkId()).thenReturn(HOMESTEAD);

    final DefaultProtocolSchedule protocolSchedule = new DefaultProtocolSchedule(CHAIN_ID);
    protocolSchedule.putBlockNumberMilestone(0, spec1);
    protocolSchedule.putTimestampMilestone(9992, spec2);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture().number(1001L).timestamp(9991L).buildHeader();
    final BlockHeader blockHeaderSpy = spy(blockHeader);
    final ProtocolSpec spec = protocolSchedule.getForNextBlockHeader(blockHeaderSpy, 9992L);

    assertThat(spec).isEqualTo(spec2);
  }

  @Test
  public void isOnMilestoneBoundary() {
    config.berlinBlock(1L);
    config.londonBlock(2L);
    config.mergeNetSplitBlock(4L);
    config.shanghaiTime(FIRST_TIMESTAMP_FORK);
    config.cancunTime(9992L);
    config.experimentalEipsTime(9994L);
    final ProtocolSchedule protocolSchedule = builder.createProtocolSchedule();

    assertThat(protocolSchedule.isOnMilestoneBoundary(header(0L, 0L))).isEqualTo(true);

    // blockNumber schedule
    assertThat(protocolSchedule.isOnMilestoneBoundary(header(0L, 8880L))).isEqualTo(true);
    assertThat(protocolSchedule.isOnMilestoneBoundary(header(1L, 8881L))).isEqualTo(true);
    assertThat(protocolSchedule.isOnMilestoneBoundary(header(2L, 8882L))).isEqualTo(true);
    assertThat(protocolSchedule.isOnMilestoneBoundary(header(3L, 8883L))).isEqualTo(false);
    assertThat(protocolSchedule.isOnMilestoneBoundary(header(4L, 8884L))).isEqualTo(true);

    // timestamp schedule
    assertThat(protocolSchedule.isOnMilestoneBoundary(header(10L, FIRST_TIMESTAMP_FORK)))
        .isEqualTo(true);
    assertThat(protocolSchedule.isOnMilestoneBoundary(header(12L, 9992L))).isEqualTo(true);
    assertThat(protocolSchedule.isOnMilestoneBoundary(header(13L, 9993L))).isEqualTo(false);
    assertThat(protocolSchedule.isOnMilestoneBoundary(header(14L, 9994L))).isEqualTo(true);
  }

  private BlockHeader header(final long number, final long timestamp) {
    return new BlockHeaderTestFixture().number(number).timestamp(timestamp).buildHeader();
  }

  @Test
  public void copyConstructor_propagatesMilestonesMap() {
    final ProtocolSpec frontierSpec = mock(ProtocolSpec.class);
    when(frontierSpec.getHardforkId()).thenReturn(FRONTIER);

    final DefaultProtocolSchedule source = new DefaultProtocolSchedule(CHAIN_ID);
    source.putBlockNumberMilestone(0, frontierSpec);
    source.setMilestones(Map.of(SHANGHAI, 100L, CANCUN, 200L, PRAGUE, 300L));

    final DefaultProtocolSchedule copy = new TestProtocolScheduleCopy(source);

    assertThat(copy.milestoneFor(SHANGHAI)).contains(100L);
    assertThat(copy.milestoneFor(CANCUN)).contains(200L);
    assertThat(copy.milestoneFor(PRAGUE)).contains(300L);
    assertThat(copy.getChainId()).isEqualTo(CHAIN_ID);
    assertThat(copy.getByBlockHeader(header(0L, 0L))).isSameAs(frontierSpec);

    source.setMilestones(Map.of());

    assertThat(copy.milestoneFor(SHANGHAI)).contains(100L);
    assertThat(copy.milestoneFor(CANCUN)).contains(200L);
    assertThat(copy.milestoneFor(PRAGUE)).contains(300L);
  }

  private static final class TestProtocolScheduleCopy extends DefaultProtocolSchedule {
    TestProtocolScheduleCopy(final DefaultProtocolSchedule source) {
      super(source);
    }
  }
}
