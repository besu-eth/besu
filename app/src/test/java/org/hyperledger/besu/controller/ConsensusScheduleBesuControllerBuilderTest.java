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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.MigratingConsensusContext;
import org.hyperledger.besu.consensus.common.MigratingMiningCoordinator;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftMiningCoordinator;
import org.hyperledger.besu.ethereum.ConsensusContext;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.config.SubProtocolConfiguration;
import org.hyperledger.besu.ethereum.p2p.network.ProtocolManager;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConsensusScheduleBesuControllerBuilderTest {
  private @Mock BiFunction<
          NavigableSet<ForkSpec<ProtocolSchedule>>, Optional<BigInteger>, ProtocolSchedule>
      combinedProtocolScheduleFactory;
  private @Mock GenesisConfig genesisConfig;
  private @Mock BesuControllerBuilder besuControllerBuilder1;
  private @Mock BesuControllerBuilder besuControllerBuilder2;
  private @Mock BesuControllerBuilder besuControllerBuilder3;
  private @Mock ProtocolSchedule protocolSchedule1;
  private @Mock ProtocolSchedule protocolSchedule2;
  private @Mock ProtocolSchedule protocolSchedule3;
  private @Mock MiningCoordinator miningCoordinator1;
  private @Mock BftMiningCoordinator miningCoordinator2;

  @Test
  public void mustProvideNonNullConsensusScheduleWhenInstantiatingNew() {
    assertThatThrownBy(() -> new ConsensusScheduleBesuControllerBuilder(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("BesuControllerBuilder schedule can't be null");
  }

  @Test
  public void mustProvideNonEmptyConsensusScheduleWhenInstantiatingNew() {
    assertThatThrownBy(() -> new ConsensusScheduleBesuControllerBuilder(Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("BesuControllerBuilder schedule can't be empty");
  }

  @Test
  public void mustCreateCombinedProtocolScheduleUsingProtocolSchedulesOrderedByBlock() {
    // Use an ordered map with keys in the incorrect order so that we can show that set is created
    // with the correct order
    final Map<Long, BesuControllerBuilder> besuControllerBuilderSchedule = new TreeMap<>();
    besuControllerBuilderSchedule.put(30L, besuControllerBuilder3);
    besuControllerBuilderSchedule.put(10L, besuControllerBuilder2);
    besuControllerBuilderSchedule.put(0L, besuControllerBuilder1);

    when(besuControllerBuilder1.createProtocolSchedule()).thenReturn(protocolSchedule1);
    when(besuControllerBuilder2.createProtocolSchedule()).thenReturn(protocolSchedule2);
    when(besuControllerBuilder3.createProtocolSchedule()).thenReturn(protocolSchedule3);

    final StubGenesisConfigOptions genesisConfigOptions = new StubGenesisConfigOptions();
    genesisConfigOptions.chainId(BigInteger.TEN);

    final ConsensusScheduleBesuControllerBuilder consensusScheduleBesuControllerBuilder =
        new ConsensusScheduleBesuControllerBuilder(
            besuControllerBuilderSchedule, combinedProtocolScheduleFactory);
    when(genesisConfig.getConfigOptions()).thenReturn(genesisConfigOptions);
    consensusScheduleBesuControllerBuilder.genesisConfig(genesisConfig);
    consensusScheduleBesuControllerBuilder.createProtocolSchedule();

    final NavigableSet<ForkSpec<ProtocolSchedule>> expectedProtocolSchedulesSpecs =
        new TreeSet<>(ForkSpec.COMPARATOR);
    expectedProtocolSchedulesSpecs.add(new ForkSpec<>(0L, protocolSchedule1));
    expectedProtocolSchedulesSpecs.add(new ForkSpec<>(10L, protocolSchedule2));
    expectedProtocolSchedulesSpecs.add(new ForkSpec<>(30L, protocolSchedule3));
    Mockito.verify(combinedProtocolScheduleFactory)
        .apply(expectedProtocolSchedulesSpecs, Optional.of(BigInteger.TEN));
  }

  @Test
  public void createsMigratingMiningCoordinator() {
    final Map<Long, BesuControllerBuilder> consensusSchedule =
        Map.of(0L, besuControllerBuilder1, 5L, besuControllerBuilder2);

    when(besuControllerBuilder1.createMiningCoordinator(any(), any(), any(), any(), any(), any()))
        .thenReturn(miningCoordinator1);
    when(besuControllerBuilder2.createMiningCoordinator(any(), any(), any(), any(), any(), any()))
        .thenReturn(miningCoordinator2);
    final ProtocolContext mockProtocolContext = mock(ProtocolContext.class);
    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(mockProtocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadHeader()).thenReturn(mock(BlockHeader.class));

    final ConsensusScheduleBesuControllerBuilder builder =
        new ConsensusScheduleBesuControllerBuilder(consensusSchedule);
    final MiningCoordinator miningCoordinator =
        builder.createMiningCoordinator(
            protocolSchedule1,
            mockProtocolContext,
            mock(TransactionPool.class),
            mock(MiningConfiguration.class),
            mock(SyncState.class),
            mock(EthProtocolManager.class));

    assertThat(miningCoordinator).isInstanceOf(MigratingMiningCoordinator.class);
    final MigratingMiningCoordinator migratingMiningCoordinator =
        (MigratingMiningCoordinator) miningCoordinator;

    SoftAssertions.assertSoftly(
        (softly) -> {
          softly
              .assertThat(
                  migratingMiningCoordinator
                      .getMiningCoordinatorSchedule()
                      .getFork(0L, 0)
                      .getValue())
              .isSameAs(miningCoordinator1);
          softly
              .assertThat(
                  migratingMiningCoordinator
                      .getMiningCoordinatorSchedule()
                      .getFork(4L, 0)
                      .getValue())
              .isSameAs(miningCoordinator1);
          softly
              .assertThat(
                  migratingMiningCoordinator
                      .getMiningCoordinatorSchedule()
                      .getFork(5L, 0)
                      .getValue())
              .isSameAs(miningCoordinator2);
          softly
              .assertThat(
                  migratingMiningCoordinator
                      .getMiningCoordinatorSchedule()
                      .getFork(6L, 0)
                      .getValue())
              .isSameAs(miningCoordinator2);
        });
  }

  @Test
  public void createsMigratingContext() {
    final ConsensusContext context1 = mock(ConsensusContext.class);
    final ConsensusContext context2 = mock(ConsensusContext.class);

    final Map<Long, BesuControllerBuilder> besuControllerBuilderSchedule = new TreeMap<>();
    besuControllerBuilderSchedule.put(0L, besuControllerBuilder1);
    besuControllerBuilderSchedule.put(10L, besuControllerBuilder2);

    when(besuControllerBuilder1.createConsensusContext(any(), any(), any())).thenReturn(context1);
    when(besuControllerBuilder2.createConsensusContext(any(), any(), any())).thenReturn(context2);

    final ConsensusScheduleBesuControllerBuilder controllerBuilder =
        new ConsensusScheduleBesuControllerBuilder(besuControllerBuilderSchedule);
    final ConsensusContext consensusContext =
        controllerBuilder.createConsensusContext(
            mock(Blockchain.class), mock(WorldStateArchive.class), mock(ProtocolSchedule.class));

    assertThat(consensusContext).isInstanceOf(MigratingConsensusContext.class);
    final MigratingConsensusContext migratingConsensusContext =
        (MigratingConsensusContext) consensusContext;

    final ForksSchedule<ConsensusContext> contextSchedule =
        migratingConsensusContext.getConsensusContextSchedule();

    final NavigableSet<ForkSpec<ConsensusContext>> expectedConsensusContextSpecs =
        new TreeSet<>(ForkSpec.COMPARATOR);
    expectedConsensusContextSpecs.add(new ForkSpec<>(0L, context1));
    expectedConsensusContextSpecs.add(new ForkSpec<>(10L, context2));
    assertThat(contextSchedule.getForks()).isEqualTo(expectedConsensusContextSpecs);

    assertThat(contextSchedule.getFork(0, 0).getValue()).isSameAs(context1);
    assertThat(contextSchedule.getFork(1, 0).getValue()).isSameAs(context1);
    assertThat(contextSchedule.getFork(9, 0).getValue()).isSameAs(context1);
    assertThat(contextSchedule.getFork(10, 0).getValue()).isSameAs(context2);
    assertThat(contextSchedule.getFork(11, 0).getValue()).isSameAs(context2);
  }

  @Test
  public void createsSubProtocolConfigurationWithAllSubProtocols() {
    final Map<Long, BesuControllerBuilder> consensusSchedule =
        Map.of(0L, besuControllerBuilder1, 50L, besuControllerBuilder2);

    final SubProtocol subProtocol1 = mock(SubProtocol.class);
    final SubProtocol subProtocol2 = mock(SubProtocol.class);
    when(subProtocol1.getName()).thenReturn("IBF");
    when(subProtocol2.getName()).thenReturn("istanbul");

    final SubProtocolConfiguration config1 = new SubProtocolConfiguration();
    config1.withSubProtocol(subProtocol1, mock(ProtocolManager.class));

    final SubProtocolConfiguration config2 = new SubProtocolConfiguration();
    config2.withSubProtocol(subProtocol2, mock(ProtocolManager.class));

    when(besuControllerBuilder1.createSubProtocolConfiguration(any(), any())).thenReturn(config1);
    when(besuControllerBuilder2.createSubProtocolConfiguration(any(), any())).thenReturn(config2);

    final ConsensusScheduleBesuControllerBuilder builder =
        new ConsensusScheduleBesuControllerBuilder(consensusSchedule);

    final SubProtocolConfiguration result =
        builder.createSubProtocolConfiguration(mock(EthProtocolManager.class), Optional.empty());

    assertThat(result.getSubProtocols()).hasSize(2);
    assertThat(result.getSubProtocols())
        .extracting(SubProtocol::getName)
        .containsExactlyInAnyOrder("IBF", "istanbul");
  }
}
