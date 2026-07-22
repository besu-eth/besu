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

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.milestones.MilestoneDefinition;
import org.hyperledger.besu.ethereum.mainnet.milestones.MilestoneDefinitions;
import org.hyperledger.besu.ethereum.mainnet.milestones.MilestoneType;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtocolScheduleBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(ProtocolScheduleBuilder.class);
  private final GenesisConfigOptions config;
  private final Optional<BigInteger> defaultChainId;
  private final ProtocolSpecAdapters protocolSpecAdapters;
  private final boolean isRevertReasonEnabled;
  private final EvmConfiguration evmConfiguration;
  private final BadBlockManager badBlockManager;
  private final boolean isParallelTxProcessingEnabled;
  private final BalConfiguration balConfiguration;
  private final MetricsSystem metricsSystem;
  private final MiningConfiguration miningConfiguration;

  public ProtocolScheduleBuilder(
      final GenesisConfigOptions config,
      final Optional<BigInteger> defaultChainId,
      final ProtocolSpecAdapters protocolSpecAdapters,
      final boolean isRevertReasonEnabled,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final BadBlockManager badBlockManager,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    this.config = config;
    this.protocolSpecAdapters = protocolSpecAdapters;
    this.isRevertReasonEnabled = isRevertReasonEnabled;
    this.evmConfiguration = evmConfiguration;
    this.defaultChainId = defaultChainId;
    this.badBlockManager = badBlockManager;
    this.isParallelTxProcessingEnabled = isParallelTxProcessingEnabled;
    this.balConfiguration = balConfiguration;
    this.metricsSystem = metricsSystem;
    this.miningConfiguration = miningConfiguration;
  }

  public ProtocolSchedule createProtocolSchedule() {
    final Optional<BigInteger> chainId = config.getChainId().or(() -> defaultChainId);
    DefaultProtocolSchedule protocolSchedule = new DefaultProtocolSchedule(chainId);
    initSchedule(protocolSchedule);
    return protocolSchedule;
  }

  public void initSchedule(final ProtocolSchedule protocolSchedule) {

    final MainnetProtocolSpecFactory specFactory =
        new MainnetProtocolSpecFactory(
            protocolSchedule.getChainId(),
            isRevertReasonEnabled,
            config,
            evmConfiguration.overrides(
                config.getContractSizeLimit(), OptionalInt.empty(), config.getEvmStackSize()),
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem);

    final List<BuilderMapEntry> mileStones = createMilestones(specFactory);
    final Map<HardforkId, Long> completeMileStoneList = buildFullMilestoneMap(mileStones);
    protocolSchedule.setMilestones(completeMileStoneList);

    final NavigableMap<Long, BuilderMapEntry> builders = buildFlattenedMilestoneMap(mileStones);

    // At this stage, all milestones are flagged with the correct modifier, but ProtocolSpecs must
    // be inserted _AT_ the modifier block entry.
    if (!builders.isEmpty()) {
      protocolSpecAdapters.stream()
          .forEach(
              entry -> {
                final long modifierBlock = entry.getKey();
                final BuilderMapEntry parent =
                    Optional.ofNullable(builders.floorEntry(modifierBlock))
                        .orElse(builders.firstEntry())
                        .getValue();
                builders.put(
                    modifierBlock,
                    new BuilderMapEntry(
                        parent.hardforkId,
                        parent.milestoneType,
                        modifierBlock,
                        parent.builder(),
                        entry.getValue()));
              });
    }

    // Create the ProtocolSchedule, such that the Dao/fork milestones can be inserted
    builders
        .values()
        .forEach(
            e ->
                addProtocolSpec(
                    protocolSchedule,
                    e.milestoneType,
                    e.blockIdentifier(),
                    e.builder(),
                    e.modifier));

    // NOTE: It is assumed that Daofork blocks will not be used for private networks
    // as too many risks exist around inserting a protocol-spec between daoBlock and daoBlock+10.
    config
        .getDaoForkBlock()
        .ifPresent(
            daoBlockNumber -> {
              final BuilderMapEntry previousSpecBuilder =
                  builders.floorEntry(daoBlockNumber).getValue();
              final ProtocolSpec originalProtocolSpec =
                  getProtocolSpec(
                      protocolSchedule,
                      previousSpecBuilder.builder(),
                      previousSpecBuilder.modifier());
              addProtocolSpec(
                  protocolSchedule,
                  MilestoneType.BLOCK_NUMBER,
                  daoBlockNumber,
                  specFactory.daoRecoveryInitDefinition(),
                  protocolSpecAdapters.getModifierForBlock(daoBlockNumber));
              addProtocolSpec(
                  protocolSchedule,
                  MilestoneType.BLOCK_NUMBER,
                  daoBlockNumber + 1L,
                  specFactory.daoRecoveryTransitionDefinition(),
                  protocolSpecAdapters.getModifierForBlock(daoBlockNumber + 1L));
              // Return to the previous protocol spec after the dao fork has completed.
              protocolSchedule.putBlockNumberMilestone(daoBlockNumber + 10, originalProtocolSpec);
            });

    LOG.info("Protocol schedule created with milestones: {}", protocolSchedule.listMilestones());
  }

  private long validateForkOrder(
      final String forkName, final OptionalLong thisForkBlock, final long lastForkBlock) {
    final long referenceForkBlock = thisForkBlock.orElse(lastForkBlock);
    if (lastForkBlock > referenceForkBlock) {
      throw new RuntimeException(
          String.format(
              "Genesis Config Error: '%s' is scheduled for milestone %d but it must be on or after milestone %d.",
              forkName, thisForkBlock.getAsLong(), lastForkBlock));
    }
    return referenceForkBlock;
  }

  private NavigableMap<Long, BuilderMapEntry> buildFlattenedMilestoneMap(
      final List<BuilderMapEntry> mileStones) {
    return mileStones.stream()
        .collect(
            Collectors.toMap(
                BuilderMapEntry::blockIdentifier,
                b -> b,
                (existing, replacement) -> replacement,
                TreeMap::new));
  }

  private Map<HardforkId, Long> buildFullMilestoneMap(final List<BuilderMapEntry> mileStones) {
    return mileStones.stream()
        .collect(
            Collectors.toMap(
                b -> b.hardforkId,
                BuilderMapEntry::blockIdentifier,
                (existing, replacement) -> existing));
  }

  private List<BuilderMapEntry> createMilestones(final MainnetProtocolSpecFactory specFactory) {

    long lastForkBlock = 0;
    final List<Optional<BuilderMapEntry>> milestones = new ArrayList<>();
    final List<MilestoneDefinition> pendingDefinitions = new ArrayList<>();
    for (MilestoneDefinition milestoneDefinition :
        MilestoneDefinitions.createMilestoneDefinitions(specFactory, config)) {
      if (milestoneDefinition.blockNumberOrTimestamp().isPresent()) {
        pendingDefinitions.add(milestoneDefinition);

        final long thisForkBlock = milestoneDefinition.blockNumberOrTimestamp().getAsLong();
        for (final MilestoneDefinition pendingDefinition : pendingDefinitions) {
          validateForkOrder(
              pendingDefinition.hardforkId().name(),
              pendingDefinition.blockNumberOrTimestamp(),
              lastForkBlock);
          milestones.add(createMilestone(pendingDefinition, thisForkBlock));
        }
        pendingDefinitions.clear();
        lastForkBlock = thisForkBlock;
      } else {
        pendingDefinitions.add(milestoneDefinition);
      }
    }
    return milestones.stream().flatMap(Optional::stream).toList();
  }

  private Optional<BuilderMapEntry> createMilestone(
      final MilestoneDefinition milestoneDefinition, final long numberOrTimestamp) {
    return Optional.of(
        new BuilderMapEntry(
            milestoneDefinition.hardforkId(),
            milestoneDefinition.milestoneType(),
            numberOrTimestamp,
            milestoneDefinition.specBuilder().get(),
            protocolSpecAdapters.getModifierForBlock(numberOrTimestamp)));
  }

  private ProtocolSpec getProtocolSpec(
      final ProtocolSchedule protocolSchedule,
      final ProtocolSpecBuilder definition,
      final Function<ProtocolSpecBuilder, ProtocolSpecBuilder> modifier) {
    definition.badBlocksManager(badBlockManager);

    return modifier.apply(definition).build(protocolSchedule);
  }

  private void addProtocolSpec(
      final ProtocolSchedule protocolSchedule,
      final MilestoneType milestoneType,
      final long blockNumberOrTimestamp,
      final ProtocolSpecBuilder definition,
      final Function<ProtocolSpecBuilder, ProtocolSpecBuilder> modifier) {

    switch (milestoneType) {
      case BLOCK_NUMBER ->
          protocolSchedule.putBlockNumberMilestone(
              blockNumberOrTimestamp, getProtocolSpec(protocolSchedule, definition, modifier));
      case TIMESTAMP ->
          protocolSchedule.putTimestampMilestone(
              blockNumberOrTimestamp, getProtocolSpec(protocolSchedule, definition, modifier));
      default ->
          throw new IllegalStateException(
              "Unexpected milestoneType: "
                  + milestoneType
                  + " for milestone: "
                  + blockNumberOrTimestamp);
    }
  }

  private record BuilderMapEntry(
      HardforkId hardforkId,
      MilestoneType milestoneType,
      long blockIdentifier,
      ProtocolSpecBuilder builder,
      Function<ProtocolSpecBuilder, ProtocolSpecBuilder> modifier) {}

  public Optional<BigInteger> getDefaultChainId() {
    return defaultChainId;
  }
}
