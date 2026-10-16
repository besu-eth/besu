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
package org.hyperledger.besu.ethereum.referencetests;

import org.hyperledger.besu.config.BlobScheduleOptions;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionPathListener;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.precompile.KZGPointEvalPrecompiledContract;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Loads all available protocol schedules into memory and lets users select the appropriate one.
 * Beware of the high memory usage and object allocation cost since **all** protocol schedules will
 * be created and initialized for each created instance. This might cause your tests to slow down.
 */
public class ReferenceTestProtocolSchedules {

  private static final BigInteger CHAIN_ID = BigInteger.ONE;

  private static final Set<String> SPECS_PRIOR_TO_DELETING_EMPTY_ACCOUNTS =
      Set.of("frontier", "homestead", "eip150");

  /**
   * Upgrade names the stable fixtures use for forks these schedules know by their EIP names.
   * Resolved before every lookup, so a fixture naming either gets the same schedule and rules.
   */
  private static final Map<String, String> FORK_ALIASES =
      Map.of("tangerinewhistle", "eip150", "spuriousdragon", "eip158");

  private static final Set<String> FORKS_WITHOUT_BLOCK_BUILDING =
      Set.of(
          "frontier",
          "frontiertohomesteadat5",
          "homestead",
          "homesteadtoeip150at5",
          "homesteadtodaoat5",
          "eip150",
          "eip158",
          "eip158tobyzantiumat5");

  /**
   * Guarded by {@link #cached(EvmConfiguration, BlobScheduleOptions, boolean,
   * BlockExecutionPathListener)}, which is synchronized.
   */
  private static final Map<CacheKey, ReferenceTestProtocolSchedules> CACHED_SCHEDULES =
      new HashMap<>();

  private record CacheKey(
      EvmConfiguration evmConfiguration,
      ObjectNode blobSchedule,
      boolean isParallelTxProcessingEnabled,
      BlockExecutionPathListener executionPathListener) {}

  public static ReferenceTestProtocolSchedules create() {
    return create(new StubGenesisConfigOptions(), evmConfiguration());
  }

  /**
   * The EVM configuration the reference tests run with: EVM v2 when the system property test.evm.v2
   * is set to true, the default configuration otherwise.
   *
   * @return the EVM configuration
   */
  public static EvmConfiguration evmConfiguration() {
    return Boolean.getBoolean("test.evm.v2")
        ? new EvmConfiguration(
            EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
            EvmConfiguration.DEFAULT.worldUpdaterMode(),
            EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
            true)
        : EvmConfiguration.DEFAULT;
  }

  public static ReferenceTestProtocolSchedules create(final EvmConfiguration evmConfiguration) {
    return create(new StubGenesisConfigOptions(), evmConfiguration);
  }

  /**
   * Creates the reference-test schedules with a fixture-supplied blob schedule applied to every
   * fork (used by engine/blockchain tests on devnets whose blob target/max differ from defaults).
   *
   * @param evmConfiguration the EVM configuration
   * @param blobScheduleOptions the blob schedule from the fixture config, or null for defaults
   * @param isParallelTxProcessingEnabled build the parallel block processor a Bonsai node runs by
   *     default, rather than the sequential one
   * @param executionPathListener told which executor runs each block
   * @return the schedules
   */
  public static ReferenceTestProtocolSchedules create(
      final EvmConfiguration evmConfiguration,
      final BlobScheduleOptions blobScheduleOptions,
      final boolean isParallelTxProcessingEnabled,
      final BlockExecutionPathListener executionPathListener) {
    final StubGenesisConfigOptions genesisStub = new StubGenesisConfigOptions();
    if (blobScheduleOptions != null) {
      genesisStub.blobScheduleOptions(blobScheduleOptions);
    }
    return create(
        genesisStub, evmConfiguration, isParallelTxProcessingEnabled, executionPathListener);
  }

  /**
   * As {@link #create(EvmConfiguration, BlobScheduleOptions, boolean, BlockExecutionPathListener)},
   * but built once per distinct blob schedule and shared by every caller. Fixture runners hand each
   * fixture's own blob schedule in, and a fixture tree holds only a handful of distinct ones, so
   * building a schedule set per fixture is pure waste.
   *
   * <p>Synchronized rather than a {@code ConcurrentHashMap.computeIfAbsent}: the map holds a key
   * per distinct blob schedule, so per-key serialisation would still let several threads into
   * {@code create()} at once, and {@code create()} initialises the KZG trusted setup, which is
   * process-wide state guarded by a {@code compareAndSet} that lets the losing thread proceed
   * before the setup is loaded. The check-then-act has to be atomic across keys, not merely
   * visible.
   *
   * <p>The key is the blob schedule's whole config root rather than {@link
   * BlobScheduleOptions#asMap()}, which enumerates only the fork keys this Besu version has a
   * getter for and would silently collide two schedules differing only outside that set — the
   * devnet forks these runners exist to test. {@link
   * com.fasterxml.jackson.databind.node.ObjectNode} equality is by content and insensitive to field
   * order.
   *
   * @param evmConfiguration the EVM configuration
   * @param blobScheduleOptions the blob schedule from the fixture config, or null for defaults
   * @param isParallelTxProcessingEnabled build the parallel block processor rather than the
   *     sequential one; the two are cached apart
   * @param executionPathListener told which executor runs each block; each listener gets its own
   *     schedules
   * @return the schedules
   */
  public static synchronized ReferenceTestProtocolSchedules cached(
      final EvmConfiguration evmConfiguration,
      final BlobScheduleOptions blobScheduleOptions,
      final boolean isParallelTxProcessingEnabled,
      final BlockExecutionPathListener executionPathListener) {
    final ObjectNode key =
        blobScheduleOptions == null
            ? JsonUtil.createEmptyObjectNode()
            : blobScheduleOptions.getConfigRoot().deepCopy();
    return CACHED_SCHEDULES.computeIfAbsent(
        new CacheKey(evmConfiguration, key, isParallelTxProcessingEnabled, executionPathListener),
        ignored ->
            create(
                evmConfiguration,
                blobScheduleOptions,
                isParallelTxProcessingEnabled,
                executionPathListener));
  }

  public static ReferenceTestProtocolSchedules create(
      final StubGenesisConfigOptions genesisStub, final EvmConfiguration evmConfiguration) {
    return create(genesisStub, evmConfiguration, false, BlockExecutionPathListener.NONE);
  }

  private static ReferenceTestProtocolSchedules create(
      final StubGenesisConfigOptions genesisStub,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BlockExecutionPathListener executionPathListener) {
    // the following schedules activate EIP-1559, but may have non-default
    if (genesisStub.getBaseFeePerGas().isEmpty()) {
      genesisStub.baseFeePerGas(0x0a);
    }
    // also load KZG file for mainnet
    KZGPointEvalPrecompiledContract.init();
    final Function<GenesisConfigOptions, ProtocolSchedule> schedule =
        options ->
            createSchedule(
                options, evmConfiguration, isParallelTxProcessingEnabled, executionPathListener);
    return new ReferenceTestProtocolSchedules(
        Map.ofEntries(
                Map.entry("Frontier", schedule.apply(genesisStub.clone())),
                Map.entry(
                    "FrontierToHomesteadAt5",
                    schedule.apply(genesisStub.clone().homesteadBlock(5))),
                Map.entry("Homestead", schedule.apply(genesisStub.clone().homesteadBlock(0))),
                Map.entry(
                    "HomesteadToEIP150At5",
                    schedule.apply(genesisStub.clone().homesteadBlock(0).eip150Block(5))),
                Map.entry(
                    "HomesteadToDaoAt5",
                    schedule.apply(genesisStub.clone().homesteadBlock(0).daoForkBlock(5))),
                Map.entry("EIP150", schedule.apply(genesisStub.clone().eip150Block(0))),
                Map.entry("EIP158", schedule.apply(genesisStub.clone().eip158Block(0))),
                Map.entry(
                    "EIP158ToByzantiumAt5",
                    schedule.apply(genesisStub.clone().eip158Block(0).byzantiumBlock(5))),
                Map.entry("Byzantium", schedule.apply(genesisStub.clone().byzantiumBlock(0))),
                Map.entry(
                    "Constantinople", schedule.apply(genesisStub.clone().constantinopleBlock(0))),
                Map.entry(
                    "ConstantinopleFix", schedule.apply(genesisStub.clone().petersburgBlock(0))),
                Map.entry("Petersburg", schedule.apply(genesisStub.clone().petersburgBlock(0))),
                Map.entry("Istanbul", schedule.apply(genesisStub.clone().istanbulBlock(0))),
                Map.entry("MuirGlacier", schedule.apply(genesisStub.clone().muirGlacierBlock(0))),
                Map.entry("Berlin", schedule.apply(genesisStub.clone().berlinBlock(0))),
                Map.entry("London", schedule.apply(genesisStub.clone().londonBlock(0))),
                Map.entry("ArrowGlacier", schedule.apply(genesisStub.clone().arrowGlacierBlock(0))),
                Map.entry("GrayGlacier", schedule.apply(genesisStub.clone().grayGlacierBlock(0))),
                Map.entry("Merge", schedule.apply(genesisStub.clone().mergeNetSplitBlock(0))),
                Map.entry("Paris", schedule.apply(genesisStub.clone().mergeNetSplitBlock(0))),
                Map.entry(
                    "ParisToShanghaiAtTime15k",
                    schedule.apply(genesisStub.clone().mergeNetSplitBlock(0).shanghaiTime(15000))),
                Map.entry("Shanghai", schedule.apply(genesisStub.clone().shanghaiTime(0))),
                Map.entry(
                    "ShanghaiToCancunAtTime15k",
                    schedule.apply(genesisStub.clone().shanghaiTime(0).cancunTime(15000))),
                Map.entry("Cancun", schedule.apply(genesisStub.clone().cancunTime(0))),
                Map.entry(
                    "CancunToPragueAtTime15k",
                    schedule.apply(genesisStub.clone().cancunTime(0).pragueTime(15000))),
                Map.entry("Prague", schedule.apply(genesisStub.clone().pragueTime(0))),
                // Forks left without an activation time are folded into the first configured
                // milestone, so each entry only needs to give times to the forks that have to be
                // told apart (the fork under test and, for transitions, the one it starts from).
                Map.entry(
                    "PragueToOsakaAtTime15k",
                    schedule.apply(genesisStub.clone().pragueTime(0).osakaTime(15000))),
                Map.entry("Osaka", schedule.apply(genesisStub.clone().pragueTime(0).osakaTime(0))),
                Map.entry(
                    "OsakaToBPO1AtTime15k",
                    schedule.apply(genesisStub.clone().pragueTime(0).osakaTime(0).bpo1Time(15000))),
                Map.entry(
                    "BPO1ToBPO2AtTime15k",
                    schedule.apply(
                        genesisStub
                            .clone()
                            .pragueTime(0)
                            .osakaTime(0)
                            .bpo1Time(0)
                            .bpo2Time(15000))),
                Map.entry(
                    "BPO2ToBPO3AtTime15k",
                    schedule.apply(
                        genesisStub
                            .clone()
                            .pragueTime(0)
                            .osakaTime(0)
                            .bpo1Time(0)
                            .bpo2Time(0)
                            .bpo3Time(15000))),
                Map.entry(
                    "BPO3ToBPO4AtTime15k",
                    schedule.apply(
                        genesisStub
                            .clone()
                            .pragueTime(0)
                            .osakaTime(0)
                            .bpo1Time(0)
                            .bpo2Time(0)
                            .bpo3Time(0)
                            .bpo4Time(15000))),
                Map.entry(
                    "Amsterdam",
                    schedule.apply(
                        genesisStub
                            .clone()
                            .pragueTime(0)
                            .osakaTime(0)
                            .bpo1Time(0)
                            .bpo2Time(0)
                            .amsterdamTime(0))),
                Map.entry(
                    "BPO2ToAmsterdamAtTime15k",
                    schedule.apply(
                        genesisStub
                            .clone()
                            .pragueTime(0)
                            .osakaTime(0)
                            .bpo1Time(0)
                            .bpo2Time(0)
                            .amsterdamTime(15000))),
                Map.entry(
                    "Bogota",
                    schedule.apply(
                        genesisStub
                            .clone()
                            .pragueTime(0)
                            .osakaTime(0)
                            .bpo1Time(0)
                            .bpo2Time(0)
                            .amsterdamTime(0)
                            .bogotaTime(0))),
                Map.entry(
                    "AmsterdamToBogotaAtTime15k",
                    schedule.apply(
                        genesisStub
                            .clone()
                            .pragueTime(0)
                            .osakaTime(0)
                            .bpo1Time(0)
                            .bpo2Time(0)
                            .amsterdamTime(0)
                            .bogotaTime(15000))),
                Map.entry("Polis", schedule.apply(genesisStub.clone().futureEipsTime(0))),
                Map.entry("Bangkok", schedule.apply(genesisStub.clone().futureEipsTime(0))),
                Map.entry("Future_EIPs", schedule.apply(genesisStub.clone().futureEipsTime(0))),
                Map.entry(
                    "Experimental_EIPs",
                    schedule.apply(genesisStub.clone().experimentalEipsTime(0))))
            .entrySet()
            .stream()
            .map(e -> Map.entry(e.getKey().toLowerCase(Locale.ROOT), e.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  private final Map<String, ProtocolSchedule> schedules;

  private ReferenceTestProtocolSchedules(final Map<String, ProtocolSchedule> schedules) {
    this.schedules = schedules;
  }

  public ProtocolSchedule getByName(final String name) {
    return schedules.get(canonicalForkName(name));
  }

  /**
   * The name every lookup here keys on: lower case, with upgrade names resolved to the EIP names
   * the schedules are registered under.
   */
  private static String canonicalForkName(final String fork) {
    final String lowerCase = fork.toLowerCase(Locale.ROOT);
    return FORK_ALIASES.getOrDefault(lowerCase, lowerCase);
  }

  /**
   * Whether two fork names select the same schedule, so a filter naming a fork by its upgrade name
   * matches a fixture naming it by its EIP name, and the other way round.
   *
   * @param fork a fork name
   * @param other another fork name
   * @return true if both resolve to the same fork
   */
  public static boolean isSameFork(final String fork, final String other) {
    return canonicalForkName(fork).equals(canonicalForkName(other));
  }

  public ProtocolSpec geSpecByName(final String name) {
    ProtocolSchedule schedule = getByName(name);
    if (schedule == null) {
      return null;
    }
    BlockHeader header =
        new BlockHeaderTestFixture().timestamp(Long.MAX_VALUE).number(Long.MAX_VALUE).buildHeader();
    return schedule.getByBlockHeader(header);
  }

  private static ProtocolSchedule createSchedule(
      final GenesisConfigOptions options,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BlockExecutionPathListener executionPathListener) {
    return new ProtocolScheduleBuilder(
            options,
            Optional.of(CHAIN_ID),
            ProtocolSpecAdapters.create(0, Function.identity()),
            false,
            evmConfiguration,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            isParallelTxProcessingEnabled,
            ImmutableBalConfiguration.builder()
                .executionPathListener(executionPathListener)
                .build(),
            new NoOpMetricsSystem())
        .createProtocolSchedule();
  }

  public static boolean shouldClearEmptyAccounts(final String fork) {
    return !SPECS_PRIOR_TO_DELETING_EMPTY_ACCOUNTS.contains(canonicalForkName(fork));
  }

  public static boolean supportsBlockBuilding(final String fork) {
    return !FORKS_WITHOUT_BLOCK_BUILDING.contains(canonicalForkName(fork));
  }
}
