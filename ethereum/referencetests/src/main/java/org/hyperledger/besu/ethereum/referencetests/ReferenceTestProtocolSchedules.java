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

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.precompile.KZGPointEvalPrecompiledContract;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Loads all available protocol schedules into memory and lets users select the appropriate one.
 * Beware of the high memory usage and object allocation cost since **all** protocol schedules will
 * be created and initialized for each created instance. This might cause your tests to slow down.
 */
public class ReferenceTestProtocolSchedules {

  private static final BigInteger CHAIN_ID = BigInteger.ONE;

  private static final List<String> SPECS_PRIOR_TO_DELETING_EMPTY_ACCOUNTS =
      Arrays.asList("Frontier", "Homestead", "EIP150");

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

  public static ReferenceTestProtocolSchedules create() {
    return create(new StubGenesisConfigOptions(), EvmConfiguration.DEFAULT);
  }

  public static ReferenceTestProtocolSchedules create(final EvmConfiguration evmConfiguration) {
    return create(new StubGenesisConfigOptions(), evmConfiguration);
  }

  public static ReferenceTestProtocolSchedules create(
      final StubGenesisConfigOptions genesisStub, final EvmConfiguration evmConfiguration) {
    // the following schedules activate EIP-1559, but may have non-default
    if (genesisStub.getBaseFeePerGas().isEmpty()) {
      genesisStub.baseFeePerGas(0x0a);
    }
    // also load KZG file for mainnet
    KZGPointEvalPrecompiledContract.init();
    return new ReferenceTestProtocolSchedules(
        Map.ofEntries(
                Map.entry("Frontier", createSchedule(genesisStub.clone(), evmConfiguration)),
                Map.entry(
                    "FrontierToHomesteadAt5",
                    createSchedule(genesisStub.clone().homesteadBlock(5), evmConfiguration)),
                Map.entry(
                    "Homestead",
                    createSchedule(genesisStub.clone().homesteadBlock(0), evmConfiguration)),
                Map.entry(
                    "HomesteadToEIP150At5",
                    createSchedule(
                        genesisStub.clone().homesteadBlock(0).eip150Block(5), evmConfiguration)),
                Map.entry(
                    "HomesteadToDaoAt5",
                    createSchedule(
                        genesisStub.clone().homesteadBlock(0).daoForkBlock(5), evmConfiguration)),
                Map.entry(
                    "EIP150", createSchedule(genesisStub.clone().eip150Block(0), evmConfiguration)),
                Map.entry(
                    "EIP158", createSchedule(genesisStub.clone().eip158Block(0), evmConfiguration)),
                Map.entry(
                    "EIP158ToByzantiumAt5",
                    createSchedule(
                        genesisStub.clone().eip158Block(0).byzantiumBlock(5), evmConfiguration)),
                Map.entry(
                    "Byzantium",
                    createSchedule(genesisStub.clone().byzantiumBlock(0), evmConfiguration)),
                Map.entry(
                    "Constantinople",
                    createSchedule(genesisStub.clone().constantinopleBlock(0), evmConfiguration)),
                Map.entry(
                    "ConstantinopleFix",
                    createSchedule(genesisStub.clone().petersburgBlock(0), evmConfiguration)),
                Map.entry(
                    "Petersburg",
                    createSchedule(genesisStub.clone().petersburgBlock(0), evmConfiguration)),
                Map.entry(
                    "Istanbul",
                    createSchedule(genesisStub.clone().istanbulBlock(0), evmConfiguration)),
                Map.entry(
                    "MuirGlacier",
                    createSchedule(genesisStub.clone().muirGlacierBlock(0), evmConfiguration)),
                Map.entry(
                    "Berlin", createSchedule(genesisStub.clone().berlinBlock(0), evmConfiguration)),
                Map.entry(
                    "London", createSchedule(genesisStub.clone().londonBlock(0), evmConfiguration)),
                Map.entry(
                    "ArrowGlacier",
                    createSchedule(genesisStub.clone().arrowGlacierBlock(0), evmConfiguration)),
                Map.entry(
                    "GrayGlacier",
                    createSchedule(genesisStub.clone().grayGlacierBlock(0), evmConfiguration)),
                Map.entry(
                    "Merge",
                    createSchedule(genesisStub.clone().mergeNetSplitBlock(0), evmConfiguration)),
                Map.entry(
                    "Paris",
                    createSchedule(genesisStub.clone().mergeNetSplitBlock(0), evmConfiguration)),
                Map.entry(
                    "Shanghai",
                    createSchedule(genesisStub.clone().shanghaiTime(0), evmConfiguration)),
                Map.entry(
                    "ShanghaiToCancunAtTime15k",
                    createSchedule(
                        genesisStub.clone().shanghaiTime(0).cancunTime(15000), evmConfiguration)),
                Map.entry(
                    "Cancun", createSchedule(genesisStub.clone().cancunTime(0), evmConfiguration)),
                Map.entry(
                    "CancunToPragueAtTime15k",
                    createSchedule(
                        genesisStub.clone().cancunTime(0).pragueTime(15000), evmConfiguration)),
                Map.entry(
                    "Prague", createSchedule(genesisStub.clone().pragueTime(0), evmConfiguration)),
                Map.entry(
                    "Osaka", createSchedule(genesisStub.clone().osakaTime(0), evmConfiguration)),
                Map.entry(
                    "Amsterdam",
                    createSchedule(genesisStub.clone().amsterdamTime(0), evmConfiguration)),
                Map.entry(
                    "Bogota",
                    createSchedule(genesisStub.clone().futureEipsTime(0), evmConfiguration)),
                Map.entry(
                    "Polis",
                    createSchedule(genesisStub.clone().futureEipsTime(0), evmConfiguration)),
                Map.entry(
                    "Bangkok",
                    createSchedule(genesisStub.clone().futureEipsTime(0), evmConfiguration)),
                Map.entry(
                    "Future_EIPs",
                    createSchedule(genesisStub.clone().futureEipsTime(0), evmConfiguration)),
                Map.entry(
                    "Experimental_EIPs",
                    createSchedule(genesisStub.clone().experimentalEipsTime(0), evmConfiguration)))
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
    return schedules.get(name.toLowerCase(Locale.ROOT));
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
      final GenesisConfigOptions options, final EvmConfiguration evmConfiguration) {
    return new ProtocolScheduleBuilder(
            options,
            Optional.of(CHAIN_ID),
            ProtocolSpecAdapters.create(0, Function.identity()),
            false,
            evmConfiguration,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem())
        .createProtocolSchedule();
  }

  public static boolean shouldClearEmptyAccounts(final String fork) {
    return !SPECS_PRIOR_TO_DELETING_EMPTY_ACCOUNTS.contains(fork);
  }

  public static boolean supportsBlockBuilding(final String fork) {
    return !FORKS_WITHOUT_BLOCK_BUILDING.contains(fork.toLowerCase(Locale.ROOT));
  }
}
