/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.evmtool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BOGOTA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO1;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO2;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO3;

import org.hyperledger.besu.config.BlobScheduleOptions;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BlobFeeMarket;
import org.hyperledger.besu.ethereum.referencetests.BlockchainReferenceTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.EngineTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The BPO networks block-test and engine-test run: each resolves to a schedule that keeps its
 * fork's rules and applies the fixture's blob schedule for the network from its activation on.
 */
class FixtureBpoScenarioTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final long BLOB_GAS_PER_BLOB = 1L << 17;

  private record Blobs(int target, int max, long updateFraction) {
    String json() {
      return String.format(
          "{\"target\": \"0x%x\", \"max\": \"0x%x\", \"baseFeeUpdateFraction\": \"0x%x\"}",
          target, max, updateFraction);
    }
  }

  private static final Blobs BPO1_BLOBS = new Blobs(10, 15, 8346193);
  private static final Blobs BPO2_BLOBS = new Blobs(14, 21, 11684671);
  // The EEST scenarios scale BPO2's target by 3/2 or 2/3, and the max and update fraction with it.
  private static final Blobs INCREASE = new Blobs(21, 32, 17527006);
  private static final Blobs DECREASE = new Blobs(9, 14, 7511574);

  /**
   * A BPO network, the fixture blob-schedule key for its own blobs, when they activate, and the
   * fork the spec from then on reports. Before a 15k transition the spec is Amsterdam's.
   */
  private record Network(
      String name, String blobKey, long activation, HardforkId hardfork, Blobs blobs) {
    @Override
    public String toString() {
      return name;
    }
  }

  private static final List<Network> NETWORKS =
      List.of(
          new Network("BPO1", "BPO1", 0, BPO1, BPO1_BLOBS),
          new Network("BPO2", "BPO2", 0, BPO2, BPO2_BLOBS),
          new Network("AmsterdamBPOIncrease", "AmsterdamBPOIncrease", 0, BPO3, INCREASE),
          new Network("AmsterdamBPODecrease", "AmsterdamBPODecrease", 0, BPO3, DECREASE),
          new Network(
              "AmsterdamToBPOIncreaseAtTime15k", "AmsterdamBPOIncrease", 15000, BPO3, INCREASE),
          new Network(
              "AmsterdamToBPODecreaseAtTime15k", "AmsterdamBPODecrease", 15000, BPO3, DECREASE),
          new Network("BogotaBPOIncrease", "BogotaBPOIncrease", 0, BOGOTA, INCREASE),
          new Network("BogotaBPODecrease", "BogotaBPODecrease", 0, BOGOTA, DECREASE));

  static Stream<Arguments> networks() {
    final Function<String, BlobScheduleOptions> blockchain =
        config ->
            read(config, BlockchainReferenceTestCaseSpec.class).getBlobScheduleOptions().get();
    final Function<String, BlobScheduleOptions> engine =
        config -> read(config, EngineTestCaseSpec.class).getBlobScheduleOptions().get();
    return Stream.of(Arguments.of("blockchain", blockchain), Arguments.of("engine", engine))
        .flatMap(
            parser ->
                NETWORKS.stream()
                    .map(network -> Arguments.of(parser.get()[0], parser.get()[1], network)));
  }

  @ParameterizedTest(name = "{0} {2}")
  @MethodSource("networks")
  void appliesTheNetworkBlobScheduleFromItsActivation(
      final String parser,
      final Function<String, BlobScheduleOptions> blobScheduleOf,
      final Network network) {
    final String config =
        "{\"blobSchedule\": {\"BPO1\": "
            + BPO1_BLOBS.json()
            + ", \"BPO2\": "
            + BPO2_BLOBS.json()
            + ", \""
            + network.blobKey()
            + "\": "
            + network.blobs().json()
            + "}}";
    final ProtocolSchedule schedule =
        ReferenceTestProtocolSchedules.cached(
                EvmConfiguration.DEFAULT, blobScheduleOf.apply(config), List.of())
            .getByName(network.name());
    assertThat(schedule).isNotNull();

    assertSpec(specAt(schedule, network.activation()), network.hardfork(), network.blobs());
    if (network.activation() > 0) {
      assertSpec(specAt(schedule, network.activation() - 1), AMSTERDAM, BPO2_BLOBS);
    }
  }

  private static void assertSpec(
      final ProtocolSpec spec, final HardforkId hardfork, final Blobs blobs) {
    assertThat(spec.getHardforkId()).isEqualTo(hardfork);
    assertThat(spec.getGasLimitCalculator().getTargetBlobGasPerBlock())
        .isEqualTo(blobs.target() * BLOB_GAS_PER_BLOB);
    assertThat(spec.getGasLimitCalculator().currentBlobGasLimit())
        .isEqualTo(blobs.max() * BLOB_GAS_PER_BLOB);
    assertThat(((BlobFeeMarket) spec.getFeeMarket()).getBaseFeeUpdateFraction())
        .isEqualTo(BigInteger.valueOf(blobs.updateFraction()));
  }

  private static ProtocolSpec specAt(final ProtocolSchedule schedule, final long timestamp) {
    return schedule.getByBlockHeader(
        BlockHeaderBuilder.createDefault()
            .number(1)
            .timestamp(timestamp)
            .buildProcessableBlockHeader());
  }

  private static <T> T read(final String config, final Class<T> type) {
    try {
      return MAPPER.readValue(
          "{\"lastblockhash\": \"0x" + "00".repeat(32) + "\", \"config\": " + config + "}", type);
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
