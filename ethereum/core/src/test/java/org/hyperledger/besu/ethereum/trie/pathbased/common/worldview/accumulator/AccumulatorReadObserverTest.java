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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.mainnet.slowblock.SlowBlockReadCounts;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.function.BiConsumer;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccumulatorReadObserverTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  private ExecutionContextTestFixture contextTestFixture;
  private ProtocolContext protocolContext;
  private BlockHeader chainHeadHeader;

  @BeforeEach
  void setUp() {
    contextTestFixture =
        ExecutionContextTestFixture.builder(GenesisConfig.mainnet())
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    protocolContext = contextTestFixture.getProtocolContext();
    chainHeadHeader = contextTestFixture.getBlockchain().getChainHeadHeader();
  }

  @AfterEach
  void tearDown() throws Exception {
    contextTestFixture.getStateArchive().close();
  }

  @Test
  void everyLogicalReadIsCountedIncludingOnesTheAccumulatorServesItself() {
    withObservedAccumulator(
        (accumulator, reads) -> {
          accumulator.get(ADDRESS);
          accumulator.get(ADDRESS); // served from the accumulator's own map, still a read
          accumulator.getStorageValue(ADDRESS, UInt256.ONE);
          accumulator.getStorageValue(ADDRESS, UInt256.ONE);
          accumulator.getStorageValue(ADDRESS, UInt256.valueOf(2));
          accumulator.getCode(ADDRESS, Hash.EMPTY);

          assertThat(reads.accounts()).isEqualTo(2);
          assertThat(reads.storageSlots()).isEqualTo(3);
          assertThat(reads.code()).isEqualTo(1);
        });
  }

  @Test
  void onlyReadsThatFallThroughToTheWorldStateAreTimed() {
    withObservedAccumulator(
        (accumulator, reads) -> {
          accumulator.get(ADDRESS);
          final long afterFirstRead = reads.readNanos();
          assertThat(afterFirstRead).isPositive();

          accumulator.get(ADDRESS); // no fall through, so no additional read time
          assertThat(reads.readNanos()).isEqualTo(afterFirstRead);
        });
  }

  @Test
  void removingTheObserverStopsCounting() {
    withObservedAccumulator(
        (accumulator, reads) -> {
          accumulator.get(ADDRESS);
          assertThat(reads.accounts()).isEqualTo(1);

          accumulator.setReadObserver(null);
          accumulator.get(ADDRESS);
          accumulator.getStorageValue(ADDRESS, UInt256.ONE);

          assertThat(reads.accounts()).isEqualTo(1);
          assertThat(reads.storageSlots()).isZero();
        });
  }

  private void withObservedAccumulator(
      final BiConsumer<PathBasedWorldStateUpdateAccumulator<?>, SlowBlockReadCounts> consumer) {
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            protocolContext
                .getWorldStateArchive()
                .getWorldState(
                    WorldStateQueryParams.newBuilder()
                        .withBlockHeader(chainHeadHeader)
                        .withShouldWorldStateUpdateHead(false)
                        .build())
                .orElseThrow();
    try {
      final PathBasedWorldStateUpdateAccumulator<?> accumulator = worldState.getAccumulator();
      final SlowBlockReadCounts reads = new SlowBlockReadCounts();
      accumulator.setReadObserver(reads);
      consumer.accept(accumulator, reads);
    } finally {
      worldState.close();
    }
  }
}
