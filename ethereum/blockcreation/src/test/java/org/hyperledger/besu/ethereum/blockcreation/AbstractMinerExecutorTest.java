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
package org.hyperledger.besu.ethereum.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MinedBlockObserver;
import org.hyperledger.besu.ethereum.chain.PoWObserver;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.util.Subscribers;

import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AbstractMinerExecutorTest {

  private MiningConfiguration miningConfiguration;
  private TestMinerExecutor executor;

  @BeforeEach
  public void setUp() {
    miningConfiguration = ImmutableMiningConfiguration.builder().build();
    executor =
        new TestMinerExecutor(
            mock(ProtocolContext.class),
            mock(ProtocolSchedule.class),
            mock(TransactionPool.class),
            miningConfiguration,
            mock(DefaultBlockScheduler.class),
            mock(EthScheduler.class));
  }

  @Test
  public void changeTargetGasLimit_shouldUpdateMiningConfiguration() {
    final long newTargetGasLimit = 15_000_000L;
    assertThat(miningConfiguration.getTargetGasLimit()).isEqualTo(OptionalLong.empty());

    executor.changeTargetGasLimit(newTargetGasLimit);

    assertThat(miningConfiguration.getTargetGasLimit())
        .isEqualTo(OptionalLong.of(newTargetGasLimit));
  }

  @Test
  public void changeTargetGasLimit_shouldRejectInvalidValue() {
    assertThatThrownBy(() -> executor.changeTargetGasLimit(-1L))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void changeTargetGasLimit_shouldAllowMultipleUpdates() {
    executor.changeTargetGasLimit(10_000_000L);
    assertThat(miningConfiguration.getTargetGasLimit()).isEqualTo(OptionalLong.of(10_000_000L));

    executor.changeTargetGasLimit(20_000_000L);
    assertThat(miningConfiguration.getTargetGasLimit()).isEqualTo(OptionalLong.of(20_000_000L));
  }

  /**
   * Minimal concrete subclass of AbstractMinerExecutor for testing purposes. Only the
   * changeTargetGasLimit method (inherited from the abstract class) is under test.
   */
  private static class TestMinerExecutor
      extends AbstractMinerExecutor<BlockMiner<AbstractBlockCreator>> {

    TestMinerExecutor(
        final ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final TransactionPool transactionPool,
        final MiningConfiguration miningParams,
        final DefaultBlockScheduler blockScheduler,
        final EthScheduler ethScheduler) {
      super(
          protocolContext,
          protocolSchedule,
          transactionPool,
          miningParams,
          blockScheduler,
          ethScheduler);
    }

    @Override
    public BlockMiner<AbstractBlockCreator> createMiner(
        final Subscribers<MinedBlockObserver> subscribers,
        final Subscribers<PoWObserver> ethHashObservers,
        final BlockHeader parentHeader) {
      throw new UnsupportedOperationException("not needed for this test");
    }

    @Override
    public Optional<Address> getCoinbase() {
      return Optional.empty();
    }
  }
}
