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
package org.hyperledger.besu.ethereum.vm.operations;

import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MessageFrameTestFixture;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.SelfBalanceOperation;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Thread)
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class SelfBalanceOperationBenchmark {

  private static final int NUMBER_ADDRESSES = 20000;
  private static final Random RANDOM_GENERATOR = new Random();
  private SelfBalanceOperation operation;
  private MessageFrame[] frames;
  private int executingFrameIndex = 0;

  private MessageFrame createMessageFrame(
      final Blockchain blockchain,
      final WorldUpdater worldUpdater,
      final ExecutionContextTestFixture executionContextTestFixture,
      final Address address) {
    final BlockHeader blockHeader = new BlockHeaderTestFixture().buildHeader();
    return new MessageFrameTestFixture()
        .address(address)
        .worldUpdater(worldUpdater)
        .blockHeader(blockHeader)
        .executionContextTestFixture(executionContextTestFixture)
        .blockchain(blockchain)
        .build();
  }

  private WorldUpdater createWorldUpdater(final Blockchain blockchain, final Address[] addresses)
      throws IOException {
    final WorldUpdater worldStateUpdater;
    try (WorldStateArchive worldStateArchive = createBonsaiInMemoryWorldStateArchive(blockchain)) {
      worldStateUpdater = worldStateArchive.getWorldState().updater();
    }
    for (Address address : addresses) {
      worldStateUpdater.getOrCreate(address).setBalance(Wei.of(1));
    }
    worldStateUpdater.commit();
    return worldStateUpdater;
  }

  @Setup
  public void prepare() throws Exception {
    operation = new SelfBalanceOperation(mock(GasCalculator.class));
    final Blockchain blockchain = mock(Blockchain.class);
    final Address[] addresses = new Address[NUMBER_ADDRESSES];
    for (int j = 0; j < addresses.length; j++) {
      final byte[] address = new byte[Address.SIZE];
      RANDOM_GENERATOR.nextBytes(address);
      addresses[j] = Address.wrap(Bytes.wrap(address));
    }
    frames = new MessageFrame[NUMBER_ADDRESSES];
    final WorldUpdater worldUpdater = createWorldUpdater(blockchain, addresses);
    final ExecutionContextTestFixture executionContextTestFixture =
        ExecutionContextTestFixture.create();
    for (int i = 0; i < NUMBER_ADDRESSES; i++) {
      frames[i] =
          createMessageFrame(blockchain, worldUpdater, executionContextTestFixture, addresses[i]);
    }
  }

  @Benchmark
  public void executeOperation() {
    final MessageFrame executingFrame = frames[executingFrameIndex++];
    operation.execute(executingFrame, null);
    executingFrame.popStackItem();
    executingFrameIndex = executingFrameIndex % NUMBER_ADDRESSES;
  }
}
