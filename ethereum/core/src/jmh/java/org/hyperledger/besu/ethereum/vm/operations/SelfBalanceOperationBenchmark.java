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

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class SelfBalanceOperationBenchmark {

  private SelfBalanceOperation operation;
  private MessageFrame frame;

  @Setup
  public void prepare() throws Exception {
    operation = new SelfBalanceOperation(mock(GasCalculator.class));
    final Blockchain blockchain = mock(Blockchain.class);
    final Address address = Address.fromHexString("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

    final WorldUpdater worldUpdater;
    try (WorldStateArchive archive = createBonsaiInMemoryWorldStateArchive(blockchain)) {
      worldUpdater = archive.getWorldState().updater();
    }
    worldUpdater.getOrCreate(address).setBalance(Wei.of(1));
    worldUpdater.commit();

    final ExecutionContextTestFixture executionContextTestFixture =
        ExecutionContextTestFixture.create();
    final BlockHeader blockHeader = new BlockHeaderTestFixture().buildHeader();
    frame =
        new MessageFrameTestFixture()
            .address(address)
            .worldUpdater(worldUpdater)
            .blockHeader(blockHeader)
            .executionContextTestFixture(executionContextTestFixture)
            .blockchain(blockchain)
            .build();

    // Pre-warm: force trie path into memory
    worldUpdater.get(address);
  }

  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    blackhole.consume(operation.execute(frame, null));
    frame.popStackItem();
  }
}
