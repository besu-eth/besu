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
package org.hyperledger.besu.ethereum.vm.operations;

import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.vm.BlockchainBasedBlockHashLookup;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.PetersburgGasCalculator;
import org.hyperledger.besu.evm.operation.BlockHashOperation;

import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BlockHashOperationBenchmark implements GasCostBenchmark {

  @Param({
    "1", // Worst-case scenario
    "125", // Must iterate up the chain
    "255" // Hash available directly via current header's parentHash
  })
  public long blockNumber;

  private OperationBenchmarkHelper operationBenchmarkHelper;
  private BlockHashOperation operation;
  private MessageFrame frame;

  @Setup
  public void prepare() throws Exception {
    operationBenchmarkHelper = OperationBenchmarkHelper.create();
    operation = new BlockHashOperation(new PetersburgGasCalculator());
    frame = operationBenchmarkHelper.createMessageFrame();
  }

  @TearDown
  public void cleanUp() throws Exception {
    operationBenchmarkHelper.cleanUp();
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator calc) {
    return calc.getBlockHashOperationGasCost();
  }

  @Override
  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    frame.pushStackItem(UInt256.valueOf(blockNumber));
    operation.execute(frame, null);
    blackhole.consume(frame.popStackItem());
  }

  @Benchmark
  public Bytes executeOperationWithEmptyHashCache() {
    final MessageFrame cleanFrame =
        operationBenchmarkHelper
            .createMessageFrameBuilder()
            .blockHashLookup(
                new BlockchainBasedBlockHashLookup(
                    (ProcessableBlockHeader) frame.getBlockValues(),
                    operationBenchmarkHelper.getBlockchain()))
            .build();
    cleanFrame.pushStackItem(UInt256.valueOf(blockNumber));
    operation.execute(cleanFrame, null);
    return cleanFrame.popStackItem();
  }
}
