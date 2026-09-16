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
package org.hyperledger.besu.ethereum.vm.operations.v2;

import static org.hyperledger.besu.ethereum.vm.operations.v2.BlockHashBenchmarkChain.CURRENT_BLOCK;
import static org.hyperledger.besu.ethereum.vm.operations.v2.BlockHashBenchmarkChain.VALID_BLOCK;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.PetersburgGasCalculator;
import org.hyperledger.besu.evm.operation.BlockHashOperation;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BlockHashOperationBenchmarkV1 {

  @Param public BlockHashOperationBenchmarkV2.Scenario scenario;

  private BlockHashOperation operation;
  private MessageFrame frame;
  private Bytes32 requestedBlock;

  @Setup
  public void setup() {
    operation = new BlockHashOperation(new PetersburgGasCalculator());
    final BlockHashBenchmarkChain chain = BlockHashBenchmarkChain.create();
    frame =
        BenchmarkHelperV2.createMessageCallFrame(
            false, chain.currentHeader(), chain.blockHashLookup(), Optional.empty());
    requestedBlock =
        switch (scenario) {
          case VALID -> Bytes32.leftPad(Bytes.ofUnsignedLong(VALID_BLOCK));
          case CURRENT -> Bytes32.leftPad(Bytes.ofUnsignedLong(CURRENT_BLOCK));
          case OUTSIDE_LOOKBACK -> Bytes32.leftPad(Bytes.ofUnsignedLong(CURRENT_BLOCK - 257));
          case TOO_LARGE -> Bytes32.fromHexString("0x010000000000000000");
        };
  }

  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    frame.pushStackItem(requestedBlock);
    blackhole.consume(operation.execute(frame, null));
    frame.popStackItem();
  }
}
