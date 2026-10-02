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

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.PetersburgGasCalculator;
import org.hyperledger.besu.evm.v2.operation.BlockHashOperationV2;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
public class BlockHashOperationBenchmarkV2 {

  public enum Scenario {
    VALID,
    CURRENT,
    OUTSIDE_LOOKBACK,
    TOO_LARGE
  }

  @Param public Scenario scenario;

  private BlockHashOperationV2 operation;
  private MessageFrame frame;
  private UInt256 requestedBlock;

  @Setup
  public void setup() {
    operation = new BlockHashOperationV2(new PetersburgGasCalculator());
    final BlockHashBenchmarkChain chain = BlockHashBenchmarkChain.create();
    frame =
        BenchmarkHelperV2.createMessageCallFrame(
            chain.currentHeader(), chain.blockHashLookup(), Optional.empty());
    requestedBlock =
        switch (scenario) {
          case VALID -> UInt256.fromLong(VALID_BLOCK);
          case CURRENT -> UInt256.fromLong(CURRENT_BLOCK);
          case OUTSIDE_LOOKBACK -> UInt256.fromLong(CURRENT_BLOCK - 257);
          case TOO_LARGE -> new UInt256(1, 0, 0, 0);
        };
  }

  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    BenchmarkHelperV2.pushUInt256(frame, requestedBlock);
    blackhole.consume(operation.execute(frame, null));
    frame.setTopV2(0);
  }
}
