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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.BlockValues;
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

  private static final long CURRENT_BLOCK = 1_000;
  private static final long VALID_BLOCK = CURRENT_BLOCK - 1;
  private static final Hash VALID_BLOCK_HASH =
      Hash.fromHexString("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");

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
    final BlockValues blockValues =
        new BlockValues() {
          @Override
          public long getNumber() {
            return CURRENT_BLOCK;
          }
        };
    frame =
        BenchmarkHelperV2.createMessageCallFrame(
            blockValues,
            (__, blockNumber) -> blockNumber == VALID_BLOCK ? VALID_BLOCK_HASH : Hash.ZERO,
            Optional.empty());
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
