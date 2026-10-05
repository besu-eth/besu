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
package org.hyperledger.besu.ethereum.vm.operations;

import static org.hyperledger.besu.ethereum.vm.operations.BenchmarkHelper.randomValue;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.ByteOperation;
import org.hyperledger.besu.evm.operation.Operation;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.BenchmarkParams;

public class ByteOperationBenchmark extends BinaryOperationBenchmark implements GasCostBenchmark {

  /** Test cases covering the in-range and out-of-range paths of BYTE. */
  public enum Case {
    /** Index 0-31 - a byte of the value is selected. */
    IN_RANGE,
    /** Index 32-255 - out of range in the low limb. */
    OUT_OF_RANGE_SMALL,
    /** Random 256-bit index - almost always out of range in a higher limb. */
    OUT_OF_RANGE_LARGE
  }

  @Param protected Case scenario;

  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();

    aPool = new Bytes[SAMPLE_SIZE]; // index (pushed second, popped first)
    bPool = new Bytes[SAMPLE_SIZE]; // value (pushed first, popped second)

    final ThreadLocalRandom random = ThreadLocalRandom.current();

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      switch (scenario) {
        case IN_RANGE:
          aPool[i] = Bytes.of(random.nextInt(32));
          break;
        case OUT_OF_RANGE_SMALL:
          aPool[i] = Bytes.of(32 + random.nextInt(224));
          break;
        case OUT_OF_RANGE_LARGE:
          aPool[i] = randomValue(random);
          break;
      }
      bPool[i] = randomValue(random);
    }
    index = 0;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return ByteOperation.staticOperation(frame);
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator calc) {
    return new ByteOperation(calc).getGasCost();
  }
}
