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

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.ByteOperationV2;

import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

public class ByteOperationBenchmarkV2 extends BinaryOperationBenchmarkV2 {

  /** Test cases covering the in-range and out-of-range paths of BYTE. */
  public enum Case {
    /** Index 0-31 - a byte of the value is selected. */
    IN_RANGE,
    /** Random 256-bit index - almost always 32 or more, so the result is zero. */
    OUT_OF_RANGE
  }

  @Param protected Case scenario;

  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelperV2.createMessageCallFrame();

    aPool = new UInt256[SAMPLE_SIZE]; // index (pushed second, popped first)
    bPool = new UInt256[SAMPLE_SIZE]; // value (pushed first, popped second)

    final ThreadLocalRandom random = ThreadLocalRandom.current();

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      switch (scenario) {
        case IN_RANGE:
          aPool[i] = UInt256.fromInt(random.nextInt(32));
          break;
        case OUT_OF_RANGE:
          aPool[i] = BenchmarkHelperV2.randomUInt256Value(random);
          break;
      }
      bPool[i] = BenchmarkHelperV2.randomUInt256Value(random);
    }
    index = 0;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return ByteOperationV2.staticOperation(frame);
  }
}
