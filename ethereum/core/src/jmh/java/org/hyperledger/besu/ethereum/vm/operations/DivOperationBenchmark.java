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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.DivOperationOptimized;
import org.hyperledger.besu.evm.operation.Operation;

import java.math.BigInteger;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

public class DivOperationBenchmark extends BinaryOperationBenchmark {

  public enum Case {
    DIV_32_32(1, 1),
    DIV_64_32(2, 1),
    DIV_64_64(2, 2),
    DIV_128_32(4, 1),
    DIV_128_64(4, 2),
    DIV_128_128(4, 4),
    DIV_192_32(6, 1),
    DIV_192_64(6, 2),
    DIV_192_128(6, 4),
    DIV_192_192(6, 6),
    DIV_256_32(8, 1),
    DIV_256_64(8, 2),
    DIV_256_128(8, 4),
    DIV_256_192(8, 6),
    DIV_256_256(8, 8),
    DIV_ZERO_QUOTIENT_0_256(0, 8),
    DIV_ZERO_QUOTIENT_64_256(2, 8),
    DIV_ZERO_QUOTIENT_128_256(4, 8),
    DIV_ZERO_QUOTIENT_192_256(6, 8),
    DIV_FULL_RANDOM(-1, -1);

    final int numSize;
    final int denomSize;

    Case(final int numSize, final int denomSize) {
      this.numSize = numSize;
      this.denomSize = denomSize;
    }
  }

  @Param({
    "DIV_32_32",
    "DIV_64_32",
    "DIV_64_64",
    "DIV_128_32",
    "DIV_128_64",
    "DIV_128_128",
    "DIV_192_32",
    "DIV_192_64",
    "DIV_192_128",
    "DIV_192_192",
    "DIV_256_32",
    "DIV_256_64",
    "DIV_256_128",
    "DIV_256_192",
    "DIV_256_256",
    "DIV_ZERO_QUOTIENT_0_256",
    "DIV_ZERO_QUOTIENT_64_256",
    "DIV_ZERO_QUOTIENT_128_256",
    "DIV_ZERO_QUOTIENT_192_256",
    "DIV_FULL_RANDOM"
  })
  private String caseName;

  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();

    Case scenario = Case.valueOf(caseName);
    aPool = new Bytes[SAMPLE_SIZE];
    bPool = new Bytes[SAMPLE_SIZE];

    final Random random = new Random();
    int aSize;
    int bSize;

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      if (scenario.numSize < 0) aSize = random.nextInt(1, 33);
      else aSize = scenario.numSize * 4;
      if (scenario.denomSize < 0) bSize = random.nextInt(1, 33);
      else bSize = scenario.denomSize * 4;

      final byte[] a = new byte[aSize];
      final byte[] b = new byte[bSize];
      random.nextBytes(a);
      random.nextBytes(b);

      // Swap a and b if necessary
      if ((scenario.numSize != scenario.denomSize)) {
        aPool[i] = Bytes.wrap(a);
        bPool[i] = Bytes.wrap(b);
      } else {
        BigInteger aInt = new BigInteger(1, a);
        BigInteger bInt = new BigInteger(1, b);
        if ((aInt.compareTo(bInt) < 0)) {
          aPool[i] = Bytes.wrap(b);
          bPool[i] = Bytes.wrap(a);
        } else {
          aPool[i] = Bytes.wrap(a);
          bPool[i] = Bytes.wrap(b);
        }
      }
    }
    index = 0;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return DivOperationOptimized.staticOperation(frame);
  }
}
