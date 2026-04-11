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
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.SDivOperationOptimized;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

public class SDivOperationBenchmark extends BinaryOperationBenchmark {

  public enum Case {
    SDIV_32_32(1, 1),
    SDIV_64_32(2, 1),
    SDIV_64_64(2, 2),
    SDIV_128_32(4, 1),
    SDIV_128_64(4, 2),
    SDIV_128_128(4, 4),
    SDIV_192_32(6, 1),
    SDIV_192_64(6, 2),
    SDIV_192_128(6, 4),
    SDIV_192_192(6, 6),
    SDIV_256_32(8, 1),
    SDIV_256_64(8, 2),
    SDIV_256_128(8, 4),
    SDIV_256_192(8, 6),
    SDIV_256_256(8, 8),
    SDIV_ZERO_QUOTIENT_0_256(0, 8),
    SDIV_ZERO_QUOTIENT_64_256(2, 8),
    SDIV_ZERO_QUOTIENT_128_256(4, 8),
    SDIV_ZERO_QUOTIENT_192_256(6, 8),
    SDIV_FULL_RANDOM(-1, -1);

    final int numSize;
    final int denomSize;

    Case(final int numSize, final int denomSize) {
      this.numSize = numSize;
      this.denomSize = denomSize;
    }
  }

  @Param({
    "SDIV_32_32",
    "SDIV_64_32",
    "SDIV_64_64",
    "SDIV_128_32",
    "SDIV_128_64",
    "SDIV_128_128",
    "SDIV_192_32",
    "SDIV_192_64",
    "SDIV_192_128",
    "SDIV_192_192",
    "SDIV_256_32",
    "SDIV_256_64",
    "SDIV_256_128",
    "SDIV_256_192",
    "SDIV_256_256",
    "SDIV_ZERO_QUOTIENT_0_256",
    "SDIV_ZERO_QUOTIENT_64_256",
    "SDIV_ZERO_QUOTIENT_128_256",
    "SDIV_ZERO_QUOTIENT_192_256",
    "SDIV_FULL_RANDOM"
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

      byte[] a = new byte[aSize];
      byte[] b = new byte[bSize];
      random.nextBytes(a);
      random.nextBytes(b);
      a = negate(a);
      b = negate(b);

      // Swap a and b if necessary
      if ((scenario.numSize != scenario.denomSize)) {
        aPool[i] = Bytes.wrap(a);
        bPool[i] = Bytes.wrap(b);
      } else {
        BigInteger aInt = new BigInteger(a);
        BigInteger bInt = new BigInteger(b);
        if ((aInt.abs().compareTo(bInt.abs()) < 0)) {
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

  private static byte[] negate(final byte[] array) {
    byte[] tmp = new byte[32];
    Arrays.fill(tmp, (byte) 0xFF);
    System.arraycopy(array, 0, tmp, 32 - array.length, array.length);
    return tmp;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return SDivOperationOptimized.staticOperation(frame);
  }
}
