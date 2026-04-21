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

import org.hyperledger.besu.ethereum.vm.operations.BenchmarkHelper;
import org.hyperledger.besu.evm.UInt256;

import java.math.BigInteger;
import java.util.Random;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

public abstract class BinaryArithmeticOperationBenchmarkV2 extends BinaryOperationBenchmarkV2 {
  static class Case {
    final int aSizeBytes;
    final int bSizeBytes;

    private Case(final int aSize, final int bSize) {
      this.aSizeBytes = aSize;
      this.bSizeBytes = bSize;
    }

    static Case fromString(final String opcodeName, final String caseName) {
      try {
        String[] splitString = caseName.split("_", 3);
        if (splitString.length < 3 || !opcodeName.equalsIgnoreCase(splitString[0])) {
          throw new IllegalArgumentException();
        }
        return new Case(parseSizeBytes(splitString[1]), parseSizeBytes(splitString[2]));
      } catch (IllegalArgumentException t) {
        throw new IllegalArgumentException(
            String.format(
                "%s must have the format [%s_size_size] where size is #bits",
                caseName, opcodeName));
      }
    }

    private static int parseSizeBytes(final String s) {
      return "RANDOM".equalsIgnoreCase(s) ? -1 : Integer.parseInt(s) / 8;
    }
  }

  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();

    Case scenario = Case.fromString(opCode(), caseName());
    aPool = new UInt256[SAMPLE_SIZE];
    bPool = new UInt256[SAMPLE_SIZE];

    final Random random = new Random();
    int aSize;
    int bSize;

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      if (scenario.aSizeBytes < 0) aSize = random.nextInt(1, 33);
      else aSize = scenario.aSizeBytes;
      if (scenario.bSizeBytes < 0) bSize = random.nextInt(1, 33);
      else bSize = scenario.bSizeBytes;

      byte[] a = new byte[aSize];
      byte[] b = new byte[bSize];
      random.nextBytes(a);
      random.nextBytes(b);

      // Swap a and b if necessary - a must always be the biggest unsigned
      if (scenario.aSizeBytes == scenario.bSizeBytes) {
        if ((new BigInteger(1, a).compareTo(new BigInteger(1, b)) < 0)) {
          byte[] tmp = a;
          a = b;
          b = tmp;
        }
      }

      aPool[i] = BenchmarkHelperV2.bytesToUInt256(a);
      bPool[i] = BenchmarkHelperV2.bytesToUInt256(b);
    }
    index = 0;
  }

  /**
   * The benchmark case name that is currently running in the benchmark. By default, the benchmark
   * runs with full randomization on byte array sizes and their values.
   *
   * @return the benchmark case name
   */
  protected abstract String caseName();

  /**
   * The opcode under test.
   *
   * @return the opcode name, case-insensitive.
   */
  protected abstract String opCode();
}
