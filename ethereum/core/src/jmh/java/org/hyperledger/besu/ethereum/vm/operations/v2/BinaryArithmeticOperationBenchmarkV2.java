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

import java.math.BigInteger;
import java.util.Random;

public abstract class BinaryArithmeticOperationBenchmarkV2 extends BinaryOperationBenchmarkV2 {
  static class Case {
    /** Marker value for {@link #bSizeBytes} when the divisor is a known power of two. */
    static final int B_POWER_OF_TWO = -2;

    final int aSizeBytes;
    final int bSizeBytes;

    /** Set when {@link #bSizeBytes} == {@link #B_POWER_OF_TWO}; the bit position N for 2^N. */
    final int bPow2Bit;

    private Case(final int aSize, final int bSize) {
      this(aSize, bSize, -1);
    }

    private Case(final int aSize, final int bSize, final int bPow2Bit) {
      this.aSizeBytes = aSize;
      this.bSizeBytes = bSize;
      this.bPow2Bit = bPow2Bit;
    }

    static Case fromString(final String opcodeName, final String caseName) {
      try {
        String[] splitString = caseName.split("_", 3);
        if (splitString.length < 3 || !opcodeName.equalsIgnoreCase(splitString[0])) {
          throw new IllegalArgumentException();
        }
        // `<opcode>_<aSize>_POW2_<N>` builds the divisor as 2^N (e.g. address mask 2^160).
        if (splitString[2].startsWith("POW2_")) {
          int bit = Integer.parseInt(splitString[2].substring(5));
          if (bit < 1 || bit > 255) {
            throw new IllegalArgumentException("POW2 bit position must be in [1, 255]");
          }
          return new Case(parseSizeBytes(splitString[1]), B_POWER_OF_TWO, bit);
        }
        return new Case(parseSizeBytes(splitString[1]), parseSizeBytes(splitString[2]));
      } catch (IllegalArgumentException t) {
        throw new IllegalArgumentException(
            String.format(
                "%s must have the format [%s_size_size] or [%s_size_POW2_N], "
                    + "where size is #bits and N is a bit position in [1, 255]",
                caseName, opcodeName, opcodeName));
      }
    }

    private static int parseSizeBytes(final String s) {
      return "RANDOM".equalsIgnoreCase(s) ? -1 : Integer.parseInt(s) / 8;
    }
  }

  @Override
  public void setUp() {
    frame = BenchmarkHelperV2.createMessageCallFrame();

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

    // Replace random b values with the chosen 2^N divisor. setUp is not on the measured path.
    if (scenario.bSizeBytes == Case.B_POWER_OF_TWO) {
      final UInt256 pow2Divisor = pow2(scenario.bPow2Bit);
      for (int i = 0; i < bPool.length; i++) {
        bPool[i] = pow2Divisor;
      }
    }
    index = 0;
  }

  private static UInt256 pow2(final int n) {
    if (n < 64) return new UInt256(0, 0, 0, 1L << n);
    if (n < 128) return new UInt256(0, 0, 1L << (n - 64), 0);
    if (n < 192) return new UInt256(0, 1L << (n - 128), 0, 0);
    return new UInt256(1L << (n - 192), 0, 0, 0);
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
