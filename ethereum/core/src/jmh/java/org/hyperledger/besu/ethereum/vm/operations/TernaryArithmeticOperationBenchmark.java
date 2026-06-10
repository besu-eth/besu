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

import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

public abstract class TernaryArithmeticOperationBenchmark extends TernaryOperationBenchmark {

  private static class Case {
    /** Marker value for {@link #cSizeBytes} when the modulus is a known power of two. */
    static final int C_POWER_OF_TWO = -2;

    final int aSizeBytes;
    final int bSizeBytes;
    final int cSizeBytes;

    /** Set when {@link #cSizeBytes} == {@link #C_POWER_OF_TWO}; the bit position N for 2^N. */
    final int cPow2Bit;

    private Case(final int aSize, final int bSize, final int cSize) {
      this(aSize, bSize, cSize, -1);
    }

    private Case(final int aSize, final int bSize, final int cSize, final int cPow2Bit) {
      this.aSizeBytes = aSize;
      this.bSizeBytes = bSize;
      this.cSizeBytes = cSize;
      this.cPow2Bit = cPow2Bit;
    }

    static Case fromString(final String opcodeName, final String caseName) {
      try {
        String[] splitString = caseName.split("_", 4);
        if (splitString.length < 4 || !opcodeName.equalsIgnoreCase(splitString[0])) {
          throw new IllegalArgumentException();
        }
        // `<opcode>_<aSize>_<bSize>_POW2_<N>` builds the modulus as 2^N (e.g. address mask 2^160).
        if (splitString[3].startsWith("POW2_")) {
          int bit = Integer.parseInt(splitString[3].substring(5));
          if (bit < 1 || bit > 255) {
            throw new IllegalArgumentException("POW2 bit position must be in [1, 255]");
          }
          return new Case(
              parseSizeBytes(splitString[1]), parseSizeBytes(splitString[2]), C_POWER_OF_TWO, bit);
        }
        return new Case(
            parseSizeBytes(splitString[1]),
            parseSizeBytes(splitString[2]),
            parseSizeBytes(splitString[3]));
      } catch (IllegalArgumentException t) {
        throw new IllegalArgumentException(
            String.format(
                "%s must have the format [%s_size_size_size] or [%s_size_size_POW2_N],"
                    + " where size is #bits and N is a bit position in [1, 255]",
                caseName, opcodeName, opcodeName));
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
    aPool = new Bytes[SAMPLE_SIZE];
    bPool = new Bytes[SAMPLE_SIZE];
    cPool = new Bytes[SAMPLE_SIZE];

    final Random random = new Random();
    int aSize;
    int bSize;
    int cSize;

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      if (scenario.aSizeBytes < 0) aSize = random.nextInt(1, 33);
      else aSize = scenario.aSizeBytes;
      if (scenario.bSizeBytes < 0) bSize = random.nextInt(1, 33);
      else bSize = scenario.bSizeBytes;
      if (scenario.cSizeBytes < 0) cSize = random.nextInt(1, 33);
      else cSize = scenario.cSizeBytes;

      final byte[] a = new byte[aSize];
      final byte[] b = new byte[bSize];
      final byte[] c = new byte[cSize];
      random.nextBytes(a);
      random.nextBytes(b);
      random.nextBytes(c);
      aPool[i] = Bytes.wrap(a);
      bPool[i] = Bytes.wrap(b);
      cPool[i] = Bytes.wrap(c);
    }

    // Replace random c values with the chosen 2^N modulus. setUp is not on the measured path.
    if (scenario.cSizeBytes == Case.C_POWER_OF_TWO) {
      final Bytes pow2Modulus = pow2(scenario.cPow2Bit);
      for (int i = 0; i < cPool.length; i++) {
        cPool[i] = pow2Modulus;
      }
    }
    index = 0;
  }

  private static Bytes pow2(final int n) {
    final byte[] bytes = new byte[32];
    bytes[31 - (n >>> 3)] = (byte) (1 << (n & 7));
    return Bytes.wrap(bytes);
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
