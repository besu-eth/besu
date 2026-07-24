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

import org.hyperledger.besu.ethereum.utils.Range;

import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

public abstract class TernaryArithmeticOperationBenchmark extends TernaryOperationBenchmark {

  private static class Case {
    final int aSizeBytes;
    final int bSizeBytes;
    final int cSizeBytes;

    private Case(final int aSize, final int bSize, final int cSize) {
      this.aSizeBytes = aSize;
      this.bSizeBytes = bSize;
      this.cSizeBytes = cSize;
    }

    // format OPCODE_INT_INT_INT
    static Case fromString(final String opcodeName, final String caseName) {
      try {
        String[] splitString = caseName.split("_", 4);
        if (splitString.length < 4 || !opcodeName.equalsIgnoreCase(splitString[0])) {
          throw new IllegalArgumentException();
        }
        return new Case(
            parseSizeBytes(splitString[1]),
            parseSizeBytes(splitString[2]),
            parseSizeBytes(splitString[3]));
      } catch (IllegalArgumentException t) {
        throw new IllegalArgumentException(
            String.format(
                "%s must have the format [%s_size_size_size] where size is #bits",
                caseName, opcodeName));
      }
    }

    void runSetup(final Bytes[] poolA, final Bytes[] poolB, final Bytes[] poolC) {
      final Random random = new Random();
      int aSize;
      int bSize;
      int cSize;

      for (int i = 0; i < SAMPLE_SIZE; i++) {
        if (aSizeBytes < 0) aSize = random.nextInt(1, 33);
        else aSize = aSizeBytes;
        if (bSizeBytes < 0) bSize = random.nextInt(1, 33);
        else bSize = bSizeBytes;
        if (cSizeBytes < 0) cSize = random.nextInt(1, 33);
        else cSize = cSizeBytes;

        final byte[] a = new byte[aSize];
        final byte[] b = new byte[bSize];
        final byte[] c = new byte[cSize];
        random.nextBytes(a);
        random.nextBytes(b);
        random.nextBytes(c);

        poolA[i] = Bytes.wrap(a);
        poolB[i] = Bytes.wrap(b);
        poolC[i] = Bytes.wrap(c);
      }
    }
  }

  private static class Pow2Case {
    final int aSizeBytes;
    final int bSizeBytes;
    final Range<Integer> pow2BitRange;

    private Pow2Case(final int aSize, final int bSize, final Range<Integer> pow2BitRange) {
      this.aSizeBytes = aSize;
      this.bSizeBytes = bSize;
      this.pow2BitRange = pow2BitRange;
    }

    // format OPCODE_INT_INT_POW2_INT_INT
    static Pow2Case fromString(final String opcodeName, final String caseName) {
      try {
        String[] splitString = caseName.split("_", 6);
        if (splitString.length < 6
            || !opcodeName.equalsIgnoreCase(splitString[0])
            || !splitString[3].equalsIgnoreCase("POW2")) {
          throw new IllegalArgumentException();
        }
        Range<Integer> pow2BitRange =
            new Range<>(
                Integer.parseInt(splitString[4]),
                Integer.parseInt(splitString[5]),
                Integer::compare);
        if (!pow2BitRange.isWithin(1, 255)) {
          throw new IllegalArgumentException();
        }
        return new Pow2Case(
            parseSizeBytes(splitString[1]), parseSizeBytes(splitString[2]), pow2BitRange);
      } catch (IllegalArgumentException t) {
        throw new IllegalArgumentException(
            String.format(
                "%s must have the format [%s_size_size_POW2_bit_bit] where bit_bit is the range of bits to set in the modulus",
                caseName, opcodeName));
      }
    }

    void runSetup(final Bytes[] poolA, final Bytes[] poolB, final Bytes[] poolC) {
      final Random random = new Random();
      int aSize;
      int bSize;

      for (int i = 0; i < SAMPLE_SIZE; i++) {
        if (aSizeBytes < 0) aSize = random.nextInt(1, 33);
        else aSize = aSizeBytes;
        if (bSizeBytes < 0) bSize = random.nextInt(1, 33);
        else bSize = bSizeBytes;

        final byte[] a = new byte[aSize];
        final byte[] b = new byte[bSize];
        random.nextBytes(a);
        random.nextBytes(b);

        poolA[i] = Bytes.wrap(a);
        poolB[i] = Bytes.wrap(b);
        poolC[i] = pow2(random.nextInt(pow2BitRange.minimum, pow2BitRange.maximum + 1));
      }
    }
  }

  private static int parseSizeBytes(final String s) {
    return "RANDOM".equalsIgnoreCase(s) ? -1 : Integer.parseInt(s) / 8;
  }

  @Setup(Level.Iteration)
  @Override
  @SuppressWarnings("StringCaseLocaleUsage")
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();

    aPool = new Bytes[SAMPLE_SIZE];
    bPool = new Bytes[SAMPLE_SIZE];
    cPool = new Bytes[SAMPLE_SIZE];

    if (caseName().toLowerCase().matches(".*_\\d+_\\d+_pow2_\\d+_\\d+")) {
      Pow2Case.fromString(opCode(), caseName()).runSetup(aPool, bPool, cPool);
    } else {
      Case.fromString(opCode(), caseName()).runSetup(aPool, bPool, cPool);
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
