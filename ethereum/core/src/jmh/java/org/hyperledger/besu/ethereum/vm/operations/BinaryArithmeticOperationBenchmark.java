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
import org.hyperledger.besu.evm.UInt256;

import java.math.BigInteger;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;

public abstract class BinaryArithmeticOperationBenchmark extends BinaryOperationBenchmark {
  static class Case {
    final int aSizeBytes;
    final int bSizeBytes;

    private Case(final int aSize, final int bSize) {
      this.aSizeBytes = aSize;
      this.bSizeBytes = bSize;
    }

    // format OPCODE_INT_INT
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

    void runSetup(final Bytes[] poolA, final Bytes[] poolB) {
      final Random random = new Random();
      int aSize;
      int bSize;

      for (int i = 0; i < SAMPLE_SIZE; i++) {
        if (aSizeBytes < 0) aSize = random.nextInt(1, 33);
        else aSize = aSizeBytes;
        if (bSizeBytes < 0) bSize = random.nextInt(1, 33);
        else bSize = bSizeBytes;

        byte[] a = new byte[aSize];
        byte[] b = new byte[bSize];
        random.nextBytes(a);
        random.nextBytes(b);

        // Swap a and b if necessary - a must always be the biggest unsigned
        if (aSizeBytes == bSizeBytes) {
          if ((new BigInteger(1, a).compareTo(new BigInteger(1, b)) < 0)) {
            byte[] tmp = a;
            a = b;
            b = tmp;
          }
        }

        poolA[i] = Bytes.wrap(a);
        poolB[i] = Bytes.wrap(b);
      }
    }
  }

  static class Pow2Case {
    final int aSizeBytes;
    final Range<Integer> pow2BitRange;

    private Pow2Case(final int aSize, final Range<Integer> pow2BitRange) {
      this.aSizeBytes = aSize;
      this.pow2BitRange = pow2BitRange;
    }

    // format OPCODE_INT_POW2_INT_INT
    static Pow2Case fromString(final String opcodeName, final String caseName) {
      try {
        String[] splitString = caseName.split("_", 5);
        if (splitString.length < 5
            || !opcodeName.equalsIgnoreCase(splitString[0])
            || !splitString[2].equalsIgnoreCase("POW2")) {
          throw new IllegalArgumentException();
        }
        Range<Integer> pow2BitRange =
            new Range<>(
                Integer.parseInt(splitString[3]),
                Integer.parseInt(splitString[4]),
                Integer::compare);
        if (!pow2BitRange.isWithin(1, 255)) {
          throw new IllegalArgumentException();
        }
        return new Pow2Case(parseSizeBytes(splitString[1]), pow2BitRange);
      } catch (IllegalArgumentException t) {
        throw new IllegalArgumentException(
            String.format(
                "%s must have the format [%s_POW2_bit_bit] where bit_bit is the range of bits to set in the denominator",
                caseName, opcodeName));
      }
    }

    void runSetup(final Bytes[] poolA, final Bytes[] poolB) {
      final Random random = new Random();
      int aSize;

      for (int i = 0; i < SAMPLE_SIZE; i++) {
        if (aSizeBytes < 0) aSize = random.nextInt(1, 33);
        else aSize = aSizeBytes;

        byte[] a = new byte[aSize];
        random.nextBytes(a);

        UInt256 bValue = pow2(random.nextInt(pow2BitRange.minimum, pow2BitRange.maximum + 1));

        poolA[i] = Bytes.wrap(a);
        poolB[i] = Bytes.wrap(bValue.toBytesBE());
      }
    }

    private static UInt256 pow2(final int n) {
      if (n < 64) return new UInt256(0, 0, 0, 1L << n);
      if (n < 128) return new UInt256(0, 0, 1L << (n - 64), 0);
      if (n < 192) return new UInt256(0, 1L << (n - 128), 0, 0);
      return new UInt256(1L << (n - 192), 0, 0, 0);
    }
  }

  private static int parseSizeBytes(final String s) {
    return "RANDOM".equalsIgnoreCase(s) ? -1 : Integer.parseInt(s) / 8;
  }

  @Override
  @SuppressWarnings("StringCaseLocaleUsage")
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();

    aPool = new Bytes[SAMPLE_SIZE];
    bPool = new Bytes[SAMPLE_SIZE];

    if (caseName().toLowerCase().matches(".*_\\d+_pow2_\\d+_\\d+")) {
      Pow2Case.fromString(opCode(), caseName()).runSetup(aPool, bPool);
    } else {
      Case.fromString(opCode(), caseName()).runSetup(aPool, bPool);
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
