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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

public abstract class SignedBinaryArithmeticOperationBenchmark
    extends BinaryArithmeticOperationBenchmark {
  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();

    Case scenario = Case.fromString(opCode(), caseName());
    aPool = new Bytes[SAMPLE_SIZE];
    bPool = new Bytes[SAMPLE_SIZE];

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

      a = negate(a);
      b = negate(b);

      // Swap `a` and `b` if necessary - `a` must always have the biggest magnitude
      if (scenario.aSizeBytes == scenario.bSizeBytes) {
        if ((new BigInteger(a).abs().compareTo(new BigInteger(b).abs()) < 0)) {
          byte[] tmp = a;
          a = b;
          b = tmp;
        }
      }

      aPool[i] = Bytes.wrap(a);
      bPool[i] = Bytes.wrap(b);
    }
    index = 0;
  }

  private byte[] negate(final byte[] input) {
    if (input.length == 0) {
      return new byte[32];
    }
    byte[] in = Arrays.copyOf(input, input.length);
    if (in.length == 32) {
      // mask MSB so |u| < 2^255 allowing −u 32-byte two's complement
      in[0] &= (byte) 0x7F;
    }
    BigInteger bigInt = new BigInteger(1, in);
    if (bigInt.equals(BigInteger.ZERO)) {
      return new byte[32];
    }
    byte[] out = new byte[32];
    Arrays.fill(out, (byte) 0xFF);
    byte[] negBigInt = bigInt.negate().toByteArray();
    int len = Math.min(negBigInt.length, 32);
    System.arraycopy(negBigInt, negBigInt.length - len, out, 32 - len, len);
    return out;
  }
}
