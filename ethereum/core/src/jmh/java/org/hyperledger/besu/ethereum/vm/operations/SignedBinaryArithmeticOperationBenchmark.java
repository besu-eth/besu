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

import org.hyperledger.besu.evm.UInt256;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;

public abstract class SignedBinaryArithmeticOperationBenchmark
    extends BinaryArithmeticOperationBenchmark {
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
    byte[] in = input;
    if (in.length == 32) {
      in = Arrays.copyOf(input, input.length);
      // mask MSB so |u| < 2^255 allowing −u 32-byte two's complement
      in[0] &= (byte) 0x7F;
    }
    UInt256 value = UInt256.fromBytesBE(in);
    return value.neg().toBytesBE();
  }
}
