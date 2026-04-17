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
import org.hyperledger.besu.evm.v2.operation.MulModOperationV2;

import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

public class MulModOperationBenchmarkV2 extends TernaryOperationBenchmarkV2 {

  // Benches for (a * b) % m

  // Define available scenarios
  public enum Case {
    MULMOD_32_32_32(1, 1, 1),
    MULMOD_64_32_32(2, 1, 1),
    MULMOD_64_64_32(2, 2, 1),
    MULMOD_64_64_64(2, 2, 2),
    MULMOD_128_32_32(4, 1, 1),
    MULMOD_128_64_32(4, 2, 1),
    MULMOD_128_64_64(4, 2, 2),
    MULMOD_128_128_32(4, 4, 1),
    MULMOD_128_128_64(4, 4, 2),
    MULMOD_128_128_128(4, 4, 4),
    MULMOD_192_32_32(6, 1, 1),
    MULMOD_192_64_32(6, 2, 1),
    MULMOD_192_64_64(6, 2, 2),
    MULMOD_192_128_32(6, 4, 1),
    MULMOD_192_128_64(6, 4, 2),
    MULMOD_192_128_128(6, 4, 4),
    MULMOD_192_192_32(6, 6, 1),
    MULMOD_192_192_64(6, 6, 2),
    MULMOD_192_192_128(6, 6, 4),
    MULMOD_192_192_192(6, 6, 6),
    MULMOD_256_32_32(8, 1, 1),
    MULMOD_256_64_32(8, 2, 1),
    MULMOD_256_64_64(8, 2, 2),
    MULMOD_256_64_128(8, 2, 4),
    MULMOD_256_64_192(8, 2, 6),
    MULMOD_256_128_32(8, 4, 1),
    MULMOD_256_128_64(8, 4, 2),
    MULMOD_256_128_128(8, 4, 4),
    MULMOD_256_192_32(8, 6, 1),
    MULMOD_256_192_64(8, 6, 2),
    MULMOD_256_192_128(8, 6, 4),
    MULMOD_256_192_192(8, 6, 6),
    MULMOD_256_256_32(8, 8, 1),
    MULMOD_256_256_64(8, 8, 2),
    MULMOD_256_256_128(8, 8, 4),
    MULMOD_256_256_192(8, 8, 6),
    MULMOD_256_256_256(8, 8, 8),
    LARGER_MULMOD_64_64_128(2, 2, 4),
    LARGER_MULMOD_192_192_256(6, 6, 8),
    ZERO_MULMOD_128_256_0(4, 8, 0),
    FULL_RANDOM(-1, -1, -1);

    final int aSize;
    final int bSize;
    final int mSize;

    Case(final int aSize, final int bSize, final int mSize) {
      this.aSize = aSize;
      this.bSize = bSize;
      this.mSize = mSize;
    }
  }

  @Param({
    "MULMOD_32_32_32",
    "MULMOD_64_32_32",
    "MULMOD_64_64_32",
    "MULMOD_64_64_64",
    "MULMOD_128_32_32",
    "MULMOD_128_64_32",
    "MULMOD_128_64_64",
    "MULMOD_128_128_32",
    "MULMOD_128_128_64",
    "MULMOD_128_128_128",
    "MULMOD_192_32_32",
    "MULMOD_192_64_32",
    "MULMOD_192_64_64",
    "MULMOD_192_128_32",
    "MULMOD_192_128_64",
    "MULMOD_192_128_128",
    "MULMOD_192_192_32",
    "MULMOD_192_192_64",
    "MULMOD_192_192_128",
    "MULMOD_192_192_192",
    "MULMOD_256_32_32",
    "MULMOD_256_64_32",
    "MULMOD_256_64_64",
    "MULMOD_256_64_128",
    "MULMOD_256_64_192",
    "MULMOD_256_128_32",
    "MULMOD_256_128_64",
    "MULMOD_256_128_128",
    "MULMOD_256_192_32",
    "MULMOD_256_192_64",
    "MULMOD_256_192_128",
    "MULMOD_256_192_192",
    "MULMOD_256_256_32",
    "MULMOD_256_256_64",
    "MULMOD_256_256_128",
    "MULMOD_256_256_192",
    "MULMOD_256_256_256",
    "LARGER_MULMOD_64_64_128",
    "LARGER_MULMOD_192_192_256",
    "ZERO_MULMOD_128_256_0",
    "FULL_RANDOM"
  })
  private String caseName;

  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelperV2.createMessageCallFrame();

    MulModOperationBenchmarkV2.Case scenario = MulModOperationBenchmarkV2.Case.valueOf(caseName);
    aPool = new UInt256[SAMPLE_SIZE];
    bPool = new UInt256[SAMPLE_SIZE];
    mPool = new UInt256[SAMPLE_SIZE];

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    int aSize;
    int bSize;
    int mSize;

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      if (scenario.aSize < 0) aSize = random.nextInt(1, 33);
      else aSize = scenario.aSize * 4;
      if (scenario.bSize < 0) bSize = random.nextInt(1, 33);
      else bSize = scenario.bSize * 4;
      if (scenario.mSize < 0) mSize = random.nextInt(1, 33);
      else mSize = scenario.mSize * 4;

      final byte[] a = new byte[aSize];
      final byte[] b = new byte[bSize];
      final byte[] m = new byte[mSize];
      random.nextBytes(a);
      random.nextBytes(b);
      random.nextBytes(m);
      aPool[i] = BenchmarkHelperV2.bytesToUInt256(a);
      bPool[i] = BenchmarkHelperV2.bytesToUInt256(b);
      mPool[i] = BenchmarkHelperV2.bytesToUInt256(m);
    }
    index = 0;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return MulModOperationV2.staticOperation(frame, frame.stackDataV2());
  }
}
