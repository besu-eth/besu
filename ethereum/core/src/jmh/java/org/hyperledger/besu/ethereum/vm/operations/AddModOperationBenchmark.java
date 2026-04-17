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
import org.hyperledger.besu.evm.operation.AddModOperationOptimized;
import org.hyperledger.besu.evm.operation.Operation;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

public class AddModOperationBenchmark extends TernaryOperationBenchmark {

  // Benches for (a + b) % c

  // Define available scenarios
  public enum Case {
    ADDMOD_32_32_32(1, 1, 1),
    ADDMOD_64_32_32(2, 1, 1),
    ADDMOD_64_64_32(2, 2, 1),
    ADDMOD_64_64_64(2, 2, 2),
    ADDMOD_128_32_32(4, 1, 1),
    ADDMOD_128_64_32(4, 2, 1),
    ADDMOD_128_64_64(4, 2, 2),
    ADDMOD_128_128_32(4, 4, 1),
    ADDMOD_128_128_64(4, 4, 2),
    ADDMOD_128_128_128(4, 4, 3),
    ADDMOD_192_32_32(6, 1, 1),
    ADDMOD_192_64_32(6, 2, 1),
    ADDMOD_192_64_64(6, 2, 2),
    ADDMOD_192_128_32(6, 4, 1),
    ADDMOD_192_128_64(6, 4, 2),
    ADDMOD_192_128_128(6, 4, 4),
    ADDMOD_192_192_32(6, 6, 1),
    ADDMOD_192_192_64(6, 6, 2),
    ADDMOD_192_192_128(6, 6, 4),
    ADDMOD_192_192_192(6, 6, 6),
    ADDMOD_256_32_32(8, 1, 1),
    ADDMOD_256_64_32(8, 2, 1),
    ADDMOD_256_64_64(8, 2, 2),
    ADDMOD_256_128_32(8, 4, 1),
    ADDMOD_256_128_64(8, 4, 2),
    ADDMOD_256_128_128(8, 4, 4),
    ADDMOD_256_192_32(8, 6, 1),
    ADDMOD_256_192_64(8, 6, 2),
    ADDMOD_256_192_128(8, 6, 4),
    ADDMOD_256_192_192(8, 6, 6),
    ADDMOD_256_256_32(8, 8, 1),
    ADDMOD_256_256_64(8, 8, 2),
    ADDMOD_256_256_128(8, 8, 4),
    ADDMOD_256_256_192(8, 8, 6),
    ADDMOD_256_256_256(8, 8, 8),
    LARGER_ADDMOD_64_64_128(2, 2, 4),
    LARGER_ADDMOD_192_192_256(6, 6, 8),
    ZERO_ADDMOD_128_256_0(4, 8, 0),
    FULL_RANDOM(-1, -1, -1);

    final int aSize;
    final int bSize;
    final int cSize;

    Case(final int aSize, final int bSize, final int cSize) {
      this.aSize = aSize;
      this.bSize = bSize;
      this.cSize = cSize;
    }
  }

  @Param({
    "ADDMOD_32_32_32",
    "ADDMOD_64_32_32",
    "ADDMOD_64_64_32",
    "ADDMOD_64_64_64",
    "ADDMOD_128_32_32",
    "ADDMOD_128_64_32",
    "ADDMOD_128_64_64",
    "ADDMOD_128_128_32",
    "ADDMOD_128_128_64",
    "ADDMOD_128_128_128",
    "ADDMOD_192_32_32",
    "ADDMOD_192_64_32",
    "ADDMOD_192_64_64",
    "ADDMOD_192_128_32",
    "ADDMOD_192_128_64",
    "ADDMOD_192_128_128",
    "ADDMOD_192_192_32",
    "ADDMOD_192_192_64",
    "ADDMOD_192_192_128",
    "ADDMOD_192_192_192",
    "ADDMOD_256_32_32",
    "ADDMOD_256_64_32",
    "ADDMOD_256_64_64",
    "ADDMOD_256_128_32",
    "ADDMOD_256_128_64",
    "ADDMOD_256_128_128",
    "ADDMOD_256_192_32",
    "ADDMOD_256_192_64",
    "ADDMOD_256_192_128",
    "ADDMOD_256_192_192",
    "ADDMOD_256_256_32",
    "ADDMOD_256_256_64",
    "ADDMOD_256_256_128",
    "ADDMOD_256_256_192",
    "ADDMOD_256_256_256",
    "LARGER_ADDMOD_64_64_128",
    "LARGER_ADDMOD_192_192_256",
    "ZERO_ADDMOD_128_256_0",
    "FULL_RANDOM"
  })
  private String caseName;

  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();

    Case scenario = Case.valueOf(caseName);
    aPool = new Bytes[SAMPLE_SIZE];
    bPool = new Bytes[SAMPLE_SIZE];
    mPool = new Bytes[SAMPLE_SIZE];

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    int aSize;
    int bSize;
    int cSize;

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      if (scenario.aSize < 0) aSize = random.nextInt(1, 33);
      else aSize = scenario.aSize * 4;
      if (scenario.bSize < 0) bSize = random.nextInt(1, 33);
      else bSize = scenario.bSize * 4;
      if (scenario.cSize < 0) cSize = random.nextInt(1, 33);
      else cSize = scenario.cSize * 4;

      final byte[] a = new byte[aSize];
      final byte[] b = new byte[bSize];
      final byte[] c = new byte[cSize];
      random.nextBytes(a);
      random.nextBytes(b);
      random.nextBytes(c);
      aPool[i] = Bytes.wrap(a);
      bPool[i] = Bytes.wrap(b);
      mPool[i] = Bytes.wrap(c);
    }
    index = 0;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return AddModOperationOptimized.staticOperation(frame);
  }
}
