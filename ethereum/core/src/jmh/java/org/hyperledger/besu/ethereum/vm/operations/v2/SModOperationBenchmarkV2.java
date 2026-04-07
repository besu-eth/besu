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
import org.hyperledger.besu.evm.v2.operation.SModOperationV2;

import java.math.BigInteger;
import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

public class SModOperationBenchmarkV2 extends BinaryOperationBenchmarkV2 {

  public enum Case {
    SMOD_32_32(1, 1),
    SMOD_64_32(2, 1),
    SMOD_64_64(2, 2),
    SMOD_128_32(4, 1),
    SMOD_128_64(4, 2),
    SMOD_128_128(4, 4),
    SMOD_192_32(6, 1),
    SMOD_192_64(6, 2),
    SMOD_192_128(6, 4),
    SMOD_192_192(6, 6),
    SMOD_256_32(8, 1),
    SMOD_256_64(8, 2),
    SMOD_256_128(8, 4),
    SMOD_256_192(8, 6),
    SMOD_256_256(8, 8),
    LARGER_SMOD_64_128(2, 4),
    LARGER_SMOD_192_256(6, 8),
    ZERO_SMOD_128_0(4, 0),
    FULL_RANDOM(-1, -1);

    final int divSize;
    final int modSize;

    Case(final int divSize, final int modSize) {
      this.divSize = divSize;
      this.modSize = modSize;
    }
  }

  @Param({
    "SMOD_32_32",
    "SMOD_64_32",
    "SMOD_64_64",
    "SMOD_128_32",
    "SMOD_128_64",
    "SMOD_128_128",
    "SMOD_192_32",
    "SMOD_192_64",
    "SMOD_192_128",
    "SMOD_192_192",
    "SMOD_256_32",
    "SMOD_256_64",
    "SMOD_256_128",
    "SMOD_256_192",
    "SMOD_256_256",
    "LARGER_SMOD_64_128",
    "LARGER_SMOD_192_256",
    "ZERO_SMOD_128_0",
    "FULL_RANDOM"
  })
  private String caseName;

  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelperV2.createMessageCallFrame();

    Case scenario = Case.valueOf(caseName);
    aPool = new UInt256[SAMPLE_SIZE];
    bPool = new UInt256[SAMPLE_SIZE];

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    int aSize;
    int bSize;

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      if (scenario.divSize < 0) aSize = random.nextInt(1, 33);
      else aSize = scenario.divSize * 4;
      if (scenario.modSize < 0) bSize = random.nextInt(1, 33);
      else bSize = scenario.modSize * 4;

      final byte[] a = new byte[aSize];
      final byte[] b = new byte[bSize];
      random.nextBytes(a);
      random.nextBytes(b);

      if (scenario.divSize != scenario.modSize) {
        aPool[i] = BenchmarkHelperV2.bytesToUInt256(a);
        bPool[i] = BenchmarkHelperV2.bytesToUInt256(b);
      } else {
        BigInteger aInt = new BigInteger(a);
        BigInteger bInt = new BigInteger(b);
        if (aInt.abs().compareTo(bInt.abs()) < 0) {
          aPool[i] = BenchmarkHelperV2.bytesToUInt256(b);
          bPool[i] = BenchmarkHelperV2.bytesToUInt256(a);
        } else {
          aPool[i] = BenchmarkHelperV2.bytesToUInt256(a);
          bPool[i] = BenchmarkHelperV2.bytesToUInt256(b);
        }
      }
    }
    index = 0;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return SModOperationV2.staticOperation(frame, frame.stackDataV2());
  }
}
