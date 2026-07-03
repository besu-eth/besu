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

import org.openjdk.jmh.annotations.Param;

public class AddModOperationBenchmark extends TernaryArithmeticOperationBenchmark {

  // Cases for (a + b) % c
  // Format "ADDMOD_a_b_c" - where a, b and c are the size in bits
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
    "ADDMOD_64_64_128",
    "ADDMOD_192_192_256",
    "ADDMOD_128_256_0",
    "ADDMOD_RANDOM_RANDOM_RANDOM",
    // Randomized power-of-two moduli (2^N): each range targets one UInt256.addMod limb-dispatch
    // branch so the JIT cannot constant-fold the modulus.
    "ADDMOD_256_256_POW2_1_63", // u0 limb branch
    "ADDMOD_256_256_POW2_64_127", // u1 limb branch
    "ADDMOD_256_256_POW2_128_191", // u2 limb branch — includes 2^160 address mask
    "ADDMOD_256_256_POW2_192_255", // u3 limb branch
    "ADDMOD_256_256_POW2_1_255" // full 256-bit span
  })
  private String caseName;

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return AddModOperationOptimized.staticOperation(frame);
  }

  @Override
  protected String opCode() {
    return "ADDMOD";
  }

  @Override
  protected String caseName() {
    return caseName;
  }
}
