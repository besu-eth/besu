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
import org.hyperledger.besu.evm.operation.MulModOperationOptimized;
import org.hyperledger.besu.evm.operation.Operation;

import org.openjdk.jmh.annotations.Param;

public class MulModOperationBenchmark extends TernaryArithmeticOperationBenchmark {

  // Cases for (a * b) % c
  // Format "MULMOD_a_b_c" - where a, b and c are the size in bits
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
    "MULMOD_64_64_128",
    "MULMOD_192_192_256",
    "MULMOD_128_256_0",
    "MULMOD_RANDOM_RANDOM_RANDOM"
  })
  private String caseName;

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return MulModOperationOptimized.staticOperation(frame);
  }

  @Override
  protected String caseName() {
    return caseName;
  }

  @Override
  protected String opCode() {
    return "MULMOD";
  }
}
