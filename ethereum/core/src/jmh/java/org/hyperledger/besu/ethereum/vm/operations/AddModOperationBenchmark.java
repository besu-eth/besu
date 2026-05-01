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
    "ADDMOD_RANDOM_RANDOM_RANDOM"
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
