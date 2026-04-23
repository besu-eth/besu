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
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.SModOperationOptimized;

import org.openjdk.jmh.annotations.Param;

public class SModOperationBenchmark extends SignedBinaryArithmeticOperationBenchmark {
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
    "SMOD_64_128",
    "SMOD_192_256",
    "SMOD_128_0",
    "SMOD_RANDOM_RANDOM"
  })
  private String caseName;

  @Override
  protected String opCode() {
    return "SMOD";
  }

  @Override
  protected String caseName() {
    return caseName;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return SModOperationOptimized.staticOperation(frame);
  }
}
