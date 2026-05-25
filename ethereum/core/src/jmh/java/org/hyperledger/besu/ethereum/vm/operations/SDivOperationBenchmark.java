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
import org.hyperledger.besu.evm.operation.SDivOperationOptimized;

import org.openjdk.jmh.annotations.Param;

public class SDivOperationBenchmark extends SignedBinaryArithmeticOperationBenchmark {
  @Param({
    "SDIV_32_32",
    "SDIV_64_32",
    "SDIV_64_64",
    "SDIV_128_32",
    "SDIV_128_64",
    "SDIV_128_128",
    "SDIV_192_32",
    "SDIV_192_64",
    "SDIV_192_128",
    "SDIV_192_192",
    "SDIV_256_32",
    "SDIV_256_64",
    "SDIV_256_128",
    "SDIV_256_192",
    "SDIV_256_256",
    "SDIV_0_256",
    "SDIV_64_256",
    "SDIV_128_256",
    "SDIV_192_256",
    "SDIV_RANDOM_RANDOM"
  })
  private String caseName;

  @Override
  protected String opCode() {
    return "SDIV";
  }

  @Override
  protected String caseName() {
    return caseName;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return SDivOperationOptimized.staticOperation(frame);
  }
}
