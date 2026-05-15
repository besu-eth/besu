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
import org.hyperledger.besu.evm.operation.DivOperationOptimized;
import org.hyperledger.besu.evm.operation.Operation;

import org.openjdk.jmh.annotations.Param;

public class DivOperationBenchmark extends BinaryArithmeticOperationBenchmark {
  @Param({
    "DIV_32_32",
    "DIV_64_32",
    "DIV_64_64",
    "DIV_128_32",
    "DIV_128_64",
    "DIV_128_128",
    "DIV_192_32",
    "DIV_192_64",
    "DIV_192_128",
    "DIV_192_192",
    "DIV_256_32",
    "DIV_256_64",
    "DIV_256_128",
    "DIV_256_192",
    "DIV_0_256",
    "DIV_64_256",
    "DIV_128_256",
    "DIV_192_256",
    "DIV_256_256",
    "DIV_RANDOM_RANDOM"
  })
  private String caseName;

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return DivOperationOptimized.staticOperation(frame);
  }

  @Override
  protected String caseName() {
    return caseName;
  }

  @Override
  protected String opCode() {
    return "DIV";
  }
}
