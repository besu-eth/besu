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
package org.hyperledger.besu.evm.v2.operation;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** EVM v2 operation for INVALID and for every opcode a fork does not define. */
public class InvalidOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new Invalid operation.
   *
   * @param opcode the opcode
   * @param gasCalculator the gas calculator
   */
  public InvalidOperationV2(final int opcode, final GasCalculator gasCalculator) {
    super(opcode, "INVALID", -1, -1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return invalidOperationResult(getOpcode());
  }

  /**
   * Creates the result of executing an invalid opcode.
   *
   * @param opcode the invalid opcode
   * @return a result with zero gas cost that names the invalid opcode
   */
  public static OperationResult invalidOperationResult(final int opcode) {
    return new OperationResult(0, ExceptionalHaltReason.newInvalidOperation(opcode));
  }
}
