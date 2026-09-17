/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The Jump dest operation. */
public class JumpDestOperation extends AbstractFixedCostOperation {

  /** constant for a successful jumpdest * */
  public static final OperationResult JUMPDEST_SUCCESS = new OperationResult(1L, null);

  /** The constant OPCODE. */
  public static final int OPCODE = 0x5B;

  /**
   * Instantiates a new Jump dest operation.
   *
   * @param gasCalculator the gas calculator
   */
  public JumpDestOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "JUMPDEST", 0, 0, gasCalculator, gasCalculator.getJumpDestOperationGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return successResponse;
  }
}
