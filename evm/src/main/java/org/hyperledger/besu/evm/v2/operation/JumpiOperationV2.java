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
import org.hyperledger.besu.evm.operation.Operation;

/** EVM v2 JUMPI (conditional jump) operation using long[] stack representation. */
public class JumpiOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult INVALID_JUMP_RESPONSE =
      new OperationResult(10L, ExceptionalHaltReason.INVALID_JUMP_DESTINATION);
  private static final OperationResult JUMPI_RESPONSE = new OperationResult(10L, null, 0);
  private static final OperationResult NOJUMP_RESPONSE = new OperationResult(10L, null);

  /**
   * Instantiates a new JUMPI operation.
   *
   * @param gasCalculator the gas calculator
   */
  public JumpiOperationV2(final GasCalculator gasCalculator) {
    super(0x57, "JUMPI", 2, 0, gasCalculator, gasCalculator.getHighTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Performs JUMPI operation.
   *
   * @param frame the frame
   * @param s the stack data array
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final long[] s) {
    if (!frame.stackHasItems(2)) return UNDERFLOW_RESPONSE;
    final int top = frame.stackTopV2();
    final int destOff = (top - 1) << 2;
    final int condOff = (top - 2) << 2;
    frame.setTopV2(top - 2);

    // If condition is zero (false), no jump will be performed.
    if (s[condOff] == 0 && s[condOff + 1] == 0 && s[condOff + 2] == 0 && s[condOff + 3] == 0) {
      return NOJUMP_RESPONSE;
    }

    return JumpOperationV2.performJump(frame, s, destOff, JUMPI_RESPONSE, INVALID_JUMP_RESPONSE);
  }
}
