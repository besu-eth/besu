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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.isZeroAt;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/**
 * EVM v2 JUMPI operation using long[] stack representation.
 *
 * <p>Each 256-bit word is stored as four longs: index 0 = most significant 64 bits, index 3 = least
 * significant 64 bits.
 */
public class JumpiOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult INVALID_JUMP_RESPONSE =
      new OperationResult(10L, ExceptionalHaltReason.INVALID_JUMP_DESTINATION);
  private static final OperationResult JUMPI_RESPONSE = new OperationResult(10L, null, 0);
  private static final OperationResult NO_JUMP_RESPONSE = new OperationResult(10L, null);

  /**
   * Instantiates a new Jumpi operation.
   *
   * @param gasCalculator the gas calculator
   */
  public JumpiOperationV2(final GasCalculator gasCalculator) {
    super(0x57, "JUMPI", 2, 0, gasCalculator, gasCalculator.getHighTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Execute the JUMPI opcode on the v2 long[] stack.
   *
   * <p>JUMPI: pc = stack[top-1] if stack[top-2] is not zero and stack[top-1] is a valid jump
   * destination, return top-2.
   *
   * @param frame the message frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItemsV2(2)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    frame.setTopV2(top - 2);
    // A zero condition does not jump, so the destination is not checked.
    if (isZeroAt(stack, top, 1)) {
      return NO_JUMP_RESPONSE;
    }
    return JumpOperationV2.performJump(
        frame, stack, (top - 1) << 2, JUMPI_RESPONSE, INVALID_JUMP_RESPONSE);
  }
}
