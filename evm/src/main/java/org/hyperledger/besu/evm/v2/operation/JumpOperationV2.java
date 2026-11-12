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

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/**
 * EVM v2 JUMP operation using long[] stack representation.
 *
 * <p>Each 256-bit word is stored as four longs: index 0 = most significant 64 bits, index 3 = least
 * significant 64 bits.
 */
public class JumpOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult INVALID_JUMP_RESPONSE =
      new OperationResult(8L, ExceptionalHaltReason.INVALID_JUMP_DESTINATION);
  private static final OperationResult JUMP_RESPONSE = new OperationResult(8L, null, 0);
  // JUMP and the JUMPDEST it lands on: 8 + 1 gas, and the pc moves past the JUMPDEST
  private static final OperationResult JUMP_AND_JUMPDEST_RESPONSE =
      new OperationResult(9L, null, 1);

  /**
   * Instantiates a new Jump operation.
   *
   * @param gasCalculator the gas calculator
   */
  public JumpOperationV2(final GasCalculator gasCalculator) {
    super(0x56, "JUMP", 1, 0, gasCalculator, gasCalculator.getMidTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Execute the JUMP opcode on the v2 long[] stack.
   *
   * <p>JUMP: pc = stack[top-1] if it is a valid jump destination, return top-1.
   *
   * @param frame the message frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final int top = frame.stackTopV2();
    frame.setTopV2(top - 1);
    return performJump(
        frame, frame.stackDataV2(), (top - 1) << 2, JUMP_RESPONSE, INVALID_JUMP_RESPONSE);
  }

  /**
   * Execute the JUMP opcode, and the JUMPDEST a valid jump lands on, for the untraced loop. That
   * saves the loop a dispatch for the JUMPDEST. Only when the gas covers both: otherwise this jumps
   * as {@link #staticOperation(MessageFrame)} does, and the loop runs the JUMPDEST, which then
   * halts for the gas as it would in a traced run.
   *
   * @param frame the message frame
   * @param gas the gas left, which the untraced loop holds outside the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final long gas) {
    // the body of staticOperation(frame), not a call to a shared helper, to keep the inlining
    // depth of the untraced loop as it was
    final OperationResult jumpResponse = gas >= 9L ? JUMP_AND_JUMPDEST_RESPONSE : JUMP_RESPONSE;
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final int top = frame.stackTopV2();
    frame.setTopV2(top - 1);
    return performJump(
        frame, frame.stackDataV2(), (top - 1) << 2, jumpResponse, INVALID_JUMP_RESPONSE);
  }

  /**
   * Jumps to the destination word at the given limb offset if it is a JUMPDEST.
   *
   * @param frame the message frame
   * @param stack the flat limb array
   * @param destinationOffset the limb offset of the destination word
   * @param jumpResponse the result of a valid jump
   * @param invalidJumpResponse the result of a jump to anything but a JUMPDEST
   * @return the operation result
   */
  static OperationResult performJump(
      final MessageFrame frame,
      final long[] stack,
      final int destinationOffset,
      final OperationResult jumpResponse,
      final OperationResult invalidJumpResponse) {
    final long destination = stack[destinationOffset + 3];
    if ((stack[destinationOffset] | stack[destinationOffset + 1] | stack[destinationOffset + 2])
            != 0
        || Long.compareUnsigned(destination, Integer.MAX_VALUE) > 0
        || frame.getCode().isJumpDestInvalid((int) destination)) {
      return invalidJumpResponse;
    }
    frame.setPC((int) destination);
    return jumpResponse;
  }
}
