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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.copyWord;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Eip8024Decoder;

/**
 * EVM v2 DUPN operation (EIP-8024) using long[] stack representation.
 *
 * <p>Duplicates the n'th stack item to the top, where n is decoded from the immediate byte.
 */
public class DupNOperationV2 extends AbstractFixedCostOperationV2 {

  /** The DUPN opcode number. */
  public static final int OPCODE = 0xe6;

  private static final OperationResult DUPN_SUCCESS = new OperationResult(3, null, 2);
  private static final OperationResult INVALID_IMMEDIATE =
      new OperationResult(3, ExceptionalHaltReason.INVALID_OPERATION, 2);
  private static final OperationResult DUPN_UNDERFLOW =
      new OperationResult(3, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS, 2);
  private static final OperationResult DUPN_OVERFLOW =
      new OperationResult(3, ExceptionalHaltReason.TOO_MANY_STACK_ITEMS, 2);

  /**
   * Instantiates a new DupN operation.
   *
   * @param gasCalculator the gas calculator
   */
  public DupNOperationV2(final GasCalculator gasCalculator) {
    super(OPCODE, "DUPN", 0, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame, frame.getCode().getBytes().toArrayUnsafe(), frame.getPC());
  }

  /**
   * Execute the DUPN opcode on the v2 long[] stack.
   *
   * <p>DUPN: stack[top] = stack[top-n], return top+1.
   *
   * @param frame the message frame
   * @param code the code being executed
   * @param pc the program counter of the DUPN
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final byte[] code, final int pc) {
    // The immediate is zero past the end of the code.
    final int imm = (pc + 1 >= code.length) ? 0 : code[pc + 1] & 0xFF;
    if (!Eip8024Decoder.VALID_SINGLE[imm]) {
      return INVALID_IMMEDIATE;
    }
    final int n = Eip8024Decoder.DECODE_SINGLE[imm];
    if (!frame.stackHasItemsV2(n)) return DUPN_UNDERFLOW;
    if (!frame.stackHasSpaceV2(1)) return DUPN_OVERFLOW;
    final int top = frame.stackTopV2();
    copyWord(frame.stackDataV2(), top - n, top);
    frame.setTopV2(top + 1);
    return DUPN_SUCCESS;
  }
}
