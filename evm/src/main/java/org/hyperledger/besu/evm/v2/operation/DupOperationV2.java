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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/**
 * EVM v2 DUP1-16 operation using long[] stack representation.
 *
 * <p>Each 256-bit word is stored as four longs: index 0 = most significant 64 bits, index 3 = least
 * significant 64 bits.
 */
public class DupOperationV2 extends AbstractFixedCostOperationV2 {

  /** The constant DUP_BASE: DUPn has opcode DUP_BASE + n. */
  public static final int DUP_BASE = 0x7F;

  private static final OperationResult DUP_SUCCESS = new OperationResult(3, null);

  private final int index;

  /**
   * Instantiates a new Dup operation.
   *
   * @param index the index of the duplicated item, 1-16
   * @param gasCalculator the gas calculator
   */
  public DupOperationV2(final int index, final GasCalculator gasCalculator) {
    super(
        DUP_BASE + index,
        "DUP" + index,
        index,
        index + 1,
        gasCalculator,
        gasCalculator.getVeryLowTierGasCost());
    this.index = index;
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame, index);
  }

  /**
   * Execute the DUPn opcode on the v2 long[] stack.
   *
   * <p>DUPn: stack[top] = stack[top-n], return top+1.
   *
   * @param frame the message frame
   * @param index the index of the duplicated item, 1 for the top of the stack
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final int index) {
    if (!frame.stackHasItemsV2(index)) return UNDERFLOW_RESPONSE;
    if (!frame.stackHasSpaceV2(1)) return OVERFLOW_RESPONSE;
    final int top = frame.stackTopV2();
    copyWord(frame.stackDataV2(), top - index, top);
    frame.setTopV2(top + 1);
    return DUP_SUCCESS;
  }
}
