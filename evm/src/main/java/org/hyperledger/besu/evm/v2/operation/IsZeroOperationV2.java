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
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushLong;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/**
 * EVM v2 ISZERO operation using long[] stack representation.
 *
 * <p>Each 256-bit word is stored as four longs: index 0 = most significant 64 bits, index 3 = least
 * significant 64 bits.
 */
public class IsZeroOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult ISZERO_SUCCESS = new OperationResult(3, null);

  /**
   * Instantiates a new IsZero operation.
   *
   * @param gasCalculator the gas calculator
   */
  public IsZeroOperationV2(final GasCalculator gasCalculator) {
    super(0x15, "ISZERO", 1, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Execute the ISZERO opcode on the v2 long[] stack.
   *
   * <p>ISZERO: stack[top-1] = stack[top-1] == 0 ? 1 : 0, top unchanged.
   *
   * @param frame the message frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    pushLong(isZeroAt(stack, top, 0) ? 1L : 0L, stack, top - 1);
    return ISZERO_SUCCESS;
  }
}
