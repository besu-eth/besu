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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.setLong;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/**
 * EVM v2 EQ operation using long[] stack representation.
 *
 * <p>Each 256-bit word is stored as four longs: index 0 = most significant 64 bits, index 3 = least
 * significant 64 bits.
 */
public final class EqOperationV2 extends AbstractFixedCostOperationV2 {

  /**
   * Instantiates a new Eq operation.
   *
   * @param gasCalculator the gas calculator
   */
  public EqOperationV2(final GasCalculator gasCalculator) {
    super(0x14, "EQ", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  /**
   * Execute the EQ opcode on the v2 long[] stack.
   *
   * <p>EQ: stack[top-2] = stack[top-1] == stack[top-2] ? 1 : 0, return top-1.
   *
   * @param frame the message frame
   * @return the operation result
   */
  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    // v1 charges the cost on a stack underflow of EQ, unlike the shared UNDERFLOW_RESPONSE, and
    // traces show it
    if (!frame.stackHasItemsV2(2)) return haltPoppingAll(frame, underflowResponse);
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int aOffset = (top - 1) << 2;
    final int bOffset = (top - 2) << 2;
    final boolean equal =
        stack[aOffset] == stack[bOffset]
            && stack[aOffset + 1] == stack[bOffset + 1]
            && stack[aOffset + 2] == stack[bOffset + 2]
            && stack[aOffset + 3] == stack[bOffset + 3];
    setLong(equal ? 1L : 0L, stack, top - 2);
    frame.setTopV2(top - 1);
    return successResponse;
  }
}
