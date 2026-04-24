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
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/**
 * EVM v2 SUB operation using long[] stack representation.
 *
 * <p>Each 256-bit word is stored as four longs: index 0 = most significant 64 bits, index 3 = least
 * significant 64 bits.
 */
public class SubOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult SUB_SUCCESS = new OperationResult(3, null);

  /**
   * Instantiates a new Sub operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SubOperationV2(final GasCalculator gasCalculator) {
    super(0x03, "SUB", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame);
  }

  /**
   * Execute the SUB opcode on the v2 long[] stack.
   *
   * <p>SUB: stack[top-2] = stack[top-1] - stack[top-2], return top-1.
   *
   * @param frame the message frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItemsV2(2)) return UNDERFLOW_RESPONSE;
    long[] stack = frame.stackDataV2();
    int top = frame.stackTopV2();
    final int aOffset = (top - 1) << 2;
    final int bOffset = (top - 2) << 2;

    final UInt256 valueA =
        new UInt256(stack[aOffset], stack[aOffset + 1], stack[aOffset + 2], stack[aOffset + 3]);
    final UInt256 valueB =
        new UInt256(stack[bOffset], stack[bOffset + 1], stack[bOffset + 2], stack[bOffset + 3]);

    final UInt256 r = valueA.sub(valueB);

    stack[bOffset] = r.u3();
    stack[bOffset + 1] = r.u2();
    stack[bOffset + 2] = r.u1();
    stack[bOffset + 3] = r.u0();

    frame.setTopV2(top - 1);
    return SUB_SUCCESS;
  }
}
