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
import org.hyperledger.besu.evm.operation.Operation;

/** The Shl (Shift Left) operation. */
public class ShlOperationV2 extends AbstractFixedCostOperationV2 {

  /** The Shl operation success result. */
  static final OperationResult shlSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new Shl operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ShlOperationV2(final GasCalculator gasCalculator) {
    super(0x1b, "SHL", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame);
  }

  /**
   * Performs Shift Left operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItems(2)) return UNDERFLOW_RESPONSE;
    long[] stack = frame.stackDataV2();
    int top = frame.stackTopV2();
    final int shiftOffset = (--top) << 2;
    final UInt256 shift =
        new UInt256(
            stack[shiftOffset],
            stack[shiftOffset + 1],
            stack[shiftOffset + 2],
            stack[shiftOffset + 3]);
    final int valueOffset = (--top) << 2;
    final UInt256 value =
        new UInt256(
            stack[valueOffset],
            stack[valueOffset + 1],
            stack[valueOffset + 2],
            stack[valueOffset + 3]);
    final UInt256 result = value.shl(shift);
    int resultOffset = top << 2;
    stack[resultOffset] = result.u3();
    stack[resultOffset + 1] = result.u2();
    stack[resultOffset + 2] = result.u1();
    stack[resultOffset + 3] = result.u0();
    frame.setTopV2(++top);
    return shlSuccess;
  }
}
