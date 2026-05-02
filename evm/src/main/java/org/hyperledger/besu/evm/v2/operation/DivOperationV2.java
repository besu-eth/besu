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

/** The Div operation. */
public class DivOperationV2 extends AbstractFixedCostOperationV2 {

  /** The Div success. */
  static final OperationResult divSuccess = new OperationResult(5, null);

  /**
   * Instantiates a new Div operation.
   *
   * @param gasCalculator the gas calculator
   */
  public DivOperationV2(final GasCalculator gasCalculator) {
    super(0x04, "DIV", 2, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame);
  }

  /**
   * Performs Div operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItemsV2(2)) {
      return UNDERFLOW_RESPONSE;
    }
    long[] stack = frame.stackDataV2();
    int top = frame.stackTopV2();
    final int numOffset = (--top) << 2;
    final UInt256 num =
        new UInt256(
            stack[numOffset], stack[numOffset + 1], stack[numOffset + 2], stack[numOffset + 3]);
    final int denomOffset = (--top) << 2;
    final UInt256 denom =
        new UInt256(
            stack[denomOffset],
            stack[denomOffset + 1],
            stack[denomOffset + 2],
            stack[denomOffset + 3]);
    final UInt256 result = num.div(denom);
    stack[denomOffset] = result.u3();
    stack[denomOffset + 1] = result.u2();
    stack[denomOffset + 2] = result.u1();
    stack[denomOffset + 3] = result.u0();
    frame.setTopV2(++top);
    return divSuccess;
  }
}
