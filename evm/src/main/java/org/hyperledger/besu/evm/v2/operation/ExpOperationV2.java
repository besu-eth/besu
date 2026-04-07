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
import org.hyperledger.besu.evm.v2.StackArithmetic;

/** The Exp operation. */
public class ExpOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new Exp operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ExpOperationV2(final GasCalculator gasCalculator) {
    super(0x0A, "EXP", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2(), gasCalculator());
  }

  /**
   * Performs EXP operation.
   *
   * @param frame the frame
   * @param stack the stack data array
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] stack, final GasCalculator gasCalculator) {
    if (!frame.stackHasItems(2)) {
      return new OperationResult(0, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    final int byteCount = StackArithmetic.byteLengthAt(stack, frame.stackTopV2(), 1);
    final long gasCost = gasCalculator.expOperationGasCost(byteCount);
    if (frame.getRemainingGas() < gasCost) {
      return new OperationResult(gasCost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    frame.setTopV2(StackArithmetic.exp(stack, frame.stackTopV2()));
    return new OperationResult(gasCost, null);
  }
}
