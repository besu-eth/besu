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

/** EVM v2 RETURN operation using long[] stack representation. */
public class ReturnOperationV2 extends AbstractOperationV2 {

  /** RETURN opcode number */
  public static final int OPCODE = 0xF3;

  /**
   * Instantiates a new Return operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ReturnOperationV2(final GasCalculator gasCalculator) {
    super(OPCODE, "RETURN", 2, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2(), gasCalculator());
  }

  /**
   * Performs Return operation.
   *
   * @param frame the frame
   * @param s the stack data array
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator) {
    if (!frame.stackHasItems(2)) {
      return new OperationResult(0, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    final int top = frame.stackTopV2();
    final long from = StackArithmetic.clampedToLong(s, top, 0);
    final long length = StackArithmetic.clampedToLong(s, top, 1);
    frame.setTopV2(top - 2);

    final long cost = gasCalculator.memoryExpansionGasCost(frame, from, length);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    frame.setOutputData(frame.readMemory(from, length));
    frame.setState(MessageFrame.State.CODE_SUCCESS);
    return new OperationResult(cost, null);
  }
}
