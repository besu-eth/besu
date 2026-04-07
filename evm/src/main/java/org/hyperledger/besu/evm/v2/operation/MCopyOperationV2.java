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
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.StackArithmetic;

/** The MCOPY operation for EVM v2. */
public class MCopyOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new MCOPY operation.
   *
   * @param gasCalculator the gas calculator
   */
  public MCopyOperationV2(final GasCalculator gasCalculator) {
    super(0x5e, "MCOPY", 3, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2(), gasCalculator());
  }

  /**
   * Execute the MCOPY opcode on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param stack the stack operands as a long[] array
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static Operation.OperationResult staticOperation(
      final MessageFrame frame, final long[] stack, final GasCalculator gasCalculator) {
    if (!frame.stackHasItems(3)) {
      return new OperationResult(0, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    final int top = frame.stackTopV2();
    final long dst = StackArithmetic.clampedToLong(stack, top, 0);
    final long src = StackArithmetic.clampedToLong(stack, top, 1);
    final long length = StackArithmetic.clampedToLong(stack, top, 2);
    frame.setTopV2(top - 3);

    final long cost = gasCalculator.dataCopyOperationGasCost(frame, Math.max(src, dst), length);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    frame.copyMemory(dst, src, length, true);
    return new OperationResult(cost, null);
  }
}
