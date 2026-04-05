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

/** The MSTORE8 operation for EVM v2. */
public class Mstore8OperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new MSTORE8 operation.
   *
   * @param gasCalculator the gas calculator
   */
  public Mstore8OperationV2(final GasCalculator gasCalculator) {
    super(0x53, "MSTORE8", 2, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2(), gasCalculator());
  }

  /**
   * Execute the MSTORE8 opcode on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param stack the stack operands as a long[] array
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static Operation.OperationResult staticOperation(
      final MessageFrame frame, final long[] stack, final GasCalculator gasCalculator) {
    if (!frame.stackHasItems(2)) {
      return new OperationResult(0, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    final int top = frame.stackTopV2();
    final long location = StackArithmetic.clampedToLong(stack, top, 0);
    final byte theByte = (byte) (StackArithmetic.longAt(stack, top, 1) & 0xFF);

    final long cost = gasCalculator.mStore8OperationGasCost(frame, location);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    frame.writeMemory(location, theByte, true);
    frame.setTopV2(top - 2);
    return new OperationResult(cost, null);
  }
}
