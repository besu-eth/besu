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

/**
 * EVM v2 CODECOPY operation (0x39).
 *
 * <p>Pops destOffset, sourceOffset, and size from the stack, then copies {@code size} bytes of the
 * current executing contract's code into memory at {@code destOffset}.
 */
public class CodeCopyOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new CodeCopy operation.
   *
   * @param gasCalculator the gas calculator
   */
  public CodeCopyOperationV2(final GasCalculator gasCalculator) {
    super(0x39, "CODECOPY", 3, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2(), gasCalculator());
  }

  /**
   * Execute CODECOPY on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack data
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator) {
    if (!frame.stackHasItems(3)) {
      return new OperationResult(0, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    final int top = frame.stackTopV2();
    final long memOffset = StackArithmetic.clampedToLong(s, top, 0);
    final long sourceOffset = StackArithmetic.clampedToLong(s, top, 1);
    final long numBytes = StackArithmetic.clampedToLong(s, top, 2);
    frame.setTopV2(top - 3);

    final long cost = gasCalculator.dataCopyOperationGasCost(frame, memOffset, numBytes);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    frame.writeMemory(memOffset, sourceOffset, numBytes, frame.getCode().getBytes(), true);

    return new OperationResult(cost, null);
  }
}
