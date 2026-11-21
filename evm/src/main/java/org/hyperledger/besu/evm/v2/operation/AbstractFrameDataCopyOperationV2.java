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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedIntAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedLongAt;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-8141 copy operations: copy the bytes an index selects into memory, with CALLDATACOPY gas
 * and semantics.
 */
abstract class AbstractFrameDataCopyOperationV2 extends AbstractOperationV2 {

  private static final OperationResult INVALID =
      new OperationResult(0, ExceptionalHaltReason.INVALID_OPERATION);

  /**
   * Instantiates the operation.
   *
   * @param opcode the opcode
   * @param name the name
   * @param gasCalculator the gas calculator
   */
  protected AbstractFrameDataCopyOperationV2(
      final int opcode, final String name, final GasCalculator gasCalculator) {
    super(opcode, name, 4, 0, gasCalculator);
  }

  /**
   * Checks the index operand.
   *
   * @param context the frame transaction context
   * @param index the index operand, clamped to {@link Integer#MAX_VALUE}
   * @return the halt reason, or null if the index selects bytes to copy
   */
  protected abstract ExceptionalHaltReason check(FrameTransactionContext context, int index);

  /**
   * Returns the bytes an index selects, once {@link #check} has accepted it.
   *
   * @param context the frame transaction context
   * @param index the index operand
   * @return the bytes to copy
   */
  protected abstract Bytes data(FrameTransactionContext context, int index);

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return INVALID;
    }
    if (!frame.stackHasItemsV2(4)) return haltPoppingAll(frame, UNDERFLOW_RESPONSE);
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final long memOffset = readClampedLongAt(stack, top, 0);
    final long sourceOffset = readClampedLongAt(stack, top, 1);
    final long numBytes = readClampedLongAt(stack, top, 2);
    final int index = readClampedIntAt(stack, top, 3);
    frame.setTopV2(top - 4);

    final long cost = gasCalculator().dataCopyOperationGasCost(frame, memOffset, numBytes);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    final FrameTransactionContext context = maybeContext.get();
    final ExceptionalHaltReason halt = check(context, index);
    if (halt != null) {
      return new OperationResult(cost, halt);
    }
    if (numBytes != 0) {
      frame.writeMemory(memOffset, sourceOffset, numBytes, data(context, index), true);
    }
    return new OperationResult(cost, null);
  }
}
