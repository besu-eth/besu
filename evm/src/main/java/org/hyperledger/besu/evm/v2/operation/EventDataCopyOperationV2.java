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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedLongAt;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.AssertionResult;
import org.hyperledger.besu.evm.operation.EventDataCopyOperation;

import java.util.Optional;

/**
 * The EIP-7906 EVENTDATACOPY operation (0xb8): copies a range of one event's data into memory. The
 * range check is {@link EventDataCopyOperation#eventData}, shared with the v1 operation. Its
 * operand order is not FRAMEDATACOPY's, so it does not share that operation's base class.
 */
public class EventDataCopyOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public EventDataCopyOperationV2(final GasCalculator gasCalculator) {
    super(EventDataCopyOperation.OPCODE, "EVENTDATACOPY", 4, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final Optional<FrameTransactionContext> maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return new OperationResult(
          gasCalculator().getVeryLowTierGasCost(), ExceptionalHaltReason.INVALID_OPERATION);
    }
    if (!frame.stackHasItemsV2(4)) return haltPoppingAll(frame, UNDERFLOW_RESPONSE);
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final long eventIndex = readClampedLongAt(stack, top, 0);
    final long memOffset = readClampedLongAt(stack, top, 1);
    final long dataOffset = readClampedLongAt(stack, top, 2);
    final long length = readClampedLongAt(stack, top, 3);
    frame.setTopV2(top - 4);

    final long cost = gasCalculator().dataCopyOperationGasCost(frame, memOffset, length);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!context.isCurrentFramePostTx()) {
      return new OperationResult(cost, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final AssertionResult result =
        EventDataCopyOperation.eventData(context, eventIndex, dataOffset, length);
    if (result.halt() != null) {
      return new OperationResult(cost, result.halt());
    }
    if (length > 0) {
      frame.writeMemory(memOffset, dataOffset, length, result.value(), true);
    }
    return new OperationResult(cost, null);
  }
}
