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
import org.hyperledger.besu.evm.operation.ApproveOperation;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-8141 APPROVE operation (0xaa): exits the call frame like RETURN and updates the
 * transaction's approval context. The approval itself is {@link ApproveOperation#attemptApproval},
 * shared with the v1 operation.
 */
public class ApproveOperationV2 extends AbstractOperationV2 {

  private static final OperationResult INVALID =
      new OperationResult(0, ExceptionalHaltReason.INVALID_OPERATION);

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ApproveOperationV2(final GasCalculator gasCalculator) {
    super(ApproveOperation.OPCODE, "APPROVE", 3, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return INVALID;
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!frame.stackHasItemsV2(3)) return haltPoppingAll(frame, UNDERFLOW_RESPONSE);
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final long offset = readClampedLongAt(stack, top, 0);
    final long length = readClampedLongAt(stack, top, 1);
    final int scope = readClampedIntAt(stack, top, 2);
    frame.setTopV2(top - 3);

    final long cost = gasCalculator().memoryExpansionGasCost(frame, offset, length);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    // EIP-7906: a POST_TX frame is static, and APPROVE gets no exception there.
    if (context.isCurrentFramePostTx()) {
      return new OperationResult(cost, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    if ((scope & ~FrameTransactionContext.APPROVE_SCOPE_MASK) != 0) {
      return revert(frame, cost);
    }
    // Only the frame's resolved target, or delegated code running under its address, may approve.
    if (!frame
        .getRecipientAddress()
        .getBytes()
        .equals(context.currentFrame().resolvedTarget().getBytes())) {
      return revert(frame, cost);
    }

    return switch (ApproveOperation.attemptApproval(
        frame, context, scope, gasCalculator().stateGasCostCalculator())) {
      case SUCCESS -> {
        frame.setOutputData(frame.readMemory(offset, length));
        frame.setState(MessageFrame.State.CODE_SUCCESS);
        yield new OperationResult(cost, null);
      }
      case REVERT -> revert(frame, cost);
      case OUT_OF_STATE_GAS -> new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
      case NONCE_OVERFLOW -> new OperationResult(cost, ExceptionalHaltReason.NONCE_OVERFLOW);
    };
  }

  private static OperationResult revert(final MessageFrame frame, final long cost) {
    frame.setState(MessageFrame.State.REVERT);
    frame.setOutputData(Bytes.EMPTY);
    return new OperationResult(cost, null);
  }
}
