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
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setAddress;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setLong;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setWei;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.FrameParamOperation;

/** The EIP-8141 FRAMEPARAM operation (0xb3): frame-scoped introspection. */
public class FrameParamOperationV2 extends AbstractOperationV2 {

  private static final long GAS_COST = 2L;
  private static final OperationResult SUCCESS = new OperationResult(GAS_COST, null);
  private static final OperationResult INVALID =
      new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
  private static final OperationResult OUT_OF_BOUNDS =
      new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public FrameParamOperationV2(final GasCalculator gasCalculator) {
    super(FrameParamOperation.OPCODE, "FRAMEPARAM", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return INVALID;
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!frame.stackHasItemsV2(2)) return haltPoppingAll(frame, UNDERFLOW_RESPONSE);
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int frameIndex = readClampedIntAt(stack, top, 0);
    final int param = readClampedIntAt(stack, top, 1);

    if (frameIndex >= context.frames().size()) {
      return haltPopping(frame, 2, OUT_OF_BOUNDS);
    }
    // Receipt fields are defined only for completed frames.
    if ((param == 0x05 || param == 0x0A || param == 0x0B)
        && frameIndex >= context.currentFrameIndex()) {
      return haltPopping(frame, 2, OUT_OF_BOUNDS);
    }
    final FrameTransactionContext.FrameInfo frameInfo = context.frames().get(frameIndex);
    final int slot = top - 2;
    switch (param) {
      case 0x00 -> setAddress(frameInfo.resolvedTarget(), stack, slot);
      case 0x01 -> setLong(frameInfo.executionGasLimit(), stack, slot);
      case 0x02 -> setLong(frameInfo.mode(), stack, slot);
      case 0x03 -> setLong(frameInfo.flags(), stack, slot);
      case 0x04 -> setLong(frameInfo.data().size(), stack, slot);
      case 0x05 -> setLong(context.frameStatus(frameIndex), stack, slot);
      case 0x06 ->
          setLong(frameInfo.flags() & FrameTransactionContext.APPROVE_SCOPE_MASK, stack, slot);
      case 0x07 ->
          setLong(
              (frameInfo.flags() & FrameTransactionContext.ATOMIC_BATCH_FLAG) != 0 ? 1 : 0,
              stack,
              slot);
      case 0x08 -> setWei(frameInfo.value(), stack, slot);
      case 0x09 -> setLong(frameInfo.stateGasLimit(), stack, slot);
      case 0x0A -> setLong(context.frameExecutionGasUsed(frameIndex), stack, slot);
      case 0x0B -> setLong(context.frameStateGasUsed(frameIndex), stack, slot);
      default -> {
        return haltPopping(frame, 2, INVALID);
      }
    }
    frame.setTopV2(top - 1);
    return SUCCESS;
  }
}
