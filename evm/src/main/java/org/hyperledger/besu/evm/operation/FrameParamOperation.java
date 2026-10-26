/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;

import org.apache.tuweni.bytes.Bytes;

/** The EIP-8141 FRAMEPARAM operation (0xb3): frame-scoped introspection. */
public class FrameParamOperation extends AbstractOperation {

  /** The FRAMEPARAM opcode. */
  public static final int OPCODE = 0xb3;

  private static final long GAS_COST = 2L;

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public FrameParamOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "FRAMEPARAM", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final FrameTransactionContext context = maybeContext.get();

    final Bytes frameIndexWord = frame.popStackItem().trimLeadingZeros();
    final Bytes paramBytes = frame.popStackItem().trimLeadingZeros();

    if (frameIndexWord.size() > 4) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
    }
    final int frameIndex = frameIndexWord.isEmpty() ? 0 : frameIndexWord.toInt();
    if (frameIndex < 0 || frameIndex >= context.frames().size()) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
    }
    if (paramBytes.size() > 1) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final int param = paramBytes.isEmpty() ? 0 : Byte.toUnsignedInt(paramBytes.get(0));
    final FrameTransactionContext.FrameInfo frameInfo = context.frames().get(frameIndex);

    // Receipt fields are defined only for completed frames.
    if ((param == 0x05 || param == 0x0A || param == 0x0B)
        && frameIndex >= context.currentFrameIndex()) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
    }

    final Bytes result =
        switch (param) {
          case 0x00 -> frameInfo.resolvedTarget().getBytes();
          case 0x01 -> Words.longBytes(frameInfo.executionGasLimit());
          case 0x02 -> Words.longBytes(frameInfo.mode());
          case 0x03 -> Words.longBytes(frameInfo.flags());
          case 0x04 -> Words.longBytes(frameInfo.data().size());
          case 0x05 -> Words.longBytes(context.frameStatus(frameIndex));
          case 0x06 ->
              Words.longBytes(frameInfo.flags() & FrameTransactionContext.APPROVE_SCOPE_MASK);
          case 0x07 ->
              Words.longBytes(
                  (frameInfo.flags() & FrameTransactionContext.ATOMIC_BATCH_FLAG) != 0 ? 1 : 0);
          case 0x08 -> frameInfo.value().toBytes();
          case 0x09 -> Words.longBytes(frameInfo.stateGasLimit());
          case 0x0A -> Words.longBytes(context.frameExecutionGasUsed(frameIndex));
          case 0x0B -> Words.longBytes(context.frameStateGasUsed(frameIndex));
          default -> null;
        };
    if (result == null) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    frame.pushStackItem(result);
    return new OperationResult(GAS_COST, null);
  }
}
