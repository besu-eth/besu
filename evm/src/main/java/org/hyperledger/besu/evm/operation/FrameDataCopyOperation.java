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

import static org.hyperledger.besu.evm.internal.Words.clampedToLong;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-8141 FRAMEDATACOPY operation (0xb2): copies a chosen frame's data into memory, with
 * CALLDATACOPY gas and semantics.
 */
public class FrameDataCopyOperation extends AbstractOperation {

  /** The FRAMEDATACOPY opcode. */
  public static final int OPCODE = 0xb2;

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public FrameDataCopyOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "FRAMEDATACOPY", 4, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return new OperationResult(0, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final FrameTransactionContext context = maybeContext.get();

    final long memOffset = clampedToLong(frame.popStackItem());
    final long sourceOffset = clampedToLong(frame.popStackItem());
    final long numBytes = clampedToLong(frame.popStackItem());
    final Bytes frameIndexWord = frame.popStackItem().trimLeadingZeros();

    final long cost = gasCalculator().dataCopyOperationGasCost(frame, memOffset, numBytes);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    if (frameIndexWord.size() > 4) {
      return new OperationResult(cost, ExceptionalHaltReason.OUT_OF_BOUNDS);
    }
    final int frameIndex = frameIndexWord.isEmpty() ? 0 : frameIndexWord.toInt();
    if (frameIndex < 0 || frameIndex >= context.frames().size()) {
      return new OperationResult(cost, ExceptionalHaltReason.OUT_OF_BOUNDS);
    }
    final Bytes data = context.frames().get(frameIndex).data();

    if (numBytes == 0) {
      return new OperationResult(cost, null);
    }
    frame.writeMemory(memOffset, sourceOffset, numBytes, data, true);
    return new OperationResult(cost, null);
  }
}
