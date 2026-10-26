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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes32;

/**
 * The EIP-8141 FRAMEDATALOAD operation (0xb1): loads one 32-byte word from a chosen frame's data,
 * with CALLDATALOAD semantics.
 */
public class FrameDataLoadOperation extends AbstractOperation {

  /** The FRAMEDATALOAD opcode. */
  public static final int OPCODE = 0xb1;

  private static final long GAS_COST = 3L;

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public FrameDataLoadOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "FRAMEDATALOAD", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final FrameTransactionContext context = maybeContext.get();

    final Bytes offsetWord = frame.popStackItem().trimLeadingZeros();
    final Bytes frameIndexWord = frame.popStackItem().trimLeadingZeros();

    if (frameIndexWord.size() > 4) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
    }
    final int frameIndex = frameIndexWord.isEmpty() ? 0 : frameIndexWord.toInt();
    if (frameIndex < 0 || frameIndex >= context.frames().size()) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
    }
    final Bytes data = context.frames().get(frameIndex).data();

    if (offsetWord.size() > 4) {
      frame.pushStackItem(Bytes.EMPTY);
      return new OperationResult(GAS_COST, null);
    }
    final int offset = offsetWord.isEmpty() ? 0 : offsetWord.toInt();
    if (offset < 0 || offset >= data.size()) {
      frame.pushStackItem(Bytes.EMPTY);
      return new OperationResult(GAS_COST, null);
    }
    final MutableBytes32 res = MutableBytes32.create();
    final Bytes toCopy = data.slice(offset, Math.min(Bytes32.SIZE, data.size() - offset));
    toCopy.copyTo(res, 0);
    frame.pushStackItem(res.copy());
    return new OperationResult(GAS_COST, null);
  }
}
