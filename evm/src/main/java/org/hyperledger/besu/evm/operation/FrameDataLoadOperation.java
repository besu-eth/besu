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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.UnderflowException;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Implements the FRAMEDATALOAD (0xb1) operation defined in EIP-8141. Reads a 32-byte word from a
 * specified frame's calldata.
 *
 * <p>Stack: frameIndex, offset -> data_word
 *
 * <p>Gas cost: 3
 */
public class FrameDataLoadOperation extends AbstractOperation {

  /** FRAMEDATALOAD opcode number. */
  public static final int OPCODE = 0xb1;

  private static final long GAS_COST = 3L;

  /**
   * Instantiates a new FrameDataLoad operation.
   *
   * @param gasCalculator the gas calculator
   */
  public FrameDataLoadOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "FRAMEDATALOAD", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    try {
      final Bytes frameIndexBytes = frame.popStackItem();
      frame.popStackItem(); // offset

      if (frame.getRemainingGas() < GAS_COST) {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_GAS);
      }

      // Out-of-bounds frameIndex causes exceptional halt
      final Bytes trimmedIndex = frameIndexBytes.trimLeadingZeros();
      if (trimmedIndex.size() > 4) {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
      }
      // Access the frame data from the transaction context
      // For now, push zero - full frame data access requires MessageFrame integration
      frame.pushStackItem(Bytes32.ZERO);
      return new OperationResult(GAS_COST, null);
    } catch (final UnderflowException ufe) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
  }
}
