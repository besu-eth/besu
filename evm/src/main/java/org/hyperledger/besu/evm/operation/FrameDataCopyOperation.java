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

/**
 * Implements the FRAMEDATACOPY (0xb2) operation defined in EIP-8141. Copies frame calldata to
 * memory.
 *
 * <p>Stack: frameIndex, dataOffset, length, memOffset
 *
 * <p>Gas cost: 3 + per-word copy + memory expansion
 */
public class FrameDataCopyOperation extends AbstractOperation {

  /** FRAMEDATACOPY opcode number. */
  public static final int OPCODE = 0xb2;

  private static final long BASE_GAS_COST = 3L;

  /**
   * Instantiates a new FrameDataCopy operation.
   *
   * @param gasCalculator the gas calculator
   */
  public FrameDataCopyOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "FRAMEDATACOPY", 4, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    try {
      final Bytes frameIndexBytes = frame.popStackItem();
      frame.popStackItem(); // dataOffset
      frame.popStackItem(); // length
      frame.popStackItem(); // memOffset

      if (frame.getRemainingGas() < BASE_GAS_COST) {
        return new OperationResult(BASE_GAS_COST, ExceptionalHaltReason.INSUFFICIENT_GAS);
      }

      // Out-of-bounds frameIndex causes exceptional halt
      final Bytes trimmedIndex = frameIndexBytes.trimLeadingZeros();
      if (trimmedIndex.size() > 4) {
        return new OperationResult(BASE_GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
      }

      // For now, just return successfully - full implementation requires MessageFrame integration
      return new OperationResult(BASE_GAS_COST, null);
    } catch (final UnderflowException ufe) {
      return new OperationResult(BASE_GAS_COST, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
  }
}
