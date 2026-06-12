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
 * Implements the APPROVE (0xaa) operation defined in EIP-8141. Exits the current frame successfully
 * and updates transaction approval context.
 *
 * <p>Stack Input: scope, length (unused), offset (unused)
 *
 * <p>Scope Values: 0x0=NONE, 0x1=PAYMENT, 0x2=EXECUTION, 0x3=BOTH
 */
public class ApproveOperation extends AbstractOperation {

  /** APPROVE opcode number. */
  public static final int OPCODE = 0xaa;

  /** No approval scope. */
  public static final int APPROVE_NONE = 0x0;

  /** Payment approval only. */
  public static final int APPROVE_PAYMENT = 0x1;

  /** Execution approval only. */
  public static final int APPROVE_EXECUTION = 0x2;

  /** Both execution and payment approval. */
  public static final int APPROVE_EXECUTION_AND_PAYMENT = 0x3;

  private static final long GAS_COST = 0L;

  /**
   * Instantiates a new Approve operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ApproveOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "APPROVE", 3, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    try {
      final Bytes scopeBytes = frame.popStackItem();
      frame.popStackItem(); // length - unused but must be popped
      frame.popStackItem(); // offset - unused but must be popped

      if (frame.getRemainingGas() < GAS_COST) {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_GAS);
      }

      final Bytes trimmed = scopeBytes.trimLeadingZeros();
      final int scope = trimmed.isEmpty() ? 0 : trimmed.toInt();

      if (scope > APPROVE_EXECUTION_AND_PAYMENT) {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
      }

      // Mark the frame as approved and set the approval scope
      // The frame execution engine will read this state after frame completion
      // For now, set the frame state to indicate successful approval
      frame.setState(MessageFrame.State.COMPLETED_SUCCESS);
      frame.setOutputData(Bytes.of(scope));

      return new OperationResult(GAS_COST, null);
    } catch (final UnderflowException ufe) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
  }
}
