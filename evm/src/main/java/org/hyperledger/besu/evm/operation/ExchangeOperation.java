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

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.UnderflowException;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EXCHANGE operation (EIP-8024).
 *
 * <p>Swaps the (n+1)'th stack item with the (m+1)'th stack item, where n and m are decoded from a
 * 1-byte immediate operand. This allows arbitrary stack element swapping without going through the
 * top of the stack.
 *
 * <p>The immediate operand uses a special encoding to preserve backward compatibility by avoiding
 * bytes that could be confused with JUMPDEST (0x5b) or PUSH opcodes (0x60-0x7f).
 */
public class ExchangeOperation extends AbstractFixedCostOperation {

  /** The EXCHANGE opcode value. */
  public static final int OPCODE = 0xe8;

  /** Pre-computed success result with pcIncrement = 2. */
  static final OperationResult EXCHANGE_SUCCESS = new OperationResult(3, null, 2);

  /** Pre-computed invalid immediate result. */
  static final OperationResult INVALID_IMMEDIATE =
      new OperationResult(3, ExceptionalHaltReason.INVALID_OPERATION, 2);

  /** Pre-computed underflow result with pcIncrement = 2. */
  static final OperationResult UNDERFLOW_RESPONSE =
      new OperationResult(3, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS, 2);

  /**
   * Instantiates a new EXCHANGE operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ExchangeOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "EXCHANGE", 0, 0, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame, frame.getCode().getBytes().toArrayUnsafe(), frame.getPC());
  }

  /**
   * Performs EXCHANGE operation directly for hot-path execution.
   *
   * @param frame the message frame
   * @param code the bytecode array
   * @param pc the current program counter
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final byte[] code, final int pc) {
    // Get immediate byte, treating end-of-code as 0
    final int imm = (pc + 1 >= code.length) ? 0 : code[pc + 1] & 0xFF;

    // Single lookup for validity and both indices
    final int packed = Eip8024Decoder.DECODE_PAIR_PACKED[imm];

    // Check for invalid immediate range (80-127)
    if (packed == Eip8024Decoder.INVALID_PAIR) {
      return INVALID_IMMEDIATE;
    }

    // Extract n and m from packed value
    final int n = packed & 0xFF;
    final int m = (packed >>> 8) & 0xFF;

    try {
      // Swap the (n+1)'th item (index n) with the (m+1)'th item (index m)
      // In Besu's 0-indexed stack, (n+1)'th is index n, (m+1)'th is index m
      final Bytes itemN = frame.getStackItem(n);
      final Bytes itemM = frame.getStackItem(m);
      frame.setStackItem(n, itemM);
      frame.setStackItem(m, itemN);
      return EXCHANGE_SUCCESS;
    } catch (final UnderflowException ufe) {
      return UNDERFLOW_RESPONSE;
    }
  }

  /**
   * Decodes a pair immediate byte to the stack indices n and m.
   *
   * @param imm the immediate byte value (0-255)
   * @return an array of [n, m], or null if the immediate is invalid
   */
  public static int[] decodePair(final int imm) {
    return Eip8024Decoder.decodePair(imm);
  }
}
