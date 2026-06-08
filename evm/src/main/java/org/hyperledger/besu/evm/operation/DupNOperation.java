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
import org.hyperledger.besu.evm.internal.OverflowException;
import org.hyperledger.besu.evm.internal.UnderflowException;

/**
 * The DUPN operation (EIP-8024).
 *
 * <p>Duplicates the n'th stack item to the top of the stack, where n is decoded from a 1-byte
 * immediate operand. This extends the functionality of DUP1-DUP16 to allow accessing stack items at
 * depths 17-235.
 *
 * <p>The immediate operand uses a special encoding to preserve backward compatibility by avoiding
 * bytes that could be confused with JUMPDEST (0x5b) or PUSH opcodes (0x60-0x7f).
 */
public class DupNOperation extends AbstractFixedCostOperation {

  /** The DUPN opcode value. */
  public static final int OPCODE = 0xe6;

  /** Pre-computed success result with pcIncrement = 2. */
  static final OperationResult DUPN_SUCCESS = new OperationResult(3, null, 2);

  /** Pre-computed invalid immediate result. */
  static final OperationResult INVALID_IMMEDIATE =
      new OperationResult(3, ExceptionalHaltReason.INVALID_OPERATION, 2);

  /** Pre-computed underflow result with pcIncrement = 2. */
  static final OperationResult UNDERFLOW_RESPONSE =
      new OperationResult(3, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS, 2);

  /** Pre-computed overflow result with pcIncrement = 2. */
  static final OperationResult OVERFLOW_RESPONSE =
      new OperationResult(3, ExceptionalHaltReason.TOO_MANY_STACK_ITEMS, 2);

  /**
   * Instantiates a new DUPN operation.
   *
   * @param gasCalculator the gas calculator
   */
  public DupNOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "DUPN", 0, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame, frame.getCode().getBytes().toArrayUnsafe(), frame.getPC());
  }

  /**
   * Performs DUPN operation directly for hot-path execution.
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

    // Check for invalid immediate range (91-127)
    if (!Eip8024Decoder.VALID_SINGLE[imm]) {
      return INVALID_IMMEDIATE;
    }

    final int n = Eip8024Decoder.DECODE_SINGLE[imm];

    try {
      // Duplicate the n'th stack item (1-indexed) to the top
      // In Besu's 0-indexed stack, the n'th item is at index n-1
      frame.pushStackItem(frame.getStackItem(n - 1));
      return DUPN_SUCCESS;
    } catch (final UnderflowException ufe) {
      return UNDERFLOW_RESPONSE;
    } catch (final OverflowException ofe) {
      return OVERFLOW_RESPONSE;
    }
  }

  /**
   * Decodes a single immediate byte to the stack index n.
   *
   * @param imm the immediate byte value (0-255)
   * @return the decoded n value, or -1 if the immediate is invalid
   */
  public static int decodeSingle(final int imm) {
    return Eip8024Decoder.decodeSingle(imm);
  }
}
