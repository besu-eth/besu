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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Eip8024Decoder;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.StackArithmetic;

/**
 * EVM v2 EXCHANGE operation (0xE8, EIP-8024).
 *
 * <p>Swaps the (n+1)'th stack item with the (m+1)'th stack item, where n and m are decoded from a
 * 1-byte immediate operand. PC increment is 2. Gas cost is veryLow tier (3). Requires Amsterdam
 * fork or later.
 */
public class ExchangeOperationV2 extends AbstractFixedCostOperationV2 {

  /** The EXCHANGE opcode value. */
  public static final int OPCODE = 0xe8;

  private static final OperationResult EXCHANGE_SUCCESS = new OperationResult(3, null, 2);

  private static final OperationResult INVALID_IMMEDIATE =
      new OperationResult(3, ExceptionalHaltReason.INVALID_OPERATION, 2);

  private static final OperationResult EXCHANGE_UNDERFLOW =
      new OperationResult(3, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS, 2);

  /**
   * Instantiates a new EXCHANGE operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ExchangeOperationV2(final GasCalculator gasCalculator) {
    super(OPCODE, "EXCHANGE", 0, 0, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(
        frame, frame.stackDataV2(), frame.getCode().getBytes().toArrayUnsafe(), frame.getPC());
  }

  /**
   * Performs EXCHANGE operation.
   *
   * @param frame the message frame
   * @param s the stack data array
   * @param code the bytecode array
   * @param pc the current program counter
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final byte[] code, final int pc) {
    final int imm = (pc + 1 >= code.length) ? 0 : code[pc + 1] & 0xFF;

    final int packed = Eip8024Decoder.DECODE_PAIR_PACKED[imm];

    if (packed == Eip8024Decoder.INVALID_PAIR) {
      return INVALID_IMMEDIATE;
    }

    final int n = packed & 0xFF;
    final int m = (packed >>> 8) & 0xFF;

    if (!frame.stackHasItems(Math.max(n, m) + 1)) {
      return EXCHANGE_UNDERFLOW;
    }
    StackArithmetic.exchange(s, frame.stackTopV2(), n, m);
    return EXCHANGE_SUCCESS;
  }
}
