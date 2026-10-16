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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.swapWords;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Eip8024Decoder;

/**
 * EVM v2 EXCHANGE operation (EIP-8024) using long[] stack representation.
 *
 * <p>Exchanges the (n+1)'th and the (m+1)'th stack items, where n and m are decoded from the
 * immediate byte.
 */
public class ExchangeOperationV2 extends AbstractFixedCostOperationV2 {

  /** The EXCHANGE opcode number. */
  public static final int OPCODE = 0xe8;

  private static final OperationResult EXCHANGE_SUCCESS = new OperationResult(3, null, 2);
  private static final OperationResult INVALID_IMMEDIATE =
      new OperationResult(3, ExceptionalHaltReason.INVALID_OPERATION, 2);
  private static final OperationResult EXCHANGE_UNDERFLOW =
      new OperationResult(3, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS, 2);

  /**
   * Instantiates a new Exchange operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ExchangeOperationV2(final GasCalculator gasCalculator) {
    super(OPCODE, "EXCHANGE", 0, 0, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame, frame.getCode().getBytes().toArrayUnsafe(), frame.getPC());
  }

  /**
   * Execute the EXCHANGE opcode on the v2 long[] stack.
   *
   * <p>EXCHANGE: exchange stack[top-1-n] and stack[top-1-m], top unchanged.
   *
   * @param frame the message frame
   * @param code the code being executed
   * @param pc the program counter of the EXCHANGE
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final byte[] code, final int pc) {
    // The immediate is zero past the end of the code.
    final int imm = (pc + 1 >= code.length) ? 0 : code[pc + 1] & 0xFF;
    final int packed = Eip8024Decoder.DECODE_PAIR_PACKED[imm];
    if (packed == Eip8024Decoder.INVALID_PAIR) {
      return INVALID_IMMEDIATE;
    }
    final int n = Eip8024Decoder.unpackN(packed);
    final int m = Eip8024Decoder.unpackM(packed);
    if (!frame.stackHasItemsV2(Math.max(n, m) + 1)) return EXCHANGE_UNDERFLOW;
    final int top = frame.stackTopV2();
    swapWords(frame.stackDataV2(), top - 1 - n, top - 1 - m);
    return EXCHANGE_SUCCESS;
  }
}
