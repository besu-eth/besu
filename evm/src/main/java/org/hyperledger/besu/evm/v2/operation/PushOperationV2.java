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
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.StackArithmetic;

/**
 * EVM v2 PUSH1-32 operation (opcodes 0x60–0x7F).
 *
 * <p>Reads {@code length} immediate bytes from bytecode at {@code pc+1} and pushes the resulting
 * value onto the stack. Gas cost is veryLow tier (3). PC increment is {@code 1 + length}.
 */
public class PushOperationV2 extends AbstractFixedCostOperationV2 {

  /** The PUSH opcode base (PUSH0 = 0x5F, so PUSH1 = 0x60). */
  public static final int PUSH_BASE = 0x5F;

  private static final OperationResult PUSH_SUCCESS = new OperationResult(3, null);

  private final int length;

  /**
   * Instantiates a new Push operation for PUSH{length}.
   *
   * @param length the number of bytes to push (1–32)
   * @param gasCalculator the gas calculator
   */
  public PushOperationV2(final int length, final GasCalculator gasCalculator) {
    super(
        PUSH_BASE + length,
        "PUSH" + length,
        0,
        1,
        gasCalculator,
        gasCalculator.getVeryLowTierGasCost());
    this.length = length;
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    final byte[] code = frame.getCode().getBytes().toArrayUnsafe();
    return staticOperation(frame, frame.stackDataV2(), code, frame.getPC(), length);
  }

  /**
   * Performs PUSH operation.
   *
   * @param frame the frame
   * @param s the stack data array
   * @param code the bytecode array
   * @param pc the current program counter
   * @param pushSize the number of bytes to push
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame,
      final long[] s,
      final byte[] code,
      final int pc,
      final int pushSize) {
    if (!frame.stackHasSpace(1)) return OVERFLOW_RESPONSE;
    frame.setTopV2(StackArithmetic.pushFromBytes(s, frame.stackTopV2(), code, pc + 1, pushSize));
    frame.setPC(pc + pushSize);
    return PUSH_SUCCESS;
  }
}
