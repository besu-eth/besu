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
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;

/** The Mul mod operation. */
public class MulModOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult mulModSuccess = new OperationResult(8, null);

  /**
   * Instantiates a new Mul mod operation.
   *
   * @param gasCalculator the gas calculator
   */
  public MulModOperationV2(final GasCalculator gasCalculator) {
    super(0x09, "MULMOD", 3, 1, gasCalculator, gasCalculator.getMidTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Performs MulMod operation.
   *
   * @param frame the frame
   * @param stack the v2 operand stack ({@code long[]} in big-endian limb order)
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final long[] stack) {
    if (!frame.stackHasItems(3)) return UNDERFLOW_RESPONSE;
    int top = frame.stackTopV2();
    mulMod(stack, top);
    // consumed three items and produced one item
    frame.setTopV2(top - 2);
    return mulModSuccess;
  }

  /**
   * Performs EVM MULMOD (modular multiplication) on the three top stack items.
   *
   * <p>MULMOD: mulmod(a,b,m) = (a * b) mod m
   *
   * <p>MULMOD: stack[top-3] = (stack[top-1] * stack[top-2]) mod stack[top-3].
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   */
  private static void mulMod(final long[] stack, final int top) {
    final int aOffset = (top - 1) << 2;
    final int bOffset = (top - 2) << 2;
    final int mOffset = (top - 3) << 2;
    final UInt256 valueA =
        new UInt256(stack[aOffset], stack[aOffset + 1], stack[aOffset + 2], stack[aOffset + 3]);
    final UInt256 valueB =
        new UInt256(stack[bOffset], stack[bOffset + 1], stack[bOffset + 2], stack[bOffset + 3]);
    final UInt256 modulus =
        new UInt256(stack[mOffset], stack[mOffset + 1], stack[mOffset + 2], stack[mOffset + 3]);
    final UInt256 r = modulus.isZero() ? UInt256.ZERO : valueA.mulMod(valueB, modulus);
    stack[mOffset] = r.u3();
    stack[mOffset + 1] = r.u2();
    stack[mOffset + 2] = r.u1();
    stack[mOffset + 3] = r.u0();
  }
}
