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

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/**
 * EVM v2 CLZ (EIP-7939) operation using long[] stack representation.
 *
 * <p>Each 256-bit word is stored as four longs: index 0 = most significant 64 bits, index 3 = least
 * significant 64 bits.
 */
public class CountLeadingZerosOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult CLZ_SUCCESS = new OperationResult(5, null);

  /**
   * Instantiates a new Count Leading Zeros operation.
   *
   * @param gasCalculator the gas calculator
   */
  public CountLeadingZerosOperationV2(final GasCalculator gasCalculator) {
    super(0x1e, "CLZ", 1, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Execute the CLZ opcode on the v2 long[] stack.
   *
   * <p>CLZ: stack[top-1] = number of leading zero bits of stack[top-1] (256 for zero), top
   * unchanged.
   *
   * @param frame the message frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    long[] stack = frame.stackDataV2();
    final int aOffset = (frame.stackTopV2() - 1) << 2;

    final UInt256 valueA =
        new UInt256(stack[aOffset], stack[aOffset + 1], stack[aOffset + 2], stack[aOffset + 3]);

    final UInt256 r = UInt256.fromInt(valueA.numberOfLeadingZeros());

    stack[aOffset] = r.u3();
    stack[aOffset + 1] = r.u2();
    stack[aOffset + 2] = r.u1();
    stack[aOffset + 3] = r.u0();

    return CLZ_SUCCESS;
  }
}
