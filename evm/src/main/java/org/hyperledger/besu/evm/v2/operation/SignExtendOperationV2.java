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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/**
 * EVM v2 SIGNEXTEND operation using long[] stack representation.
 *
 * <p>Each 256-bit word is stored as four longs: index 0 = most significant 64 bits, index 3 = least
 * significant 64 bits.
 */
public class SignExtendOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult SIGNEXTEND_SUCCESS = new OperationResult(5, null);

  /**
   * Instantiates a new SignExtend operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SignExtendOperationV2(final GasCalculator gasCalculator) {
    super(0x0B, "SIGNEXTEND", 2, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Execute the SIGNEXTEND opcode on the v2 long[] stack.
   *
   * <p>SIGNEXTEND: extends the sign of the byte at index stack[top-1], counted from the least
   * significant byte, of stack[top-2] into the more significant bytes, return top-1.
   *
   * @param frame the message frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItemsV2(2)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int indexOffset = (top - 1) << 2;
    final int valueOffset = (top - 2) << 2;
    frame.setTopV2(top - 1);

    // From byte 31 on the sign bit is the top bit of the word, so the value stays as it is.
    if ((stack[indexOffset] | stack[indexOffset + 1] | stack[indexOffset + 2]) != 0
        || Long.compareUnsigned(stack[indexOffset + 3], 31) >= 0) {
      return SIGNEXTEND_SUCCESS;
    }

    final int signBit = ((int) stack[indexOffset + 3] << 3) + 7;
    // 0 for the least significant limb u0, 3 for the most significant limb u3
    final int signLimbIndex = signBit >>> 6;
    final int signLimb = valueOffset + 3 - signLimbIndex;
    final int bitInLimb = signBit & 63;
    final long fill = ((stack[signLimb] >>> bitInLimb) & 1L) == 0 ? 0L : -1L;
    if (bitInLimb < 63) {
      final long kept = -1L >>> (63 - bitInLimb);
      stack[signLimb] = (stack[signLimb] & kept) | (fill & ~kept);
    }
    if (signLimbIndex < 3) {
      stack[valueOffset] = fill;
    }
    if (signLimbIndex < 2) {
      stack[valueOffset + 1] = fill;
    }
    if (signLimbIndex < 1) {
      stack[valueOffset + 2] = fill;
    }
    return SIGNEXTEND_SUCCESS;
  }
}
