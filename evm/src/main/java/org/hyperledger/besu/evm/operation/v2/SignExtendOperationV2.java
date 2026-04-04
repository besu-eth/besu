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
package org.hyperledger.besu.evm.operation.v2;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;

/**
 * EVM v2 SIGNEXTEND operation using long[] stack representation.
 *
 * <p>Sign extends a value from a smaller bit width to 256 bits. SIGNEXTEND(b, x) extends the sign
 * bit at position b*8+7 across all higher bits. If b >= 31, the value is unchanged.
 */
public class SignExtendOperationV2 extends AbstractFixedCostOperationV2 {

  private static final Operation.OperationResult SIGNEXTEND_SUCCESS =
      new Operation.OperationResult(5, null);

  /**
   * Instantiates a new SignExtend operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SignExtendOperationV2(final GasCalculator gasCalculator) {
    super(0x0B, "SIGNEXTEND", 2, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Performs SIGNEXTEND on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack data as a long[] array
   * @return the operation result
   */
  public static Operation.OperationResult staticOperation(
      final MessageFrame frame, final long[] s) {
    final int top = frame.stackTopV2();
    final int a = (top - 1) * 4;
    final int b = (top - 2) * 4;

    // If the byte index value has any high bits set or is >= 31, no sign extension needed.
    // Note: s[a+3] < 0 means the unsigned value exceeds Long.MAX_VALUE, so definitely >= 31.
    if (s[a] != 0L || s[a + 1] != 0L || s[a + 2] != 0L || s[a + 3] < 0L || s[a + 3] >= 31L) {
      // Value is unchanged, just pop the index
    } else {
      final int byteIndex = (int) s[a + 3];
      // The sign bit is at position byteIndex*8+7 within the 256-bit value
      // The 256-bit value is stored as: s[b]=MSB(u3), s[b+1](u2), s[b+2](u1), s[b+3]=LSB(u0)
      // Byte 0 is the least significant byte (in s[b+3] lowest byte)
      // Byte 31 is the most significant byte (in s[b] highest byte)

      // Determine which long and bit position the sign bit is in
      final int longIndex = byteIndex / 8; // 0=u0(s[b+3]), 1=u1(s[b+2]), 2=u2(s[b+1]), 3=u3(s[b])
      final int bitInLong = (byteIndex % 8) * 8 + 7;

      final long targetLong = s[b + 3 - longIndex];
      final boolean signBit = ((targetLong >>> bitInLong) & 1L) != 0;

      if (signBit) {
        // Set all bits above the sign bit to 1
        // Fill the current long above the sign bit
        final long mask = -1L << (bitInLong + 1);
        s[b + 3 - longIndex] = targetLong | mask;
        // Fill all higher longs with 0xFF..FF
        for (int i = longIndex + 1; i < 4; i++) {
          s[b + 3 - i] = -1L;
        }
      } else {
        // Set all bits above the sign bit to 0
        final long mask = (1L << (bitInLong + 1)) - 1;
        s[b + 3 - longIndex] = targetLong & mask;
        // Clear all higher longs
        for (int i = longIndex + 1; i < 4; i++) {
          s[b + 3 - i] = 0L;
        }
      }
    }

    frame.setTopV2(top - 1);
    return SIGNEXTEND_SUCCESS;
  }
}
