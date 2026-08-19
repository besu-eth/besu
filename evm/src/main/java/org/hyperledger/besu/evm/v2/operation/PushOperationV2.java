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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/** The Push operation. */
public class PushOperationV2 extends AbstractFixedCostOperationV2 {
  /** The constant PUSH_BASE. */
  public static final int PUSH_BASE = 0x5F;

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  private final int length;

  /** The Push operation success result. */
  static final OperationResult pushSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new Push operation.
   *
   * @param length the length
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
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    final byte[] code = frame.getCode().getBytes().toArrayUnsafe();
    return staticOperation(frame, code, frame.getPC(), length);
  }

  /**
   * Performs Push operation. {@code pushSize} must be in 1..32.
   *
   * @param frame the frame
   * @param code the code
   * @param pc the pc
   * @param pushSize the push size
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final byte[] code, final int pc, final int pushSize) {
    final int start = pc + 1;
    final int end = start + pushSize;
    long u0 = 0, u1 = 0, u2 = 0, u3 = 0;
    // Hot path: the full push data is in bounds and there are at least 8 bytes of code before
    // it, so every limb can be read as one unaligned big-endian long whose window may overlap
    // preceding code bytes; the top limb is masked down to its actual width. The unconditional
    // mask is a no-op (-1L >>> 0) when the top limb is full.
    if (end <= code.length && start >= 8) {
      switch ((pushSize - 1) >>> 3) {
        case 0 -> u0 = ((long) LONG_BE.get(code, end - 8)) & (-1L >>> ((8 - pushSize) << 3));
        case 1 -> {
          u1 = ((long) LONG_BE.get(code, end - 16)) & (-1L >>> ((16 - pushSize) << 3));
          u0 = (long) LONG_BE.get(code, end - 8);
        }
        case 2 -> {
          u2 = ((long) LONG_BE.get(code, end - 24)) & (-1L >>> ((24 - pushSize) << 3));
          u1 = (long) LONG_BE.get(code, end - 16);
          u0 = (long) LONG_BE.get(code, end - 8);
        }
        default -> {
          u3 = ((long) LONG_BE.get(code, end - 32)) & (-1L >>> ((32 - pushSize) << 3));
          u2 = (long) LONG_BE.get(code, end - 24);
          u1 = (long) LONG_BE.get(code, end - 16);
          u0 = (long) LONG_BE.get(code, end - 8);
        }
      }
    } else {
      return pushSlow(frame, code, pc, pushSize);
    }

    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = top << 2;
    try {
      // A full stack makes the first store land past the end of the backing array (its length is
      // maxStackSize * 4), so overflow costs nothing on the non-overflowing path.
      stack[offset] = u3;
    } catch (final ArrayIndexOutOfBoundsException e) {
      return OVERFLOW_RESPONSE;
    }
    stack[offset + 1] = u2;
    stack[offset + 2] = u1;
    stack[offset + 3] = u0;
    frame.setTopV2(top + 1);

    frame.setPC(pc + pushSize);
    return pushSuccess;
  }

  // Rare path, kept out of staticOperation so the hot path stays inlineable: push data truncated
  // by end of code (zero-padded on the right) or too close to the start of code for windowed
  // reads.
  private static OperationResult pushSlow(
      final MessageFrame frame, final byte[] code, final int pc, final int pushSize) {
    final int start = pc + 1;
    final int remainingSize = Math.max(0, code.length - start);
    final int copyLength = Math.min(remainingSize, pushSize);
    final UInt256 pushValue =
        UInt256.fromBytesBE(code, start, start + copyLength)
            .shl0(Math.max(0, (pushSize - remainingSize) * 8));

    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = top << 2;
    try {
      stack[offset] = pushValue.u3();
    } catch (final ArrayIndexOutOfBoundsException e) {
      return OVERFLOW_RESPONSE;
    }
    stack[offset + 1] = pushValue.u2();
    stack[offset + 2] = pushValue.u1();
    stack[offset + 3] = pushValue.u0();
    frame.setTopV2(top + 1);

    frame.setPC(pc + pushSize);
    return pushSuccess;
  }
}
