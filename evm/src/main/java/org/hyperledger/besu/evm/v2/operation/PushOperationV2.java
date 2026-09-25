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
import org.hyperledger.besu.evm.internal.OverflowException;

/** The Push operation. */
public abstract class PushOperationV2 extends AbstractFixedCostOperationV2 {
  /** The constant PUSH_BASE. */
  public static final int PUSH_BASE = 0x5F;

  private final int length;

  /** The Push operation success result. */
  private static final OperationResult pushSuccess = new OperationResult(3, null);

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

  private static void pushUInt256ToStack(final MessageFrame frame, final UInt256 pushValue) {
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = top << 2;
    try {
      stack[offset] = pushValue.u3();
      stack[offset + 1] = pushValue.u2();
      stack[offset + 2] = pushValue.u1();
      stack[offset + 3] = pushValue.u0();
    } catch (ArrayIndexOutOfBoundsException aiobe) {
      throw new OverflowException();
    }
    frame.setTopV2(top + 1);
  }

  private static void pushLongToStack(final MessageFrame frame, final long u0) {
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = top << 2;
    try {
      stack[offset] = 0;
      stack[offset + 1] = 0;
      stack[offset + 2] = 0;
      stack[offset + 3] = u0;
    } catch (ArrayIndexOutOfBoundsException aiobe) {
      throw new OverflowException();
    }
    frame.setTopV2(top + 1);
  }

  /** Optimized version of PUSH opcode for PUSH0 and PUSH1 only. */
  public class SingleByte extends PushOperationV2 {

    /**
     * Instantiates a new Push operation.
     *
     * @param length the length
     * @param gasCalculator the gas calculator
     */
    public SingleByte(final int length, final GasCalculator gasCalculator) {
      super(length, gasCalculator);
    }

    @Override
    public OperationResult executeFixedCostOperation(final MessageFrame frame) {
      final byte[] code = frame.getCode().getBytes().toArrayUnsafe();
      return staticOperation(frame, code, frame.getPC(), length);
    }

    /**
     * Performs Push operation statically.
     *
     * @param frame the frame
     * @param code the code
     * @param pc the pc
     * @param pushSize the push size
     * @return the operation result
     */
    public static OperationResult staticOperation(
        final MessageFrame frame, final byte[] code, final int pc, final int pushSize) {
      long u0 = 0;
      final int start = pc + 1;
      if (pushSize != 0 && start < code.length) {
        u0 = code[start] & 0xFFL;
      }

      pushLongToStack(frame, u0);
      frame.setPC(pc + pushSize);
      return pushSuccess;
    }
  }

  /** Optimized version for PUSH opcode for PUSH2 to PUSH8. */
  public class SingleLimb extends PushOperationV2 {

    /**
     * Instantiates a new Push operation.
     *
     * @param length the length
     * @param gasCalculator the gas calculator
     */
    public SingleLimb(final int length, final GasCalculator gasCalculator) {
      super(length, gasCalculator);
    }

    @Override
    public OperationResult executeFixedCostOperation(final MessageFrame frame) {
      final byte[] code = frame.getCode().getBytes().toArrayUnsafe();
      return staticOperation(frame, code, frame.getPC(), length);
    }

    /**
     * Performs Push operation statically.
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
      long u0 = UInt256.getLongBE(code, start, Math.min(end, code.length));

      // Slow-path - when push is truncated and zeros need to be appended
      if (end > code.length) {
        final int remainingSize = code.length - start;
        final int shift = (pushSize - remainingSize) * 8;
        u0 <<= shift;
      }

      pushLongToStack(frame, u0);
      frame.setPC(pc + pushSize);
      return pushSuccess;
    }
  }

  /**
   * Generic multi limb version of PUSH opcode, can execute PUSH0-32, but it should only be used for
   * multiple long limbs because of performance.
   */
  public class MultiLimb extends PushOperationV2 {

    /**
     * Instantiates a new Push operation.
     *
     * @param length the length
     * @param gasCalculator the gas calculator
     */
    public MultiLimb(final int length, final GasCalculator gasCalculator) {
      super(length, gasCalculator);
    }

    @Override
    public OperationResult executeFixedCostOperation(final MessageFrame frame) {
      final byte[] code = frame.getCode().getBytes().toArrayUnsafe();
      return staticOperation(frame, code, frame.getPC(), length);
    }

    /**
     * Performs Push operation statically.
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
      final int remainingSize = Math.min(end, code.length) - start;
      UInt256 pushValue = UInt256.fromBytesBE(code, start, remainingSize);

      // Slow-path - when push is truncated and zeros need to be appended
      if (end > code.length) {
        pushValue = pushValue.shiftLeft((pushSize - remainingSize) * 8);
      }

      pushUInt256ToStack(frame, pushValue);
      frame.setPC(pc + pushSize);
      return pushSuccess;
    }
  }
}
