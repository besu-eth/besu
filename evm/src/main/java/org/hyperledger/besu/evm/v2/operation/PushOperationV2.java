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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.fromBytesBE;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.getLongBE;

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.OverflowException;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

/**
 * The PUSH0-32 operations. The interpreter runs them inline, so they exist only as static methods
 * and the fork's operation table has no instance of them.
 */
public final class PushOperationV2 {
  /** The constant PUSH_BASE. */
  public static final int PUSH_BASE = 0x5F;

  /** The Push operation success result. */
  private static final OperationResult pushSuccess = new OperationResult(3, null);

  /** The PUSH0 operation success result, at the base tier cost. */
  private static final OperationResult push0Success = new OperationResult(2, null);

  private PushOperationV2() {}

  private static void pushUInt256ToStack(final MessageFrame frame, final UInt256 pushValue) {
    if (!frame.stackHasSpaceV2(1)) {
      throw new OverflowException();
    }
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = top << 2;
    stack[offset] = pushValue.u3();
    stack[offset + 1] = pushValue.u2();
    stack[offset + 2] = pushValue.u1();
    stack[offset + 3] = pushValue.u0();
    frame.setTopV2(top + 1);
  }

  private static void pushLongToStack(final MessageFrame frame, final long u0) {
    if (!frame.stackHasSpaceV2(1)) {
      throw new OverflowException();
    }
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = top << 2;
    stack[offset] = 0;
    stack[offset + 1] = 0;
    stack[offset + 2] = 0;
    stack[offset + 3] = u0;
    frame.setTopV2(top + 1);
  }

  /** Optimized version of PUSH opcode for PUSH0 and PUSH1 only. */
  public static final class SingleByte {

    private SingleByte() {}

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
      return pushSize == 0 ? push0Success : pushSuccess;
    }
  }

  /** Optimized version for PUSH opcode for PUSH2 to PUSH8. */
  public static final class SingleLimb {

    private SingleLimb() {}

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
      long u0 = getLongBE(code, start, Math.min(end, code.length));

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
  public static final class MultiLimb {

    private MultiLimb() {}

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
      UInt256 pushValue = fromBytesBE(code, start, remainingSize);

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
