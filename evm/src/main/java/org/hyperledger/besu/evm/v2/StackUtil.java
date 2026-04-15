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
package org.hyperledger.besu.evm.v2;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.operation.Operation;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Static utility for reading/writing typed values on the flat {@code long[]} V2 operand stack. Each
 * 256-bit word occupies 4 consecutive longs in big-endian limb order: {@code [u3, u2, u1, u0]}
 * where u3 is the most-significant limb.
 */
public final class StackUtil {

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT_BE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

  /** Shared underflow response (zero gas cost). */
  public static final Operation.OperationResult UNDERFLOW_RESPONSE =
      new Operation.OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);

  /** Shared overflow response (zero gas cost). */
  public static final Operation.OperationResult OVERFLOW_RESPONSE =
      new Operation.OperationResult(0L, ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);

  private StackUtil() {}

  /**
   * Writes a zero-valued 256-bit word at the given stack slot.
   *
   * @param stack the flat limb array (4 longs per 256-bit word)
   * @param top the slot index to write to
   */
  public static void pushZero(final long[] stack, final int top) {
    final int offset = top << 2;
    stack[offset] = 0;
    stack[offset + 1] = 0;
    stack[offset + 2] = 0;
    stack[offset + 3] = 0;
  }

  /**
   * Writes a {@link Wei} value as four big-endian limbs at the given stack slot.
   *
   * @param wei the Wei value to write
   * @param stack the flat limb array
   * @param top the slot index to write to
   */
  public static void pushWei(final Wei wei, final long[] stack, final int top) {
    wei.writeLimbs(stack, top << 2);
  }

  /**
   * Extracts a 160-bit {@link Address} from a 256-bit stack word at the given depth below the top
   * of stack.
   *
   * @param stack the flat limb array
   * @param top current stack-top (item count)
   * @param depth 0 for the topmost item, 1 for the item below, etc.
   * @return the address formed from the lower 160 bits of the stack word
   */
  public static Address toAddressAt(final long[] stack, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    byte[] bytes = new byte[20];
    INT_BE.set(bytes, 0, (int) stack[off + 1]);
    LONG_BE.set(bytes, 4, stack[off + 2]);
    LONG_BE.set(bytes, 12, stack[off + 3]);
    return Address.wrap(org.apache.tuweni.bytes.Bytes.wrap(bytes));
  }
}
