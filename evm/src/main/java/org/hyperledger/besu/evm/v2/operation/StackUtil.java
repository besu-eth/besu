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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import org.apache.tuweni.bytes.Bytes;

/**
 * Static utility for reading/writing typed values on the flat {@code long[]} V2 operand stack. Each
 * 256-bit word occupies 4 consecutive longs in big-endian limb order: {@code [u3, u2, u1, u0]}
 * where u3 is the most-significant limb. This class has a default modifier (package-private)
 * because it shouldn't be used outside the EVM operations
 */
final class StackUtil {

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT_BE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

  private StackUtil() {}

  /**
   * Writes a zero-valued 256-bit word at the given stack slot.
   *
   * @param stack the flat limb array (4 longs per 256-bit word)
   * @param top the slot index to write to
   */
  static void pushZero(final long[] stack, final int top) {
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
  static void pushWei(final Wei wei, final long[] stack, final int top) {
    // TODO EVMv2 store this representation at Wei object construction time when switching from v2
    // to v1
    int offset = top << 2;
    final byte[] b = wei.toArrayUnsafe();
    stack[offset] = (long) LONG_BE.get(b, 0);
    stack[offset + 1] = (long) LONG_BE.get(b, 8);
    stack[offset + 2] = (long) LONG_BE.get(b, 16);
    stack[offset + 3] = (long) LONG_BE.get(b, 24);
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
  static Address readAddressAt(final long[] stack, final int top, final int depth) {
    final int off = (top - 1 - depth) << 2;
    byte[] bytes = new byte[20];
    INT_BE.set(bytes, 0, (int) stack[off + 1]);
    LONG_BE.set(bytes, 4, stack[off + 2]);
    LONG_BE.set(bytes, 12, stack[off + 3]);
    return Address.wrap(Bytes.wrap(bytes));
  }
}
