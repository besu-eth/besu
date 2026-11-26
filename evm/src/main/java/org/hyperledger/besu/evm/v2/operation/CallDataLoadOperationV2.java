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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.setZero;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/** The Call data load operation. */
public class CallDataLoadOperationV2 extends AbstractFixedCostOperationV2 {

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  /**
   * Instantiates a new Call data load operation.
   *
   * @param gasCalculator the gas calculator
   */
  public CallDataLoadOperationV2(final GasCalculator gasCalculator) {
    super(0x35, "CALLDATALOAD", 1, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    if (!frame.stackHasItemsV2(1)) return underflowResponse;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = (top - 1) << 2;

    // An offset that does not fit in an int starts after any call data, so the word is zero.
    if ((stack[offset] | stack[offset + 1] | stack[offset + 2]) != 0
        || Long.compareUnsigned(stack[offset + 3], Integer.MAX_VALUE) > 0) {
      setZero(stack, top - 1);
      return successResponse;
    }
    final int start = (int) stack[offset + 3];
    final byte[] data = frame.inputDataArrayV2();
    final int size = frame.inputDataSizeV2();
    if (start >= size) {
      setZero(stack, top - 1);
      return successResponse;
    }
    final int from = frame.inputDataOffsetV2() + start;
    if (size - start >= 32) {
      // the whole word lies in the call data
      stack[offset] = (long) LONG_BE.get(data, from);
      stack[offset + 1] = (long) LONG_BE.get(data, from + 8);
      stack[offset + 2] = (long) LONG_BE.get(data, from + 16);
      stack[offset + 3] = (long) LONG_BE.get(data, from + 24);
    } else {
      loadPartialWord(stack, offset, data, from, size - start);
    }
    return successResponse;
  }

  /**
   * Loads a word that runs past the end of the call data onto the stack; the missing bytes read as
   * zero. Kept out of {@link #executeFixedCostOperation} so that method stays small enough for C2
   * to inline.
   *
   * @param stack the flat limb array
   * @param offset the index of the word's most significant limb
   * @param data the array the call data lies in
   * @param from the index of the word's first byte in {@code data}
   * @param available the call data bytes from {@code from} to the end, from 1 to 31
   */
  private static void loadPartialWord(
      final long[] stack,
      final int offset,
      final byte[] data,
      final int from,
      final int available) {
    // the limbs that lie wholly in the call data
    final int wholeLimbs = available >>> 3;
    int limb = 0;
    for (; limb < wholeLimbs; limb++) {
      stack[offset + limb] = (long) LONG_BE.get(data, from + (limb << 3));
    }
    // the limb the call data ends in, its missing low bytes zero
    final int remaining = available & 7;
    final int base = from + (limb << 3);
    long value = 0L;
    for (int i = 0; i < remaining; i++) {
      value = (value << 8) | (data[base + i] & 0xffL);
    }
    stack[offset + limb] = remaining == 0 ? 0L : value << ((8 - remaining) << 3);
    // the limbs past the end of the call data
    for (limb++; limb < 4; limb++) {
      stack[offset + limb] = 0L;
    }
  }
}
