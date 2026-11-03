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
    final int size = data.length;
    if (start >= size) {
      setZero(stack, top - 1);
    } else if (size - start >= 32) {
      // the whole word lies in the call data
      stack[offset] = (long) LONG_BE.get(data, start);
      stack[offset + 1] = (long) LONG_BE.get(data, start + 8);
      stack[offset + 2] = (long) LONG_BE.get(data, start + 16);
      stack[offset + 3] = (long) LONG_BE.get(data, start + 24);
    } else {
      // the word runs past the end of the call data; the missing bytes read as zero
      for (int limb = 0; limb < 4; limb++) {
        long value = 0L;
        for (int i = 0; i < 8; i++) {
          final int index = start + (limb << 3) + i;
          value = (value << 8) | (index < size ? (data[index] & 0xffL) : 0L);
        }
        stack[offset + limb] = value;
      }
    }
    return successResponse;
  }
}
