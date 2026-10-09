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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushBytes32;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushZero;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes32;

/** The Call data load operation. */
public class CallDataLoadOperationV2 extends AbstractFixedCostOperationV2 {

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
      pushZero(stack, top - 1);
      return successResponse;
    }
    final int start = (int) stack[offset + 3];
    final Bytes data = frame.getInputData();
    if (start >= data.size()) {
      pushZero(stack, top - 1);
      return successResponse;
    }
    final MutableBytes32 word = MutableBytes32.create();
    data.slice(start, Math.min(Bytes32.SIZE, data.size() - start)).copyTo(word, 0);
    pushBytes32(word, stack, top - 1);
    return successResponse;
  }
}
