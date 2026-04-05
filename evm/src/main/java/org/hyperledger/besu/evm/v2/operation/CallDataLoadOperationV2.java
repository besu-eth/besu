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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import org.apache.tuweni.bytes.Bytes;

/**
 * EVM v2 CALLDATALOAD operation (0x35).
 *
 * <p>Pops an offset from the stack and pushes 32 bytes of input data starting at that offset,
 * right-padding with zeros if the offset is beyond the end of the input data.
 */
public class CallDataLoadOperationV2 extends AbstractFixedCostOperationV2 {

  private static final Operation.OperationResult CALLDATALOAD_SUCCESS =
      new Operation.OperationResult(3, null);

  /**
   * Instantiates a new CallDataLoad operation.
   *
   * @param gasCalculator the gas calculator
   */
  public CallDataLoadOperationV2(final GasCalculator gasCalculator) {
    super(0x35, "CALLDATALOAD", 1, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Execute CALLDATALOAD on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack data
   * @return the operation result
   */
  public static Operation.OperationResult staticOperation(
      final MessageFrame frame, final long[] s) {
    if (!frame.stackHasItems(1)) return UNDERFLOW_RESPONSE;
    final int top = frame.stackTopV2();
    final int off = (top - 1) << 2;

    // If the offset doesn't fit in a non-negative int, result is zero
    if (s[off] != 0 || s[off + 1] != 0 || s[off + 2] != 0 || (s[off + 3] >>> 31) != 0) {
      s[off] = 0;
      s[off + 1] = 0;
      s[off + 2] = 0;
      s[off + 3] = 0;
      return CALLDATALOAD_SUCCESS;
    }

    final int offset = (int) s[off + 3];
    final Bytes data = frame.getInputData();
    if (offset < data.size()) {
      final byte[] result = new byte[32];
      final int toCopy = Math.min(32, data.size() - offset);
      System.arraycopy(data.slice(offset, toCopy).toArrayUnsafe(), 0, result, 0, toCopy);
      StackArithmetic.fromBytesAt(s, top, 0, result, 0, 32);
    } else {
      s[off] = 0;
      s[off + 1] = 0;
      s[off + 2] = 0;
      s[off + 3] = 0;
    }

    return CALLDATALOAD_SUCCESS;
  }
}
