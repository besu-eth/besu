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
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import org.apache.tuweni.bytes.Bytes32;

/**
 * EVM v2 TLOAD operation (EIP-1153) using long[] stack representation.
 *
 * <p>Pops a slot key from the stack, loads the value from transient storage, and pushes the result
 * back. Fixed gas cost equal to veryLow tier.
 */
public class TLoadOperationV2 extends AbstractFixedCostOperationV2 {

  private static final long GAS_COST = 3L;
  private static final OperationResult TLOAD_SUCCESS = new OperationResult(GAS_COST, null);
  private static final OperationResult TLOAD_OUT_OF_GAS =
      new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_GAS);

  /**
   * Instantiates a new TLoad operation.
   *
   * @param gasCalculator the gas calculator
   */
  public TLoadOperationV2(final GasCalculator gasCalculator) {
    super(0x5C, "TLOAD", 1, 1, gasCalculator, gasCalculator.getTransientLoadOperationGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Execute the TLOAD opcode on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack as a long[] array
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final long[] s) {
    if (!frame.stackHasItems(1)) {
      return UNDERFLOW_RESPONSE;
    }

    if (frame.getRemainingGas() < GAS_COST) {
      return TLOAD_OUT_OF_GAS;
    }

    final int top = frame.stackTopV2();

    // Extract the 32-byte slot key from the stack
    final byte[] keyBytes = new byte[32];
    StackArithmetic.toBytesAt(s, top, 0, keyBytes);
    final Bytes32 slot = Bytes32.wrap(keyBytes);

    // Load from transient storage and overwrite top slot in place
    final byte[] result =
        frame.getTransientStorageValue(frame.getRecipientAddress(), slot).toArrayUnsafe();
    StackArithmetic.fromBytesAt(s, top, 0, result, 0, result.length);

    return TLOAD_SUCCESS;
  }
}
