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

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import java.util.Optional;

/** EVM v2 BASEFEE operation — pushes the block base fee onto the stack (London+). */
public class BaseFeeOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult baseFeeSuccess = new OperationResult(2, null);
  private static final OperationResult INVALID_OPERATION_RESPONSE =
      new OperationResult(2, ExceptionalHaltReason.INVALID_OPERATION);

  /**
   * Instantiates a new Base fee operation.
   *
   * @param gasCalculator the gas calculator
   */
  public BaseFeeOperationV2(final GasCalculator gasCalculator) {
    super(0x48, "BASEFEE", 0, 1, gasCalculator, gasCalculator.getBaseTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Performs the BASEFEE operation.
   *
   * @param frame the message frame
   * @param stack the v2 long[] stack
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final long[] stack) {
    if (!frame.stackHasSpace(1)) return OVERFLOW_RESPONSE;
    final Optional<Wei> maybeBaseFee = frame.getBlockValues().getBaseFee();
    if (maybeBaseFee.isEmpty()) {
      return INVALID_OPERATION_RESPONSE;
    }
    frame.setTopV2(StackArithmetic.pushWei(stack, frame.stackTopV2(), maybeBaseFee.get()));
    return baseFeeSuccess;
  }
}
