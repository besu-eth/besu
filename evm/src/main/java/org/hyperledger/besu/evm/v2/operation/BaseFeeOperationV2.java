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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushWei;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;

import java.util.Optional;

/** The Base fee operation. */
public class BaseFeeOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult successResponse = new OperationResult(2, null);

  /**
   * Instantiates a new Base fee operation.
   *
   * @param gasCalculator the gas calculator
   */
  public BaseFeeOperationV2(final GasCalculator gasCalculator) {
    super(0x48, "BASEFEE", 0, 1, gasCalculator, gasCalculator.getBaseTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Performs BASEFEE operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static Operation.OperationResult staticOperation(final MessageFrame frame) {
    final Optional<Wei> maybeBaseFee = frame.getBlockValues().getBaseFee();
    if (maybeBaseFee.isEmpty()) {
      return new Operation.OperationResult(2, ExceptionalHaltReason.INVALID_OPERATION);
    }
    if (!frame.stackHasSpaceV2(1)) return OVERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    pushWei(maybeBaseFee.get(), stack, top);
    frame.setTopV2(top + 1);
    return successResponse;
  }
}
