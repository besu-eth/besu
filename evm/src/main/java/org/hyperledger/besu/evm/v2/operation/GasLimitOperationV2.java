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

/** EVM v2 GASLIMIT operation — pushes the block gas limit onto the stack. */
public class GasLimitOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult gasLimitSuccess = new OperationResult(2, null);

  /**
   * Instantiates a new Gas limit operation.
   *
   * @param gasCalculator the gas calculator
   */
  public GasLimitOperationV2(final GasCalculator gasCalculator) {
    super(0x45, "GASLIMIT", 0, 1, gasCalculator, gasCalculator.getBaseTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Performs the GASLIMIT operation.
   *
   * @param frame the message frame
   * @param stack the v2 long[] stack
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final long[] stack) {
    if (!frame.stackHasSpace(1)) return OVERFLOW_RESPONSE;
    frame.setTopV2(
        StackArithmetic.pushLong(stack, frame.stackTopV2(), frame.getBlockValues().getGasLimit()));
    return gasLimitSuccess;
  }
}
