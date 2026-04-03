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

/**
 * EVM v2 PUSH0 operation (0x5F).
 *
 * <p>Pushes the constant value 0 onto the stack. Requires Shanghai fork or later. Gas cost is base
 * tier (2).
 */
public class Push0OperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult PUSH0_SUCCESS = new OperationResult(2, null);

  /**
   * Instantiates a new Push0 operation.
   *
   * @param gasCalculator the gas calculator
   */
  public Push0OperationV2(final GasCalculator gasCalculator) {
    super(0x5F, "PUSH0", 0, 1, gasCalculator, gasCalculator.getBaseTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Performs PUSH0 operation.
   *
   * @param frame the frame
   * @param s the stack data array
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final long[] s) {
    if (!frame.stackHasSpace(1)) return OVERFLOW_RESPONSE;
    frame.setTopV2(StackArithmetic.pushZero(s, frame.stackTopV2()));
    return PUSH0_SUCCESS;
  }
}
