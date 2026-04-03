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

/**
 * EVM v2 POP operation (0x50).
 *
 * <p>Discards the top stack item by decrementing the stack pointer. Gas cost is base tier (2).
 */
public class PopOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult POP_SUCCESS = new OperationResult(2, null);

  /**
   * Instantiates a new Pop operation.
   *
   * @param gasCalculator the gas calculator
   */
  public PopOperationV2(final GasCalculator gasCalculator) {
    super(0x50, "POP", 1, 0, gasCalculator, gasCalculator.getBaseTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame);
  }

  /**
   * Performs POP operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItems(1)) return UNDERFLOW_RESPONSE;
    frame.setTopV2(frame.stackTopV2() - 1);
    return POP_SUCCESS;
  }
}
