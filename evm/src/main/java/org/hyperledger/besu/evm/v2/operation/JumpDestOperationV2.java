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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** EVM v2 JUMPDEST operation. */
public class JumpDestOperationV2 extends AbstractFixedCostOperationV2 {

  /** The result of a JUMPDEST, which only marks a jump destination. */
  public static final OperationResult JUMPDEST_SUCCESS = new OperationResult(1L, null);

  /**
   * Instantiates a new Jump dest operation.
   *
   * @param gasCalculator the gas calculator
   */
  public JumpDestOperationV2(final GasCalculator gasCalculator) {
    super(0x5B, "JUMPDEST", 0, 0, gasCalculator, gasCalculator.getJumpDestOperationGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return successResponse;
  }
}
