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

/** The Clz (Count Leading Zeros) operation. Introduced in Osaka. */
public class ClzOperationV2 extends AbstractFixedCostOperationV2 {

  /** The Clz operation success result. */
  static final OperationResult clzSuccess = new OperationResult(5, null);

  /**
   * Instantiates a new Clz operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ClzOperationV2(final GasCalculator gasCalculator) {
    super(0x1e, "CLZ", 1, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Performs clz operation.
   *
   * @param frame the frame
   * @param stack the v2 operand stack ({@code long[]} in big-endian limb order)
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final long[] stack) {
    if (!frame.stackHasItems(1)) return UNDERFLOW_RESPONSE;
    frame.setTopV2(StackArithmetic.clz(stack, frame.stackTopV2()));
    return clzSuccess;
  }
}
