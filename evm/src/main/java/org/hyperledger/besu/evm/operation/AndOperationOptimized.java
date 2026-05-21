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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;

/** The And operation. */
public class AndOperationOptimized extends AbstractFixedCostOperation {

  /** The And operation success result. */
  static final OperationResult addSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new And operation.
   *
   * @param gasCalculator the gas calculator
   */
  public AndOperationOptimized(final GasCalculator gasCalculator) {
    super(0x16, "AND", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Static operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {

    final Bytes value0 = frame.popStackItem();
    final Bytes value1 = frame.popStackItem();

    UInt256 b0 = UInt256.fromBytesBE(value0.toArrayUnsafe());
    UInt256 b1 = UInt256.fromBytesBE(value1.toArrayUnsafe());

    UInt256 result = b0.and(b1);
    byte[] resultArray = result.toBytesBE();
    frame.pushStackItem(Bytes.wrap(resultArray));

    return addSuccess;
  }
}
