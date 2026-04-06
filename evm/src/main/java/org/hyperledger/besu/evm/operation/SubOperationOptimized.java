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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;

/** The Sub (Subtract) operation. */
public class SubOperationOptimized extends AbstractFixedCostOperation {

  /** The Sub operation success result. */
  static final OperationResult subSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new Sub operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SubOperationOptimized(final GasCalculator gasCalculator) {
    super(0x03, "SUB", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame);
  }

  /**
   * Performs Sub operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    final Bytes value0 = frame.popStackItem();
    final Bytes value1 = frame.popStackItem();

    byte[] b0 = value0.toArrayUnsafe();
    byte[] b1 = value1.toArrayUnsafe();
    Bytes resultBytes = Bytes.wrap(UInt256.sub(b0, b1));

    frame.pushStackItem(resultBytes);
    return subSuccess;
  }
}
