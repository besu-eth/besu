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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** The CLZ operation. */
public class CountLeadingZerosOperation extends AbstractFixedCostOperation {

  /** The CLZ operation success result. */
  static final OperationResult clzSuccess = new OperationResult(5, null);

  /**
   * Instantiates a new Count Leading Zeros Operation
   *
   * @param gasCalculator the gas calculator
   */
  public CountLeadingZerosOperation(final GasCalculator gasCalculator) {
    super(0x1e, "CLZ", 1, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Static operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    Bytes value = frame.popStackItem();
    final int numberOfLeadingZeros;
    if (value.size() > Bytes32.SIZE) {
      // should not happen but trim just in case
      value = value.slice(value.size() - Bytes32.SIZE, Bytes32.SIZE);
    }
    numberOfLeadingZeros = value.numberOfLeadingZeros() + (Bytes32.SIZE - value.size()) * 8;
    frame.pushStackItem(Words.intBytes(numberOfLeadingZeros));
    return clzSuccess;
  }
}
