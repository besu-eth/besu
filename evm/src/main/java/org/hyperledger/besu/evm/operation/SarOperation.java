/*
 * Copyright ConsenSys AG.
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

import static org.apache.tuweni.bytes.Bytes32.leftPad;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;

/** The Sar operation. */
public class SarOperation extends AbstractFixedCostOperation {

  /** The Sar operation success result. */
  static final OperationResult sarSuccess = new OperationResult(3, null);

  private static final Bytes ALL_BITS =
      Bytes.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  /**
   * Instantiates a new Sar operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SarOperation(final GasCalculator gasCalculator) {
    super(0x1d, "SAR", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Performs sar operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    Bytes shiftAmount = frame.popStackItem();
    final Bytes value = leftPad(frame.popStackItem());
    final boolean negativeNumber = value.get(0) < 0;
    if (shiftAmount.size() > 4 && (shiftAmount = shiftAmount.trimLeadingZeros()).size() > 4) {
      frame.pushStackItem(negativeNumber ? ALL_BITS : Bytes.EMPTY);
    } else {
      final int shiftAmountInt = shiftAmount.toInt();

      if (shiftAmountInt >= 256 || shiftAmountInt < 0) {
        frame.pushStackItem(negativeNumber ? ALL_BITS : Bytes.EMPTY);
      } else {
        // first perform standard shift right.
        Bytes result = value.shiftRight(shiftAmountInt);

        // if a negative number, carry through the sign.
        if (negativeNumber) {
          final Bytes significantBits = ALL_BITS.shiftLeft(256 - shiftAmountInt);
          result = result.or(significantBits);
        }
        frame.pushStackItem(result);
      }
    }
    return sarSuccess;
  }
}
