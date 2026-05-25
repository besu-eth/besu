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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;

/** The Div operation. */
public class DivOperation extends AbstractFixedCostOperation {

  /** The Div success. */
  static final OperationResult divSuccess = new OperationResult(5, null);

  /**
   * Instantiates a new Div operation.
   *
   * @param gasCalculator the gas calculator
   */
  public DivOperation(final GasCalculator gasCalculator) {
    super(0x04, "DIV", 2, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Performs Div operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {

    final Bytes value0 = frame.popStackItem();
    final Bytes value1 = frame.popStackItem();

    if (value1.isZero()) {
      frame.pushStackItem(Bytes.EMPTY);
    } else {
      BigInteger b1 = new BigInteger(1, value0.toArrayUnsafe());
      BigInteger b2 = new BigInteger(1, value1.toArrayUnsafe());
      final BigInteger result = b1.divide(b2);

      // because it's unsigned there is a change a 33 byte result will occur
      // there is no toByteArrayUnsigned so we have to check and trim
      byte[] resultArray = result.toByteArray();
      int length = resultArray.length;
      if (length > 32) {
        frame.pushStackItem(Bytes.wrap(resultArray, length - 32, 32));
      } else {
        frame.pushStackItem(Bytes.wrap(resultArray));
      }
    }

    return divSuccess;
  }
}
