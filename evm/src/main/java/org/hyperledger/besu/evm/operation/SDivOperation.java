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
import org.apache.tuweni.bytes.Bytes32;

/** The SDiv operation. */
public class SDivOperation extends AbstractFixedCostOperation {

  private static final OperationResult sdivSuccess = new OperationResult(5, null);

  /**
   * Instantiates a new SDiv operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SDivOperation(final GasCalculator gasCalculator) {
    super(0x05, "SDIV", 2, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Performs SDiv operation.
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
      final BigInteger b1 =
          value0.size() < 32
              ? new BigInteger(1, value0.toArrayUnsafe())
              : new BigInteger(value0.toArrayUnsafe());
      final BigInteger b2 =
          value1.size() < 32
              ? new BigInteger(1, value1.toArrayUnsafe())
              : new BigInteger(value1.toArrayUnsafe());
      final BigInteger result = b1.divide(b2);
      Bytes resultBytes = Bytes.wrap(result.toByteArray());
      if (resultBytes.size() > 32) {
        resultBytes = resultBytes.slice(resultBytes.size() - 32, 32);
      }

      frame.pushStackItem(Bytes32.leftPad(resultBytes, result.signum() < 0 ? (byte) 0xFF : 0x00));
    }

    return sdivSuccess;
  }
}
