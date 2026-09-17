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

/** The SGt operation. */
public class SGtOperation extends AbstractFixedCostOperation {

  /** The SGt operation success result. */
  static final OperationResult sgtSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new SGt operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SGtOperation(final GasCalculator gasCalculator) {
    super(0x13, "SGT", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Performs SGt operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    final Bytes value0 = frame.popStackItem();
    final Bytes value1 = frame.popStackItem();

    final BigInteger b0 =
        value0.size() < 32
            ? new BigInteger(1, value0.toArrayUnsafe())
            : new BigInteger(value0.toArrayUnsafe());
    final BigInteger b1 =
        value1.size() < 32
            ? new BigInteger(1, value1.toArrayUnsafe())
            : new BigInteger(value1.toArrayUnsafe());

    final Bytes result = b0.compareTo(b1) > 0 ? BYTES_ONE : Bytes.EMPTY;

    frame.pushStackItem(result);

    return sgtSuccess;
  }
}
