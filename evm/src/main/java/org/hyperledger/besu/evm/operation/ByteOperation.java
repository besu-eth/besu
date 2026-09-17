/*
 * Copyright contributors to Hyperledger Besu.
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

import org.apache.tuweni.bytes.Bytes;

/** The Byte operation. */
public class ByteOperation extends AbstractFixedCostOperation {

  /** The Byte operation success result. */
  static final OperationResult byteSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new Byte operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ByteOperation(final GasCalculator gasCalculator) {
    super(0x1A, "BYTE", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  private static Bytes getByte(final Bytes seq, final Bytes offset) {
    Bytes trimmedOffset = offset.trimLeadingZeros();
    if (trimmedOffset.size() > 1) {
      return Bytes.EMPTY;
    }
    final int index = trimmedOffset.toInt();

    int size = seq.size();
    int pos = index - 32 + size;
    if (pos >= size || pos < 0) {
      return Bytes.EMPTY;
    } else {
      final byte b = seq.get(pos);
      return Bytes.of(b);
    }
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Static Byte operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    final Bytes value0 = frame.popStackItem();
    final Bytes value1 = frame.popStackItem();

    // Stack items are reversed for the BYTE operation.
    final Bytes result = getByte(value1, value0);

    frame.pushStackItem(result);

    return byteSuccess;
  }
}
