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
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes32;

/** The Call data load operation. */
public class CallDataLoadOperation extends AbstractFixedCostOperation {

  /**
   * Instantiates a new Call data load operation.
   *
   * @param gasCalculator the gas calculator
   */
  public CallDataLoadOperation(final GasCalculator gasCalculator) {
    super(0x35, "CALLDATALOAD", 1, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    final Bytes startWord = frame.popStackItem().trimLeadingZeros();

    // If the start index doesn't fit in an int, it comes after anything in data, and so the
    // returned
    // word should be zero.
    if (startWord.size() > 4) {
      frame.pushStackItem(Bytes.EMPTY);
      return successResponse;
    }

    final int offset = startWord.toInt();
    if (offset < 0) {
      frame.pushStackItem(Bytes.EMPTY);
      return successResponse;
    }
    final Bytes data = frame.getInputData();
    final MutableBytes32 res = MutableBytes32.create();
    if (offset < data.size()) {
      final Bytes toCopy = data.slice(offset, Math.min(Bytes32.SIZE, data.size() - offset));
      toCopy.copyTo(res, 0);
      frame.pushStackItem(res.copy());
    } else {
      frame.pushStackItem(Bytes.EMPTY);
    }

    return successResponse;
  }
}
