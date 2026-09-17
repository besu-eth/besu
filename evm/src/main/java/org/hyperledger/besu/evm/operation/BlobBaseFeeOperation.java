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

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The Blob Base fee operation. */
public class BlobBaseFeeOperation extends AbstractFixedCostOperation {

  /**
   * Instantiates a new Blob Base fee operation.
   *
   * @param gasCalculator the gas calculator
   */
  public BlobBaseFeeOperation(final GasCalculator gasCalculator) {
    super(0x4a, "BLOBBASEFEE", 0, 1, gasCalculator, gasCalculator.getBaseTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {

    final Wei blobGasPrice = frame.getBlobGasPrice();
    frame.pushStackItem(blobGasPrice.toBytes());
    return successResponse;
  }
}
