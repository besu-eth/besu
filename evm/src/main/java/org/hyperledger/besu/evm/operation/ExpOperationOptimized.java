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
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;

/** The Exp operation, computed with {@link UInt256#exp(UInt256)}. */
public class ExpOperationOptimized extends AbstractOperation {

  /**
   * Instantiates a new Exp operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ExpOperationOptimized(final GasCalculator gasCalculator) {
    super(0x0A, "EXP", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, gasCalculator());
  }

  /**
   * Performs exp operation.
   *
   * @param frame the frame
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final GasCalculator gasCalculator) {
    final Bytes number = frame.popStackItem();
    final Bytes power = frame.popStackItem();

    final int numBytes = (power.bitLength() + 7) / 8;

    final long cost = gasCalculator.expOperationGasCost(numBytes);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final UInt256 base = UInt256.fromBytesBE(number.toArrayUnsafe());
    final UInt256 exponent = UInt256.fromBytesBE(power.toArrayUnsafe());
    frame.pushStackItem(Bytes.wrap(base.exp(exponent).toBytesBE()));
    return new OperationResult(cost, null);
  }
}
