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
package org.hyperledger.besu.evm.v2.operation;

import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushBytes;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readBytes32At;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes32;

/** The TLoad operation (EIP-1153). */
public class TLoadOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new TLoad operation.
   *
   * @param gasCalculator the gas calculator
   */
  public TLoadOperationV2(final GasCalculator gasCalculator) {
    super(0x5C, "TLOAD", 1, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final long cost = gasCalculator().getTransientLoadOperationGasCost();
    if (!frame.stackHasItemsV2(1)) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final Bytes32 slot = readBytes32At(stack, top, 0);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    pushBytes(frame.getTransientStorageValue(frame.getRecipientAddress(), slot), stack, top - 1);
    return new OperationResult(cost, null);
  }
}
