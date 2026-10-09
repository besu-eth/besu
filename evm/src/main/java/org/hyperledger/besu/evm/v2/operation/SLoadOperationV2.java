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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushBytes32;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readBytes32At;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.units.bigints.UInt256;

/** The SLoad operation. */
public class SLoadOperationV2 extends AbstractOperationV2 {

  private final long warmCost;
  private final long coldCost;

  private final OperationResult warmSuccess;
  private final OperationResult coldSuccess;
  private final OperationResult underflowResponse;

  /**
   * Instantiates a new SLoad operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SLoadOperationV2(final GasCalculator gasCalculator) {
    super(0x54, "SLOAD", 1, 1, gasCalculator);
    final long baseCost = gasCalculator.getSloadOperationGasCost();
    warmCost = baseCost + gasCalculator.getWarmStorageReadCost();
    coldCost = baseCost + gasCalculator.getColdSloadCost();

    warmSuccess = new OperationResult(warmCost, null);
    coldSuccess = new OperationResult(coldCost, null);
    underflowResponse =
        new OperationResult(warmCost, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final Account account = getAccount(frame.getRecipientAddress(), frame);
    if (!frame.stackHasItemsV2(1)) return underflowResponse;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final Address address = account.getAddress();
    final UInt256 key = UInt256.fromBytes(readBytes32At(stack, top, 0));
    final boolean slotIsWarm = frame.warmUpStorage(address, key);
    final long cost = slotIsWarm ? warmCost : coldCost;
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    pushBytes32(getStorageValue(account, key, frame), stack, top - 1);
    return slotIsWarm ? warmSuccess : coldSuccess;
  }
}
