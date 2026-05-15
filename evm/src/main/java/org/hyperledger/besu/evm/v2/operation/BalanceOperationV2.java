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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushWei;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushZero;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The Balance operation. */
public class BalanceOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new Balance operation.
   *
   * @param gasCalculator the gas calculator
   */
  public BalanceOperationV2(final GasCalculator gasCalculator) {
    super(0x31, "BALANCE", 1, 1, gasCalculator);
  }

  /**
   * Gets Balance operation Gas Cost plus warm storage read cost or cold account access cost.
   *
   * @param accountIsWarm true to add warm storage read cost, false to add cold account access cost
   * @return the long
   */
  protected long cost(final boolean accountIsWarm) {
    return gasCalculator().getBalanceOperationGasCost()
        + (accountIsWarm
            ? gasCalculator().getWarmStorageReadCost()
            : gasCalculator().getColdAccountAccessCost());
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final Address address = readAddressAt(stack, top, 0);
    final boolean accountIsWarm =
        frame.warmUpAddress(address) || gasCalculator().isPrecompile(address);
    final long cost = cost(accountIsWarm);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    final Account account = getAccount(address, frame);
    if (account == null) {
      pushZero(stack, top - 1);
    } else {
      pushWei(account.getBalance(), stack, top - 1);
    }
    // no setTopV2 needed -- pop 1 + push 1 = net 0
    return new OperationResult(cost, null);
  }
}
