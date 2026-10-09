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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushHash;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushZero;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The Ext code hash operation. */
public class ExtCodeHashOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new Ext code hash operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ExtCodeHashOperationV2(final GasCalculator gasCalculator) {
    super(0x3F, "EXTCODEHASH", 1, 1, gasCalculator);
  }

  /**
   * Cost of Ext code hash operation, including the warm or cold account access.
   *
   * @param accountIsWarm whether the account was already warm
   * @return the long
   */
  protected long cost(final boolean accountIsWarm) {
    return gasCalculator().extCodeHashOperationGasCost()
        + (accountIsWarm
            ? gasCalculator().getWarmStorageReadCost()
            : gasCalculator().getColdAccountAccessCost());
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (!frame.stackHasItemsV2(1)) {
      return new OperationResult(cost(true), ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
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
    if (account == null || account.isEmpty()) {
      pushZero(stack, top - 1);
    } else {
      pushHash(account.getCodeHash(), stack, top - 1);
    }
    return new OperationResult(cost, null);
  }
}
