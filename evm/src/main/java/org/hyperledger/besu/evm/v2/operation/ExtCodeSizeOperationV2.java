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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.StackArithmetic;

/**
 * EVM v2 EXTCODESIZE operation (0x3B).
 *
 * <p>Pops an address from the stack and replaces it with the external account's code size. Applies
 * warm/cold account access cost.
 */
public class ExtCodeSizeOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new ExtCodeSize operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ExtCodeSizeOperationV2(final GasCalculator gasCalculator) {
    super(0x3B, "EXTCODESIZE", 1, 1, gasCalculator);
  }

  /**
   * Gas cost including warm/cold access.
   *
   * @param accountIsWarm whether the account was already warm
   * @return the total gas cost
   */
  protected long cost(final boolean accountIsWarm) {
    return gasCalculator().getExtCodeSizeOperationGasCost()
        + (accountIsWarm
            ? gasCalculator().getWarmStorageReadCost()
            : gasCalculator().getColdAccountAccessCost());
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2(), gasCalculator());
  }

  /**
   * Execute EXTCODESIZE on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack data
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator) {
    final long warmCost =
        gasCalculator.getExtCodeSizeOperationGasCost() + gasCalculator.getWarmStorageReadCost();
    final long coldCost =
        gasCalculator.getExtCodeSizeOperationGasCost() + gasCalculator.getColdAccountAccessCost();
    if (!frame.stackHasItems(1)) {
      return new OperationResult(warmCost, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    final int top = frame.stackTopV2();
    final Address address = StackArithmetic.toAddressAt(s, top, 0);
    final boolean accountIsWarm =
        frame.warmUpAddress(address) || gasCalculator.isPrecompile(address);
    final long cost = accountIsWarm ? warmCost : coldCost;
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    final Account account = getAccount(address, frame);
    // Overwrite in place (pop 1, push 1)
    final long codeSize = account == null ? 0L : account.getCode().size();
    StackArithmetic.putAt(s, top, 0, 0L, 0L, 0L, codeSize);
    return new OperationResult(cost, null);
  }
}
