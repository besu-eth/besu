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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.StackArithmetic;

/**
 * EVM v2 SELFBALANCE operation (0x47, Istanbul+).
 *
 * <p>Pushes the balance of the currently executing contract onto the stack. Fixed cost (low tier).
 */
public class SelfBalanceOperationV2 extends AbstractFixedCostOperationV2 {

  /**
   * Instantiates a new SelfBalance operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SelfBalanceOperationV2(final GasCalculator gasCalculator) {
    super(0x47, "SELFBALANCE", 0, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Execute SELFBALANCE on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack data
   * @return the operation result
   */
  public static Operation.OperationResult staticOperation(
      final MessageFrame frame, final long[] s) {
    if (!frame.stackHasSpace(1)) return OVERFLOW_RESPONSE;
    final int top = frame.stackTopV2();
    final Account account = getAccount(frame.getRecipientAddress(), frame);
    if (account == null) {
      frame.setTopV2(StackArithmetic.pushZero(s, top));
    } else {
      frame.setTopV2(StackArithmetic.pushWei(s, top, account.getBalance()));
    }
    return new Operation.OperationResult(5, null);
  }
}
