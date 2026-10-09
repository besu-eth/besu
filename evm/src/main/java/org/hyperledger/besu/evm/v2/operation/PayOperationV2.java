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

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushLong;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readWeiAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The PAY operation (EIP-5920). */
public class PayOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new Pay operation.
   *
   * @param gasCalculator the gas calculator
   */
  public PayOperationV2(final GasCalculator gasCalculator) {
    super(0xfc, "PAY", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int toOffset = (top - 1) << 2;
    // an address is 20 bytes, so the 12 most significant bytes of the word must be zero
    if (stack[toOffset] != 0 || (stack[toOffset + 1] >>> 32) != 0) {
      return new OperationResult(0, ExceptionalHaltReason.ADDRESS_OUT_OF_RANGE);
    }
    final Address to = readAddressAt(stack, top, 0);
    if (!frame.stackHasItemsV2(2)) return UNDERFLOW_RESPONSE;
    final Wei value = readWeiAt(stack, top, 1);
    final boolean hasValue = value.greaterThan(Wei.ZERO);
    final Account recipient = getAccount(to, frame);

    final boolean accountIsWarm = frame.warmUpAddress(to);

    final long cost = cost(to, hasValue, recipient, accountIsWarm);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    if (!hasValue) {
      pushLong(1L, stack, top - 2);
      frame.setTopV2(top - 1);
      return new OperationResult(cost, null);
    }

    // EIP-5920 pays from the current address, the one ADDRESS returns, not from the caller
    final Address from = frame.getRecipientAddress();
    final MutableAccount fromAccount = getMutableAccount(from, frame);
    if (fromAccount == null || value.compareTo(fromAccount.getBalance()) > 0) {
      pushLong(0L, stack, top - 2);
      frame.setTopV2(top - 1);
      return new OperationResult(cost, null);
    }

    if (!from.equals(to)) {
      final MutableAccount recipientAccount = getOrCreateAccount(to, frame);
      fromAccount.decrementBalance(value);
      recipientAccount.incrementBalance(value);
    }

    pushLong(1L, stack, top - 2);
    frame.setTopV2(top - 1);
    return new OperationResult(cost, null);
  }

  private long cost(
      final Address to,
      final boolean hasValue,
      final Account recipient,
      final boolean accountIsWarm) {
    long cost = 0;
    if (hasValue) {
      cost = gasCalculator().callValueTransferGasCost();
    }
    if (accountIsWarm || gasCalculator().isPrecompile(to)) {
      return clampedAdd(cost, gasCalculator().getWarmStorageReadCost());
    }

    cost = clampedAdd(cost, gasCalculator().getColdAccountAccessCost());

    if (recipient == null && hasValue) {
      cost = clampedAdd(cost, gasCalculator().newAccountGasCost());
    }

    return cost;
  }
}
