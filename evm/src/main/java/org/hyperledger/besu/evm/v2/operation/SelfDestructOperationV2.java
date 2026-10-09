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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.log.TransferLogEmitter;

/** The Self destruct operation. */
public class SelfDestructOperationV2 extends AbstractOperationV2 {

  private final boolean eip6780Semantics;
  private final TransferLogEmitter transferLogEmitter;

  /**
   * Instantiates a new Self destruct operation.
   *
   * @param gasCalculator the gas calculator
   * @param eip6780Semantics whether only accounts created in the same transaction are destroyed
   * @param transferLogEmitter the emitter of the log for the transferred balance
   */
  public SelfDestructOperationV2(
      final GasCalculator gasCalculator,
      final boolean eip6780Semantics,
      final TransferLogEmitter transferLogEmitter) {
    super(0xFF, "SELFDESTRUCT", 1, 0, gasCalculator);
    this.eip6780Semantics = eip6780Semantics;
    this.transferLogEmitter = transferLogEmitter;
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    // checking for static violations first means fewer account accesses
    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final int top = frame.stackTopV2();
    final Address beneficiaryAddress = readAddressAt(frame.stackDataV2(), top, 0);
    frame.setTopV2(top - 1);

    final boolean beneficiaryIsWarm =
        frame.warmUpAddress(beneficiaryAddress) || gasCalculator().isPrecompile(beneficiaryAddress);
    final long beneficiaryAccessCost =
        beneficiaryIsWarm ? 0L : gasCalculator().getColdAccountAccessCost();
    final long staticCost =
        gasCalculator().selfDestructOperationStaticGasCost() + beneficiaryAccessCost;

    if (frame.getRemainingGas() < staticCost) {
      return new OperationResult(staticCost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Account beneficiaryNullable = getAccount(beneficiaryAddress, frame);
    final Address originatorAddress = frame.getRecipientAddress();
    final MutableAccount originatorAccount = getMutableAccount(originatorAddress, frame);
    final Wei originatorBalance = originatorAccount.getBalance();

    final long cost =
        gasCalculator().selfDestructOperationGasCost(beneficiaryNullable, originatorBalance)
            + beneficiaryAccessCost;
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // EIP-8037: Deduct execution gas before charging state gas (ordering requirement).
    frame.decrementRemainingGas(cost);

    // EIP-8037: Charge state gas when SELFDESTRUCT forces creation of an empty beneficiary.
    if ((beneficiaryNullable == null || beneficiaryNullable.isEmpty())
        && !originatorBalance.isZero()
        && !frame.consumeStateGas(gasCalculator().stateGasCostCalculator().newAccountStateGas())) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // Add execution gas back — the EVM loop will deduct it via the OperationResult.
    frame.incrementRemainingGas(cost);

    final MutableAccount beneficiaryAccount = getOrCreateAccount(beneficiaryAddress, frame);

    // Pre-Cancun, or for an account created in the same transaction, the account is destroyed;
    // otherwise only its balance is sent.
    final boolean willBeDestroyed =
        !eip6780Semantics || frame.wasCreatedInTransaction(originatorAccount.getAddress());

    originatorAccount.decrementBalance(originatorBalance);
    beneficiaryAccount.incrementBalance(originatorBalance);

    // EIP-7708: a transfer log for the value moved to the beneficiary. Pre-EIP-8246 a
    // self-referential destruction burned the balance, which is logged too.
    final boolean emitBurnLog =
        willBeDestroyed && !gasCalculator().isSelfDestructBalancePreserved();
    if (!originatorAddress.equals(beneficiaryAddress) || emitBurnLog) {
      transferLogEmitter.emitSelfDestructLog(
          frame, originatorAddress, beneficiaryAddress, originatorBalance);
    }

    // EIP-8246 preserves the balance of a destroyed account until the transaction ends; before it,
    // the balance is zeroed here, which burns ether when the account is its own beneficiary.
    if (willBeDestroyed) {
      frame.addSelfDestruct(originatorAccount.getAddress());
      if (!gasCalculator().isSelfDestructBalancePreserved()) {
        originatorAccount.setBalance(Wei.ZERO);
      }
    }

    frame.addRefund(beneficiaryAddress, originatorBalance);
    frame.setState(MessageFrame.State.CODE_SUCCESS);
    return new OperationResult(cost, null);
  }
}
