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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.log.TransferLogEmitter;

/** The Self destruct operation. */
public class SelfDestructOperation extends AbstractOperation {

  final boolean eip6780Semantics;
  final TransferLogEmitter transferLogEmitter;

  /**
   * Instantiates a new Self destruct operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SelfDestructOperation(final GasCalculator gasCalculator) {
    this(gasCalculator, false, TransferLogEmitter.NOOP);
  }

  /**
   * Instantiates a new Self destruct operation, with an optional EIP-6780 semantics flag. EIP-6780
   * will only remove an account if the account was created within the current transaction. All
   * other semantics remain.
   *
   * @param gasCalculator the gas calculator
   * @param eip6780Semantics Enforce EIP6780 semantics.
   */
  public SelfDestructOperation(final GasCalculator gasCalculator, final boolean eip6780Semantics) {
    this(gasCalculator, eip6780Semantics, TransferLogEmitter.NOOP);
  }

  /**
   * Instantiates a new Self destruct operation with EIP-6780 and transfer log emission support.
   *
   * @param gasCalculator the gas calculator
   * @param eip6780Semantics Enforce EIP-6780 semantics (only destroy if created in same tx).
   * @param transferLogEmitter strategy for emitting transfer logs.
   */
  public SelfDestructOperation(
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

    // First calculate cost.  There's a bit of yak shaving getting values to calculate the cost.
    final Address beneficiaryAddress = Words.toAddress(frame.popStackItem());
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

    // With the cost we can test for out-of-gas early WithdrawalRequests
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // EIP-8037: Deduct regular gas before charging state gas (ordering requirement).
    frame.decrementRemainingGas(cost);

    // EIP-8037: Charge state gas for new account creation in SELFDESTRUCT
    if (!gasCalculator()
        .stateGasCostCalculator()
        .chargeSelfDestructNewAccountStateGas(frame, beneficiaryNullable, originatorBalance)) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // Add regular gas back — the EVM loop will deduct it via the OperationResult.
    frame.incrementRemainingGas(cost);

    // We passed preliminary checks, get mutable accounts.
    final MutableAccount beneficiaryAccount = getOrCreateAccount(beneficiaryAddress, frame);

    // Determine if the account will actually be destroyed (pre-Cancun or same-tx-create)
    // or if only a SENDALL will be executed
    final boolean willBeDestroyed =
        !eip6780Semantics || frame.wasCreatedInTransaction(originatorAccount.getAddress());

    // Do the "sweep," all modes send all originator balance to the beneficiary account.
    originatorAccount.decrementBalance(originatorBalance);
    beneficiaryAccount.incrementBalance(originatorBalance);

    // EIP-7708: if the contract will actually be destroyed and it is not a self transfer emit
    // Burn log
    if (!originatorAddress.equals(beneficiaryAddress) || willBeDestroyed) {
      transferLogEmitter.emitSelfDestructLog(
          frame, originatorAddress, beneficiaryAddress, originatorBalance);
    }

    // If we are actually destroying the originator (pre-Cancun or same-tx-create) we need to
    // explicitly zero out the account balance (destroying ether/value if the originator is the
    // beneficiary) as well as tag it for later self-destruct cleanup.
    if (willBeDestroyed) {
      frame.addSelfDestruct(originatorAccount.getAddress());
      originatorAccount.setBalance(Wei.ZERO);
    }

    // Add refund in message frame.
    frame.addRefund(beneficiaryAddress, originatorBalance);

    // Set frame to CODE_SUCCESS so that the frame performs a normal halt.
    frame.setState(MessageFrame.State.CODE_SUCCESS);

    return new OperationResult(cost, null);
  }
}
