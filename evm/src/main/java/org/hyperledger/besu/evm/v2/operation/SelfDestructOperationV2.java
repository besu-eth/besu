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
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.v2.StackArithmetic;

/** EVM v2 SELFDESTRUCT operation using long[] stack representation. */
public class SelfDestructOperationV2 extends AbstractOperationV2 {

  private final boolean eip6780Semantics;
  private final TransferLogEmitter transferLogEmitter;

  /**
   * Instantiates a new Self destruct operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SelfDestructOperationV2(final GasCalculator gasCalculator) {
    this(gasCalculator, false, TransferLogEmitter.NOOP);
  }

  /**
   * Instantiates a new Self destruct operation with optional EIP-6780 semantics.
   *
   * @param gasCalculator the gas calculator
   * @param eip6780Semantics enforce EIP-6780 semantics (only destroy if created in same tx)
   */
  public SelfDestructOperationV2(
      final GasCalculator gasCalculator, final boolean eip6780Semantics) {
    this(gasCalculator, eip6780Semantics, TransferLogEmitter.NOOP);
  }

  /**
   * Instantiates a new Self destruct operation with EIP-6780 and transfer log emission support.
   *
   * @param gasCalculator the gas calculator
   * @param eip6780Semantics enforce EIP-6780 semantics (only destroy if created in same tx)
   * @param transferLogEmitter strategy for emitting transfer logs
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
    return staticOperation(
        frame, frame.stackDataV2(), gasCalculator(), eip6780Semantics, transferLogEmitter);
  }

  /**
   * Performs SELFDESTRUCT operation.
   *
   * @param frame the frame
   * @param s the stack data array
   * @param gasCalculator the gas calculator
   * @param eip6780Semantics true if EIP-6780 semantics should apply
   * @param transferLogEmitter strategy for emitting transfer logs
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame,
      final long[] s,
      final GasCalculator gasCalculator,
      final boolean eip6780Semantics,
      final TransferLogEmitter transferLogEmitter) {
    // Check for static violations first — fewer account accesses on failure.
    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }

    if (!frame.stackHasItems(1)) {
      return new OperationResult(0, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }

    final int top = frame.stackTopV2();
    final Address beneficiaryAddress = StackArithmetic.toAddressAt(s, top, 0);
    frame.setTopV2(top - 1);

    final boolean beneficiaryIsWarm =
        frame.warmUpAddress(beneficiaryAddress) || gasCalculator.isPrecompile(beneficiaryAddress);
    final long beneficiaryAccessCost =
        beneficiaryIsWarm ? 0L : gasCalculator.getColdAccountAccessCost();
    final long staticCost =
        gasCalculator.selfDestructOperationStaticGasCost() + beneficiaryAccessCost;

    if (frame.getRemainingGas() < staticCost) {
      return new OperationResult(staticCost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Account beneficiaryNullable = getAccount(beneficiaryAddress, frame);
    final Address originatorAddress = frame.getRecipientAddress();
    final MutableAccount originatorAccount = getMutableAccount(originatorAddress, frame);
    final Wei originatorBalance = originatorAccount.getBalance();

    final long cost =
        gasCalculator.selfDestructOperationGasCost(beneficiaryNullable, originatorBalance)
            + beneficiaryAccessCost;

    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // EIP-8037: Deduct regular gas before charging state gas (ordering requirement).
    frame.decrementRemainingGas(cost);

    // EIP-8037: Charge state gas for new account creation in SELFDESTRUCT
    if (!gasCalculator
        .stateGasCostCalculator()
        .chargeSelfDestructNewAccountStateGas(frame, beneficiaryNullable, originatorBalance)) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // Add regular gas back — the EVM loop will deduct it via the OperationResult.
    frame.incrementRemainingGas(cost);

    final MutableAccount beneficiaryAccount = getOrCreateAccount(beneficiaryAddress, frame);

    final boolean willBeDestroyed =
        !eip6780Semantics || frame.wasCreatedInTransaction(originatorAccount.getAddress());

    // Transfer all originator balance to beneficiary.
    originatorAccount.decrementBalance(originatorBalance);
    beneficiaryAccount.incrementBalance(originatorBalance);

    // Emit transfer log if applicable.
    if (!originatorAddress.equals(beneficiaryAddress) || willBeDestroyed) {
      transferLogEmitter.emitSelfDestructLog(
          frame, originatorAddress, beneficiaryAddress, originatorBalance);
    }

    // If actually destroying the originator, zero its balance and tag for cleanup.
    if (willBeDestroyed) {
      frame.addSelfDestruct(originatorAccount.getAddress());
      originatorAccount.setBalance(Wei.ZERO);
    }

    frame.addRefund(beneficiaryAddress, originatorBalance);
    frame.setState(MessageFrame.State.CODE_SUCCESS);

    return new OperationResult(cost, null);
  }
}
