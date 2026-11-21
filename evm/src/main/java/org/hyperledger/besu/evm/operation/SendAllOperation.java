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
import org.hyperledger.besu.evm.log.EIP7708TransferLogEmitter;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

/**
 * The SENDALL operation (EIP-4758), which replaces SELFDESTRUCT at opcode 0xFF.
 *
 * <p>It sends the whole balance of the executing account to the beneficiary and halts. Unlike
 * SELFDESTRUCT it never deletes the account, even one created in the same transaction, so code,
 * storage and nonce always persist. It is priced like the EIP-8037 SELFDESTRUCT it replaces.
 */
public class SendAllOperation extends AbstractOperation {

  /** The SENDALL opcode. */
  public static final int OPCODE = 0xFF;

  /**
   * Instantiates a new SENDALL operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SendAllOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "SENDALL", 1, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    final Address beneficiaryAddress = Words.toAddress(frame.popStackItem());
    return sendAll(frame, beneficiaryAddress, gasCalculator());
  }

  /**
   * Executes SENDALL once the beneficiary has been popped off the stack, which is the only part
   * that differs between the EVM v1 and v2 operations.
   *
   * @param frame the frame executing SENDALL
   * @param beneficiaryAddress the account receiving the balance
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult sendAll(
      final MessageFrame frame,
      final Address beneficiaryAddress,
      final GasCalculator gasCalculator) {
    // The state-independent cost is checked before any account is read.
    final boolean beneficiaryIsWarm =
        frame.isAddressWarm(beneficiaryAddress) || gasCalculator.isPrecompile(beneficiaryAddress);
    final long beneficiaryAccessCost =
        beneficiaryIsWarm ? 0L : gasCalculator.getColdAccountAccessCost();
    final long staticCost =
        gasCalculator.selfDestructOperationStaticGasCost() + beneficiaryAccessCost;
    if (frame.getRemainingGas() < staticCost) {
      return new OperationResult(staticCost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    frame.warmUpAddress(beneficiaryAddress);

    final WorldUpdater worldUpdater = frame.getWorldUpdater();
    final Account beneficiaryNullable = worldUpdater.get(beneficiaryAddress);
    recordAccess(frame, beneficiaryAddress);
    final Address originatorAddress = frame.getRecipientAddress();
    final MutableAccount originatorAccount = worldUpdater.getAccount(originatorAddress);
    recordAccess(frame, originatorAddress);
    final Wei originatorBalance = originatorAccount.getBalance();

    // EIP-8038: ACCOUNT_WRITE is added when a positive balance is sent to a dead beneficiary.
    final long cost =
        gasCalculator.selfDestructOperationGasCost(beneficiaryNullable, originatorBalance)
            + beneficiaryAccessCost;
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // EIP-8037: a sweep that creates the beneficiary pays NEW_ACCOUNT state gas. Execution gas
    // comes off first, because state gas can spill into the remaining gas.
    if ((beneficiaryNullable == null || beneficiaryNullable.isEmpty())
        && !originatorBalance.isZero()) {
      frame.decrementRemainingGas(cost);
      if (!frame.consumeStateGas(gasCalculator.stateGasCostCalculator().newAccountStateGas())) {
        return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
      }
      // the EVM loop deducts the execution gas through the operation result
      frame.incrementRemainingGas(cost);
    }

    final MutableAccount beneficiaryAccount = worldUpdater.getOrCreate(beneficiaryAddress);
    originatorAccount.decrementBalance(originatorBalance);
    beneficiaryAccount.incrementBalance(originatorBalance);

    // EIP-7708: only a transfer to another account is logged; a sweep to itself moves nothing.
    EIP7708TransferLogEmitter.INSTANCE.emitTransferLog(
        frame, originatorAddress, beneficiaryAddress, originatorBalance);

    // Tracers read the swept value from the refunds.
    frame.addRefund(beneficiaryAddress, originatorBalance);
    frame.setState(MessageFrame.State.CODE_SUCCESS);
    return new OperationResult(cost, null);
  }

  // EIP-7928: both accounts enter the block access list, as through AbstractOperation.getAccount.
  private static void recordAccess(final MessageFrame frame, final Address address) {
    final var accessList = frame.getEip7928AccessList();
    if (accessList.isPresent()) {
      accessList.get().addTouchedAccount(address);
    }
  }
}
