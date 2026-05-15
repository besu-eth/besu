/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.gascalculator;

import static org.hyperledger.besu.datatypes.Address.BLS12_MAP_FP2_TO_G2;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

/**
 * Gas Calculator for Prague
 *
 * <UL>
 *   <LI>Gas costs for EIP-7702 (Code Delegation)
 * </UL>
 */
public class PragueGasCalculator extends CancunGasCalculator {
  private static final long TOTAL_COST_FLOOR_PER_TOKEN = 10L;

  final long existingAccountGasRefund;

  /** Instantiates a new Prague Gas Calculator. */
  public PragueGasCalculator() {
    this(BLS12_MAP_FP2_TO_G2.getBytes().toArrayUnsafe()[19]);
  }

  /**
   * Instantiates a new Prague Gas Calculator
   *
   * @param maxPrecompile the max precompile
   */
  protected PragueGasCalculator(final int maxPrecompile) {
    super(maxPrecompile);
    this.existingAccountGasRefund = newAccountGasCost() - CodeDelegation.PER_AUTH_BASE_COST;
  }

  @Override
  public long delegateCodeGasCost(final int delegateCodeListLength) {
    return newAccountGasCost() * delegateCodeListLength;
  }

  @Override
  public long calculateDelegateCodeGasRefund(final long alreadyExistingAccounts) {
    return existingAccountGasRefund * alreadyExistingAccounts;
  }

  @Override
  public long calculateGasRefund(
      final Transaction transaction,
      final MessageFrame initialFrame,
      final long codeDelegationRefund) {

    final long refundAllowance =
        calculateRefundAllowance(transaction, initialFrame, codeDelegationRefund);

    final long executionGasUsed =
        transaction.getGasLimit() - initialFrame.getRemainingGas() - refundAllowance;
    final long totalGasUsed = Math.max(executionGasUsed, transactionFloorCost(transaction));
    return transaction.getGasLimit() - totalGasUsed;
  }

  private long calculateRefundAllowance(
      final Transaction transaction,
      final MessageFrame initialFrame,
      final long codeDelegationRefund) {
    final long selfDestructRefund =
        getSelfDestructRefundAmount() * initialFrame.getSelfDestructs().size();
    final long executionRefund =
        initialFrame.getGasRefund() + selfDestructRefund + codeDelegationRefund;
    // Integer truncation takes care of the floor calculation needed after the divide.
    final long maxRefundAllowance =
        (transaction.getGasLimit() - initialFrame.getRemainingGas()) / getMaxRefundQuotient();
    return Math.min(executionRefund, maxRefundAllowance);
  }

  @Override
  public long transactionFloorCost(final Transaction transaction) {
    return clampedAdd(
        getMinimumTransactionCost(),
        tokensInCallData(transaction.getPayload().size(), transaction.getPayloadZeroBytes())
            * TOTAL_COST_FLOOR_PER_TOKEN);
  }

  private long tokensInCallData(final long payloadSize, final long zeroBytes) {
    // as defined in https://eips.ethereum.org/EIPS/eip-7623#specification
    return clampedAdd(zeroBytes, (payloadSize - zeroBytes) * 4);
  }

  @Override
  public long calculateCodeDelegationResolutionGas(
      final MessageFrame frame, final Account targetAccount) {
    if (targetAccount == null) {
      return 0;
    }

    final Hash codeHash = targetAccount.getCodeHash();
    if (codeHash == null || codeHash.equals(Hash.EMPTY)) {
      return 0;
    }

    if (!hasCodeDelegation(targetAccount.getCode())) {
      return 0;
    }

    final Address targetAddress = CodeDelegationHelper.getTargetAddress(targetAccount.getCode());

    final boolean isWarm = isPrecompile(targetAddress) || frame.warmUpAddress(targetAddress);
    return isWarm ? getWarmStorageReadCost() : getColdAccountAccessCost();
  }
}
