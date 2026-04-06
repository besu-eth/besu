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
package org.hyperledger.besu.evm.gascalculator;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.function.Supplier;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Gas Calculator for Amsterdam hard fork.
 *
 * <p>Introduces EIP-8037 multidimensional gas metering with state gas costs that depend on the
 * block gas limit. All state-creation costs that were previously charged as regular gas are split:
 * the state portion is charged as state gas (drawn from the reservoir), while the regular portion
 * is reduced.
 *
 * <UL>
 *   <LI>EIP-7928: gas cost per item for block access list size limit
 * </UL>
 */
public class AmsterdamGasCalculator extends OsakaGasCalculator {

  // EIP-8037: New regular gas constants for Amsterdam
  private static final long TX_CREATE_COST = 9_000L;

  // EIP-8037: SSTORE_SET regular gas drops from 20,000 to 2,900 (state portion charged separately)
  private static final long SSTORE_SET_GAS = SSTORE_RESET_GAS;

  // EIP-8037: 0→X→0 refund is 2,800 per spec (GAS_STORAGE_UPDATE - GAS_COLD_SLOAD -
  // GAS_WARM_ACCESS)
  private static final long SSTORE_SET_GAS_LESS_SLOAD_GAS = SSTORE_SET_GAS - WARM_STORAGE_READ_COST;

  // SSTORE_CLEARS_SCHEDULE unchanged from EIP-3529 (London): 4,800
  private static final long SSTORE_CLEARS_SCHEDULE = SSTORE_RESET_GAS + ACCESS_LIST_STORAGE_COST;
  private static final long NEGATIVE_SSTORE_CLEARS_SCHEDULE = -SSTORE_CLEARS_SCHEDULE;

  /**
   * EIP-7928: gas cost per item for block access list size limit (bal_items <= block_gas_limit /
   * ITEM_COST).
   */
  private static final long BLOCK_ACCESS_LIST_ITEM_COST = 2000L;

  /** The EIP-8037 state gas cost calculator. */
  private final Eip8037StateGasCostCalculator stateGasCostCalc =
      new Eip8037StateGasCostCalculator();

  /** Instantiates a new Amsterdam Gas Calculator. */
  public AmsterdamGasCalculator() {
    super();
  }

  /**
   * Instantiates a new Amsterdam Gas Calculator
   *
   * @param maxPrecompile the max precompile address from the L1 precompile range (0x01 - 0xFF)
   * @param maxL2Precompile max precompile address from the L2 precompile space (0x0100 - 0x01FF)
   */
  public AmsterdamGasCalculator(final int maxPrecompile, final int maxL2Precompile) {
    super(maxPrecompile, maxL2Precompile);
  }

  /**
   * Instantiates a new Amsterdam Gas Calculator, uses default P256_VERIFY as max L2 precompile.
   *
   * @param maxPrecompile the max precompile address from the L1 precompile range (0x01 - 0xFF)
   */
  public AmsterdamGasCalculator(final int maxPrecompile) {
    super(maxPrecompile);
  }

  @Override
  public StateGasCostCalculator stateGasCostCalculator() {
    return stateGasCostCalc;
  }

  @Override
  public long getBlockAccessListItemCost() {
    return BLOCK_ACCESS_LIST_ITEM_COST;
  }

  // --- EIP-8037 Gas Cost Overrides ---

  @Override
  public long txCreateCost() {
    return TX_CREATE_COST;
  }

  @Override
  protected long txCreateExtraGasCost() {
    return TX_CREATE_COST;
  }

  @Override
  public long codeDepositGasCost(final int codeSize) {
    // 6 * ceil(codeSize / 32) — hash cost only; state portion (cpsb * codeSize) charged separately
    return stateGasCostCalc.codeDepositHashGas(codeSize);
  }

  @Override
  public long callOperationGasCost(
      final MessageFrame frame,
      final long staticCallCost,
      final long stipend,
      final long inputDataOffset,
      final long inputDataLength,
      final long outputDataOffset,
      final long outputDataLength,
      final Wei transferValue,
      final Address recipientAddress,
      final boolean accountIsWarm) {
    // Same as SpuriousDragon but do NOT add newAccountGasCost().
    // State gas for new accounts (112 * cpsb) is charged via chargeCallNewAccountStateGas.
    return staticCallCost;
  }

  @Override
  public long calculateStorageCost(
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    // Same Berlin truth table, but SSTORE_SET_GAS is now 2,900 (state portion charged separately)
    final UInt256 localCurrentValue = currentValue.get();
    if (localCurrentValue.equals(newValue)) {
      return WARM_STORAGE_READ_COST;
    } else {
      final UInt256 localOriginalValue = originalValue.get();
      if (localOriginalValue.equals(localCurrentValue)) {
        return localOriginalValue.isZero() ? SSTORE_SET_GAS : SSTORE_RESET_GAS;
      } else {
        return WARM_STORAGE_READ_COST;
      }
    }
  }

  @Override
  public long selfDestructOperationGasCost(final Account recipient, final Wei inheritance) {
    // Always static cost (5,000). State gas (112 * cpsb) for new accounts charged separately.
    return selfDestructOperationStaticGasCost();
  }

  @Override
  public long delegateCodeGasCost(final int delegateCodeListLength) {
    // 7,500 per delegation (regular portion only, state gas charged separately)
    return stateGasCostCalc.authBaseRegularGas() * delegateCodeListLength;
  }

  @Override
  public long calculateDelegateCodeGasRefund(final long alreadyExistingAccounts) {
    // No refund needed — regular cost is lower, state gas uses its own refund path
    return 0L;
  }

  @Override
  public long calculateGasRefund(
      final Transaction transaction,
      final MessageFrame initialFrame,
      final long codeDelegationRefund) {

    final long gasLimit = transaction.getGasLimit();
    // EIP-8037: Include leftover reservoir in remaining gas so unspent state gas is returned.
    final long totalRemaining =
        initialFrame.getRemainingGas() + initialFrame.getStateGasReservoir();
    final long totalConsumed = gasLimit - totalRemaining;

    final long selfDestructRefund =
        getSelfDestructRefundAmount() * initialFrame.getSelfDestructs().size();
    final long executionRefund =
        initialFrame.getGasRefund() + selfDestructRefund + codeDelegationRefund;
    // 1/5 cap on total consumed gas (regular + state)
    final long maxRefundAllowance = totalConsumed / getMaxRefundQuotient();
    final long refundAllowance = Math.min(executionRefund, maxRefundAllowance);

    final long gasUsed = totalConsumed - refundAllowance;
    final long floorCost =
        transactionFloorCost(transaction.getPayload(), transaction.getPayloadZeroBytes());
    return gasLimit - Math.max(gasUsed, floorCost);
  }

  @Override
  public long calculateStorageRefundAmount(
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    // Same Berlin truth table structure, but with updated constants
    final UInt256 localCurrentValue = currentValue.get();
    if (localCurrentValue.equals(newValue)) {
      return 0L;
    } else {
      final UInt256 localOriginalValue = originalValue.get();
      if (localOriginalValue.equals(localCurrentValue)) {
        if (localOriginalValue.isZero()) {
          return 0L;
        } else if (newValue.isZero()) {
          return SSTORE_CLEARS_SCHEDULE;
        } else {
          return 0L;
        }
      } else {
        long refund = 0L;
        if (!localOriginalValue.isZero()) {
          if (localCurrentValue.isZero()) {
            refund = NEGATIVE_SSTORE_CLEARS_SCHEDULE;
          } else if (newValue.isZero()) {
            refund = SSTORE_CLEARS_SCHEDULE;
          }
        }

        if (localOriginalValue.equals(newValue)) {
          refund =
              refund
                  + (localOriginalValue.isZero()
                      ? SSTORE_SET_GAS_LESS_SLOAD_GAS
                      : SSTORE_RESET_GAS_LESS_SLOAD_GAS);
        }
        return refund;
      }
    }
  }
}
