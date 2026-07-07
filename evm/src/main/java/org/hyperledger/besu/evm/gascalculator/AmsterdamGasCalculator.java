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

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.internal.Words.clampedMultiply;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.List;
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
 *   <LI>EIP-8038: state-access gas repricing (cold access, account/storage write, CALL value)
 *   <LI>EIP-7928: gas cost per item for block access list size limit
 *   <LI>EIP-7976: calldata floor cost raised to 64 gas per byte
 *   <LI>EIP-7981: access list data priced at the 64 gas/byte floor
 * </UL>
 */
public class AmsterdamGasCalculator extends OsakaGasCalculator {

  // EIP-7976 / EIP-7981: floor cost of 64 gas per data byte (calldata or access list).
  private static final long TOTAL_COST_FLOOR_PER_BYTE = 64L;

  // Byte sizes of the RLP-encoded payload elements counted toward the access list data floor.
  private static final long ACCESS_LIST_ADDRESS_BYTES = 20L;
  private static final long ACCESS_LIST_STORAGE_KEY_BYTES = 32L;

  // EIP-7981: data floor contribution of an access list entry.
  // 20 address bytes * 64 gas/byte = 1280; each 32-byte storage key * 64 gas/byte = 2048.
  private static final long ACCESS_LIST_ADDRESS_FLOOR_COST =
      ACCESS_LIST_ADDRESS_BYTES * TOTAL_COST_FLOOR_PER_BYTE;
  private static final long ACCESS_LIST_STORAGE_KEY_FLOOR_COST =
      ACCESS_LIST_STORAGE_KEY_BYTES * TOTAL_COST_FLOOR_PER_BYTE;

  // EIP-8037: New regular gas constants for Amsterdam
  private static final long TX_CREATE_COST = 9_000L;

  // --- EIP-8038: state-access gas repricing ---

  /** Cold account access cost. */
  protected static final long COLD_ACCOUNT_ACCESS = 3_000L;

  /** Cold storage slot access cost. */
  protected static final long COLD_STORAGE_ACCESS = 3_000L;

  /** Account write cost (value-bearing CALL / new account). */
  protected static final long ACCOUNT_WRITE = 8_000L;

  // EIP-8038: flat write cost charged once per slot on its first change in the transaction
  // (replaces the Berlin SSTORE_SET 20,000 / SSTORE_RESET 2,900 distinction). The set-from-zero
  // surcharge moves entirely to state gas (Eip8037StateGasCostCalculator#storageSetStateGas).
  private static final long STORAGE_WRITE = 10_000L;

  // EIP-8038: storage clear refund = (STORAGE_WRITE + COLD_STORAGE_ACCESS 3,000) * 4800 / 5000 =
  // 12,480.
  private static final long REFUND_STORAGE_CLEAR =
      (STORAGE_WRITE + COLD_STORAGE_ACCESS) * 4800L / 5000L;
  private static final long NEGATIVE_REFUND_STORAGE_CLEAR = -REFUND_STORAGE_CLEAR;

  /** EIP-8038: per-word copy cost (3 gas) used for EXTCODECOPY memory copy accounting. */
  private static final long COPY_WORD_GAS_COST = 3L;

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

  @Override
  public long transactionFloorCost(final Transaction transaction) {
    // EIP-7976: uniform 64 gas per calldata byte, so zero/non-zero split is irrelevant.
    // EIP-7981: include access list bytes in the data floor so they can't be used to bypass it.
    final long calldataBytes = transaction.getPayload().size();
    final long accessListBytes =
        transaction.getAccessList().map(AmsterdamGasCalculator::accessListBytes).orElse(0L);
    return clampedAdd(
        getMinimumTransactionCost(),
        clampedMultiply(clampedAdd(calldataBytes, accessListBytes), TOTAL_COST_FLOOR_PER_BYTE));
  }

  @Override
  public long accessListGasCost(final int addresses, final int storageSlots) {
    // EIP-2930 baseline (2400 per address, 1900 per key) plus EIP-7981 data floor
    // (1280 per address, 2048 per storage key), so access list data is always charged
    // at floor rate regardless of which branch of the gasUsed max() wins.
    return clampedAdd(
        super.accessListGasCost(addresses, storageSlots),
        clampedAdd(
            clampedMultiply(addresses, ACCESS_LIST_ADDRESS_FLOOR_COST),
            clampedMultiply(storageSlots, ACCESS_LIST_STORAGE_KEY_FLOOR_COST)));
  }

  private static long accessListBytes(final List<AccessListEntry> accessList) {
    long bytes = 0L;
    for (final AccessListEntry entry : accessList) {
      bytes +=
          ACCESS_LIST_ADDRESS_BYTES + ACCESS_LIST_STORAGE_KEY_BYTES * entry.storageKeys().size();
    }
    return bytes;
  }

  // --- EIP-8038 state-access gas repricing ---

  @Override
  public long getColdSloadCost() {
    // EIP-8038: cold storage slot access raised to 3,000.
    return COLD_STORAGE_ACCESS;
  }

  @Override
  public long getColdAccountAccessCost() {
    // EIP-8038: cold account access raised to 3,000.
    return COLD_ACCOUNT_ACCESS;
  }

  @Override
  public long getSStoreColdAccessGasCost() {
    // EIP-8038: SSTORE access is a full cold/warm cost (3,000 / 100), so the cold surcharge added
    // on top of the warm base baked into calculateStorageCost is COLD_STORAGE_ACCESS - WARM_ACCESS.
    return COLD_STORAGE_ACCESS - WARM_STORAGE_READ_COST;
  }

  @Override
  public long callValueTransferGasCost() {
    // EIP-8038: CALL_VALUE = ACCOUNT_WRITE (8,000) + CALL_STIPEND (2,300) = 10,300. The 2,300
    // stipend is still handed to the callee via getAdditionalCallStipend().
    return ACCOUNT_WRITE + ADDITIONAL_CALL_STIPEND;
  }

  @Override
  public long getExtCodeSizeOperationGasCost() {
    // EIP-8038: EXTCODESIZE pays an extra WARM_ACCESS (100) "code reading cost" on top of the
    // cold/warm account access.
    return WARM_STORAGE_READ_COST;
  }

  @Override
  public long extCodeCopyOperationGasCost(
      final MessageFrame frame, final long offset, final long length) {
    // EIP-8038: EXTCODECOPY pays an extra WARM_ACCESS (100) "code reading cost" (the base argument)
    // on top of the cold/warm account access added by the operation.
    return copyWordsToMemoryGasCost(
        frame, WARM_STORAGE_READ_COST, COPY_WORD_GAS_COST, offset, length);
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
    // State gas for new accounts (112 * cpsb) is charged at the call site (AbstractCallOperation).
    return staticCallCost;
  }

  @Override
  public long calculateStorageCost(
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    // EIP-8038: warm access base (100) always charged; flat STORAGE_WRITE (10,000) on the first
    // change to the slot this transaction. The set-from-zero surcharge is state gas, not regular.
    final UInt256 localCurrentValue = currentValue.get();
    if (localCurrentValue.equals(newValue)) {
      return WARM_STORAGE_READ_COST;
    }
    final UInt256 localOriginalValue = originalValue.get();
    if (localOriginalValue.equals(localCurrentValue)) {
      // First change to this slot in the transaction.
      return WARM_STORAGE_READ_COST + STORAGE_WRITE;
    }
    // Slot already changed earlier in the transaction (dirty): access only.
    return WARM_STORAGE_READ_COST;
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
    // EIP-8037: leftover reservoir is unspent state gas returned to the user.
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
    final long floorCost = transactionFloorCost(transaction);
    return gasLimit - Math.max(gasUsed, floorCost);
  }

  @Override
  public long calculateStorageRefundAmount(
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    // EIP-8038 refund model: no per-set/reset distinction; the flat STORAGE_WRITE is refunded when
    // the slot is restored to its original value, and the storage-clear refund is the larger
    // 12,480.
    final UInt256 localCurrentValue = currentValue.get();
    if (localCurrentValue.equals(newValue)) {
      return 0L;
    }
    final UInt256 localOriginalValue = originalValue.get();
    long refund = 0L;
    if (!localOriginalValue.isZero()) {
      if (!localCurrentValue.isZero() && newValue.isZero()) {
        // Storage cleared for the first time this transaction.
        refund += REFUND_STORAGE_CLEAR;
      } else if (localCurrentValue.isZero()) {
        // A clear refund issued earlier this transaction is being reversed.
        refund += NEGATIVE_REFUND_STORAGE_CLEAR;
      }
    }
    if (localOriginalValue.equals(newValue)) {
      // Slot restored to its original value: refund the STORAGE_WRITE charged on the first change.
      refund += STORAGE_WRITE;
    }
    return refund;
  }
}
