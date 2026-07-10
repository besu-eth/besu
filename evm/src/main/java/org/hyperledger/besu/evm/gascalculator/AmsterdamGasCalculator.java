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
 *   <LI>EIP-8038: state-access gas repricing (cold access, account/storage write, CALL value,
 *       CREATE/CREATE2 access, access-list per-entry cost)
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

  // --- EIP-8038: state-access gas repricing (see the EIP for the authoritative values) ---

  /** Cold account access cost. */
  protected static final long COLD_ACCOUNT_ACCESS = 3_000L;

  /** Cold storage slot access cost. */
  protected static final long COLD_STORAGE_ACCESS = 3_000L;

  /** Account write cost (value-bearing CALL / new account). */
  protected static final long ACCOUNT_WRITE = 8_000L;

  /**
   * Flat write cost charged once per slot, on its first change in the transaction. Replaces the
   * Berlin SSTORE set/reset distinction; the set-from-zero surcharge now lives entirely in state
   * gas (see {@link Eip8037StateGasCostCalculator#storageSetStateGas}).
   */
  private static final long STORAGE_WRITE = 10_000L;

  /** Refund for clearing a slot: {@code (STORAGE_WRITE + COLD_STORAGE_ACCESS) * 4800 / 5000}. */
  private static final long STORAGE_CLEAR_REFUND =
      (STORAGE_WRITE + COLD_STORAGE_ACCESS) * 4800L / 5000L;

  /**
   * Regular-gas state-access cost for {@code CREATE}/{@code CREATE2}: {@code ACCOUNT_WRITE +
   * COLD_ACCOUNT_ACCESS}. The new-account state creation cost is charged separately as state gas
   * (see {@link Eip8037StateGasCostCalculator}).
   */
  private static final long CREATE_ACCESS = ACCOUNT_WRITE + COLD_ACCOUNT_ACCESS;

  /** Per-word copy cost used for EXTCODECOPY memory-copy accounting. */
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
    // EIP-8038: per-entry access cost is the cold access cost for both addresses and storage keys.
    // EIP-7981: plus the access-list data floor, so the data is always charged at the floor rate
    // regardless of which branch of the gasUsed max() wins.
    return clampedAdd(
        clampedMultiply(addresses, clampedAdd(COLD_ACCOUNT_ACCESS, ACCESS_LIST_ADDRESS_FLOOR_COST)),
        clampedMultiply(
            storageSlots, clampedAdd(COLD_STORAGE_ACCESS, ACCESS_LIST_STORAGE_KEY_FLOOR_COST)));
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
    return COLD_STORAGE_ACCESS;
  }

  @Override
  public long getColdAccountAccessCost() {
    return COLD_ACCOUNT_ACCESS;
  }

  @Override
  public long getSStoreColdAccessGasCost() {
    // The warm access base is already folded into calculateStorageCost, so SSTORE adds only the
    // cold surcharge on top of it: full cold access minus that warm base.
    return COLD_STORAGE_ACCESS - WARM_STORAGE_READ_COST;
  }

  @Override
  public long callValueTransferGasCost() {
    // The stipend is charged here but handed back to the callee via getAdditionalCallStipend(), so
    // the net caller cost for the value transfer is ACCOUNT_WRITE.
    return ACCOUNT_WRITE + ADDITIONAL_CALL_STIPEND;
  }

  @Override
  public long getExtCodeSizeOperationGasCost() {
    // Extra "code reading" surcharge on top of the account access added by the operation.
    return WARM_STORAGE_READ_COST;
  }

  @Override
  public long extCodeCopyOperationGasCost(
      final MessageFrame frame, final long offset, final long length) {
    // Extra "code reading" surcharge (the base argument) on top of the account access added by the
    // operation.
    return copyWordsToMemoryGasCost(
        frame, WARM_STORAGE_READ_COST, COPY_WORD_GAS_COST, offset, length);
  }

  // --- EIP-8037 Gas Cost Overrides ---

  @Override
  public long txCreateCost() {
    // EIP-8038: CREATE/CREATE2 opcodes are charged CREATE_ACCESS in regular gas (plus the
    // new-account state-creation cost as state gas). The transaction-intrinsic create cost is
    // repriced separately by EIP-2780; txCreateExtraGasCost() below keeps the EIP-8037 value.
    return CREATE_ACCESS;
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
    // Regular gas only: the warm access base is always charged, plus a flat STORAGE_WRITE on the
    // first change to the slot this transaction (its current value still equals the original).
    // Dirty re-writes and no-ops pay the access base only. The set-from-zero surcharge is state
    // gas, not regular. (originalValue is only read when the value actually changes.)
    final UInt256 localCurrentValue = currentValue.get();
    final boolean firstChange =
        !localCurrentValue.equals(newValue) && originalValue.get().equals(localCurrentValue);
    return firstChange ? WARM_STORAGE_READ_COST + STORAGE_WRITE : WARM_STORAGE_READ_COST;
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
    // Regular-gas refunds (credited to the refund counter): the storage-clear refund is granted
    // when a slot is first cleared and reversed if that clear is later undone, and the flat
    // STORAGE_WRITE is refunded when the slot ends up back at its original value. There is no
    // per-set/reset distinction.
    final UInt256 localCurrentValue = currentValue.get();
    if (localCurrentValue.equals(newValue)) {
      return 0L;
    }
    final UInt256 localOriginalValue = originalValue.get();
    long refund = 0L;
    if (!localOriginalValue.isZero()) {
      if (!localCurrentValue.isZero() && newValue.isZero()) {
        // Slot cleared for the first time this transaction.
        refund += STORAGE_CLEAR_REFUND;
      } else if (localCurrentValue.isZero() && !newValue.isZero()) {
        // An earlier clear is being undone: the slot is written back to a non-zero value.
        // (x -> 0 -> 0 never reaches here — the current == new guard above returns first.)
        refund -= STORAGE_CLEAR_REFUND;
      }
    }
    if (localOriginalValue.equals(newValue)) {
      // Slot restored to its original value: refund the STORAGE_WRITE charged on the first change.
      refund += STORAGE_WRITE;
    }
    return refund;
  }
}
