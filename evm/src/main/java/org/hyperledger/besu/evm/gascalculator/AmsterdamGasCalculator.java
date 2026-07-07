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

  // --- EIP-8038: state-access gas repricing ---

  /** EIP-8038: cold account access cost (was 2,600). */
  private static final long COLD_ACCOUNT_ACCESS = 3_000L;

  /** EIP-8038: cold storage slot access cost (was 2,100). */
  private static final long COLD_STORAGE_ACCESS = 3_000L;

  /** EIP-8038: account write cost (value-bearing CALL / new account). */
  private static final long ACCOUNT_WRITE = 8_000L;

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

  // --- EIP-2780: resource-based intrinsic transaction gas ---

  /** EIP-2780: sender cost (ECDSA recovery + sender access + sender write). Replaces 21,000. */
  private static final long TX_BASE = 12_000L;

  /** EIP-2780: per data token; a calldata token is 1 (zero byte) or 4 (non-zero byte). */
  private static final long TX_DATA_TOKEN_STANDARD = 4L;

  /**
   * EIP-2780: contract-creation recipient access = ACCOUNT_WRITE + COLD_STORAGE_ACCESS = 11,000.
   */
  private static final long CREATE_ACCESS = ACCOUNT_WRITE + COLD_STORAGE_ACCESS;

  /**
   * EIP-2780: top-level contract-creation transaction cost, equal to the CREATE recipient access.
   */
  private static final long TX_CREATE_COST = CREATE_ACCESS;

  /** EIP-2780: recipient balance-write cost charged in intrinsic gas on a value transfer. */
  private static final long TX_VALUE_COST = 4_244L;

  /** EIP-2780/EIP-7708: transfer-log cost charged in intrinsic gas on a value transfer. */
  private static final long TRANSFER_LOG_COST = 1_756L;

  /** EIP-2780: access-list entry costs now align with cold access (3,000 each). */
  private static final long TX_ACCESS_LIST_ADDRESS = COLD_ACCOUNT_ACCESS;

  private static final long TX_ACCESS_LIST_STORAGE_KEY = COLD_STORAGE_ACCESS;

  /**
   * EIP-2780: regular gas per EIP-7702 authorization, in addition to {@link #ACCOUNT_WRITE}:
   * AUTH_TUPLE_BYTES(101) * TX_DATA_TOKEN_FLOOR(16) + ECRECOVER(3000) + COLD_ACCOUNT_ACCESS(3000) +
   * 2 * WARM_ACCESS(100) = 7,816.
   */
  private static final long REGULAR_PER_AUTH_BASE_COST =
      101L * 16L + 3_000L + COLD_ACCOUNT_ACCESS + 2L * 100L;

  /** EIP-3860 init code word cost (2 gas per 32-byte word), charged in intrinsic for creations. */
  private static final long CODE_INIT_PER_WORD = 2L;

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
    // EIP-2780/EIP-8038 baseline (3000 per address, 3000 per key) plus EIP-7981 data floor
    // (1280 per address, 2048 per storage key), so access list data is always charged
    // at floor rate regardless of which branch of the gasUsed max() wins.
    return clampedAdd(
        clampedMultiply(addresses, TX_ACCESS_LIST_ADDRESS + ACCESS_LIST_ADDRESS_FLOOR_COST),
        clampedMultiply(
            storageSlots, TX_ACCESS_LIST_STORAGE_KEY + ACCESS_LIST_STORAGE_KEY_FLOOR_COST));
  }

  @Override
  public long getMinimumTransactionCost() {
    // EIP-2780: TX_BASE replaces the flat 21,000 minimum.
    return TX_BASE;
  }

  @Override
  public long transactionIntrinsicGasCost(final Transaction transaction, final long baselineGas) {
    // EIP-2780: regular intrinsic gas =
    //   TX_BASE + data_cost + recipient_regular + access_list_cost + auth_regular
    // where baselineGas already carries access_list_cost + auth_regular (accessListGasCost +
    // delegateCodeGasCost from transactionIntrinsicRegularGas).
    final int payloadSize = transaction.getPayload().size();
    final long zeroBytes = transaction.getPayloadZeroBytes();
    final long nonZeroBytes = payloadSize - zeroBytes;
    // tokens_in_calldata = zeroBytes * 1 + nonZeroBytes * 4; data_cost = tokens * TX_DATA_TOKEN_STD
    final long tokens = clampedAdd(zeroBytes, nonZeroBytes * 4L);
    final long dataCost = tokens * TX_DATA_TOKEN_STANDARD;

    final long recipientRegular;
    final boolean valueTransfer = transaction.getValue().getAsBigInteger().signum() > 0;
    if (transaction.isContractCreation()) {
      long create = CREATE_ACCESS + initCodeCost(payloadSize);
      if (valueTransfer) {
        create += TRANSFER_LOG_COST;
      }
      recipientRegular = create;
    } else if (isSelfTransfer(transaction)) {
      recipientRegular = 0L;
    } else {
      long call = COLD_ACCOUNT_ACCESS;
      if (valueTransfer) {
        call += TRANSFER_LOG_COST + TX_VALUE_COST;
      }
      recipientRegular = call;
    }

    return clampedAdd(clampedAdd(TX_BASE, dataCost), clampedAdd(recipientRegular, baselineGas));
  }

  /** EIP-2780: a self-transfer (sender == recipient) skips the recipient and value charges. */
  private static boolean isSelfTransfer(final Transaction transaction) {
    return transaction.getTo().map(to -> to.equals(transaction.getSender())).orElse(false);
  }

  /** EIP-3860 init code cost: CODE_INIT_PER_WORD * ceil(len / 32). */
  private static long initCodeCost(final int initCodeLength) {
    return CODE_INIT_PER_WORD * ((initCodeLength + 31L) / 32L);
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
    // EIP-2780: (ACCOUNT_WRITE + REGULAR_PER_AUTH_BASE_COST) = 8,000 + 7,816 = 15,816 per
    // delegation (regular portion only; state gas charged separately).
    return (ACCOUNT_WRITE + REGULAR_PER_AUTH_BASE_COST) * delegateCodeListLength;
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
