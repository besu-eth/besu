/*
 * Copyright ConsenSys AG.
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
import static org.hyperledger.besu.evm.internal.Words.clampedToInt;
import static org.hyperledger.besu.evm.internal.Words.numWords;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.ExpOperation;

import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/** The Frontier gas calculator. */
public class FrontierGasCalculator implements GasCalculator {

  private static final long TX_DATA_ZERO_COST = 4L;

  private static final long TX_DATA_NON_ZERO_COST = 68L;

  /** Minimum base cost that every transaction needs to pay */
  protected static final long TX_BASE_COST = 21_000L;

  private static final long TX_CREATE_EXTRA_COST = 0L;

  private static final long CODE_DEPOSIT_BYTE_COST = 200L;

  private static final long ID_PRECOMPILED_BASE_GAS_COST = 15L;

  private static final long ID_PRECOMPILED_WORD_GAS_COST = 3L;

  private static final long ECREC_PRECOMPILED_GAS_COST = 3_000L;

  private static final long P256VERIFY_PRECOMPILED_GAS_COST = 6_900L;

  private static final long SHA256_PRECOMPILED_BASE_GAS_COST = 60L;

  private static final long SHA256_PRECOMPILED_WORD_GAS_COST = 12L;

  private static final long RIPEMD160_PRECOMPILED_WORD_GAS_COST = 120L;

  private static final long RIPEMD160_PRECOMPILED_BASE_GAS_COST = 600L;

  private static final long VERY_LOW_TIER_GAS_COST = 3L;

  private static final long LOW_TIER_GAS_COST = 5L;

  private static final long BASE_TIER_GAS_COST = 2L;

  private static final long MID_TIER_GAS_COST = 8L;

  private static final long HIGH_TIER_GAS_COST = 10L;

  private static final long CALL_OPERATION_BASE_GAS_COST = 40L;

  private static final long CALL_VALUE_TRANSFER_GAS_COST = 9_000L;

  /** additional call stipend to be used for gas estimate calculations */
  public static final long ADDITIONAL_CALL_STIPEND = 2_300L;

  private static final long NEW_ACCOUNT_GAS_COST = 25_000L;

  /** Yellow paper constant for the cost of creating a new contract on-chain */
  protected static final long CREATE_OPERATION_GAS_COST = 32_000L;

  private static final long COPY_WORD_GAS_COST = 3L;

  private static final long MEMORY_WORD_GAS_COST = 3L;

  private static final long BALANCE_OPERATION_GAS_COST = 20L;

  /** The constant BLOCKHASH_OPERATION_GAS_COST = 20gwei. */
  public static final long BLOCKHASH_OPERATION_GAS_COST = 20L;

  private static final long EXP_OPERATION_BASE_GAS_COST = 10L;

  private static final long EXP_OPERATION_BYTE_GAS_COST = 10L;

  private static final long EXT_CODE_BASE_GAS_COST = 20L;

  private static final long JUMPDEST_OPERATION_GAS_COST = 1L;

  private static final long LOG_OPERATION_BASE_GAS_COST = 375L;

  private static final long LOG_OPERATION_DATA_BYTE_GAS_COST = 8L;

  private static final long LOG_OPERATION_TOPIC_GAS_COST = 375L;

  private static final long KECCAK256_OPERATION_BASE_GAS_COST = 30L;

  /** The constant KECCAK256_OPERATION_WORD_GAS_COST. */
  static final long KECCAK256_OPERATION_WORD_GAS_COST = 6L;

  private static final long SLOAD_OPERATION_GAS_COST = 50L;

  /** The constant STORAGE_SET_GAS_COST. */
  public static final long STORAGE_SET_GAS_COST = 20_000L;

  /** The constant STORAGE_RESET_GAS_COST. */
  public static final long STORAGE_RESET_GAS_COST = 5_000L;

  /** The constant STORAGE_RESET_REFUND_AMOUNT. */
  public static final long STORAGE_RESET_REFUND_AMOUNT = 15_000L;

  private static final long SELF_DESTRUCT_REFUND_AMOUNT = 24_000L;

  /** Default constructor. */
  public FrontierGasCalculator() {
    // Default Constructor, for JavaDoc lint
  }

  @Override
  public long transactionIntrinsicGasCost(final Transaction transaction, final long baselineGas) {
    final long dynamicIntrinsicGasCost = dynamicIntrinsicGasCost(transaction, baselineGas);

    if (dynamicIntrinsicGasCost == Long.MIN_VALUE || dynamicIntrinsicGasCost == Long.MAX_VALUE) {
      return dynamicIntrinsicGasCost;
    }
    return clampedAdd(getMinimumTransactionCost(), dynamicIntrinsicGasCost);
  }

  /**
   * Calculates the dynamic part of the intrinsic gas cost
   *
   * @param transaction the transaction
   * @param baselineGas how much gas is used by access lists and code delegations
   * @return the dynamic part of the intrinsic gas cost
   */
  protected long dynamicIntrinsicGasCost(final Transaction transaction, final long baselineGas) {
    final int payloadSize = transaction.getPayload().size();

    long cost =
        clampedAdd(callDataCost(payloadSize, transaction.getPayloadZeroBytes()), baselineGas);

    if (cost == Long.MIN_VALUE || cost == Long.MAX_VALUE) {
      return cost;
    }

    if (transaction.isContractCreation()) {
      cost = clampedAdd(cost, contractCreationCost(payloadSize));

      if (cost == Long.MIN_VALUE || cost == Long.MAX_VALUE) {
        return cost;
      }
    }

    return cost;
  }

  /**
   * Calculates the cost of the call data
   *
   * @param payloadSize the total size of the payload
   * @param zeroBytes the number of zero bytes in the payload
   * @return the cost of the call data
   */
  protected long callDataCost(final long payloadSize, final long zeroBytes) {
    return clampedAdd(
        TX_DATA_NON_ZERO_COST * (payloadSize - zeroBytes), TX_DATA_ZERO_COST * zeroBytes);
  }

  /**
   * Returns the gas cost for contract creation transactions
   *
   * @param ignored the size of the contract creation code (ignored in Frontier)
   * @return the gas cost for contract creation transactions
   */
  protected long contractCreationCost(final int ignored) {
    return txCreateExtraGasCost();
  }

  /**
   * Returns the additional gas cost for contract creation transactions
   *
   * @return the additional gas cost for contract creation transactions
   */
  protected long txCreateExtraGasCost() {
    return TX_CREATE_EXTRA_COST;
  }

  @Override
  public long transactionFloorCost(final Transaction transaction) {
    return 0L;
  }

  @Override
  public long codeDepositGasCost(final int codeSize) {
    return CODE_DEPOSIT_BYTE_COST * codeSize;
  }

  @Override
  public long idPrecompiledContractGasCost(final Bytes input) {
    return ID_PRECOMPILED_WORD_GAS_COST * numWords(input) + ID_PRECOMPILED_BASE_GAS_COST;
  }

  @Override
  public long getEcrecPrecompiledContractGasCost() {
    return ECREC_PRECOMPILED_GAS_COST;
  }

  @Override
  public long getP256VerifyPrecompiledContractGasCost() {
    return P256VERIFY_PRECOMPILED_GAS_COST;
  }

  @Override
  public long sha256PrecompiledContractGasCost(final Bytes input) {
    return SHA256_PRECOMPILED_WORD_GAS_COST * numWords(input) + SHA256_PRECOMPILED_BASE_GAS_COST;
  }

  @Override
  public long ripemd160PrecompiledContractGasCost(final Bytes input) {
    return RIPEMD160_PRECOMPILED_WORD_GAS_COST * numWords(input)
        + RIPEMD160_PRECOMPILED_BASE_GAS_COST;
  }

  @Override
  public long getZeroTierGasCost() {
    return 0L;
  }

  @Override
  public long getVeryLowTierGasCost() {
    return VERY_LOW_TIER_GAS_COST;
  }

  @Override
  public long getLowTierGasCost() {
    return LOW_TIER_GAS_COST;
  }

  @Override
  public long getBaseTierGasCost() {
    return BASE_TIER_GAS_COST;
  }

  @Override
  public long getMidTierGasCost() {
    return MID_TIER_GAS_COST;
  }

  @Override
  public long getHighTierGasCost() {
    return HIGH_TIER_GAS_COST;
  }

  /**
   * Returns the base gas cost to execute a call operation.
   *
   * @return the base gas cost to execute a call operation
   */
  @Override
  public long callOperationBaseGasCost() {
    return CALL_OPERATION_BASE_GAS_COST;
  }

  @Override
  public long callValueTransferGasCost() {
    return CALL_VALUE_TRANSFER_GAS_COST;
  }

  @Override
  public long newAccountGasCost() {
    return NEW_ACCOUNT_GAS_COST;
  }

  @Override
  public long callOperationStaticGasCost(
      final MessageFrame frame,
      final long stipend,
      final long inputDataOffset,
      final long inputDataLength,
      final long outputDataOffset,
      final long outputDataLength,
      final Wei transferValue,
      final Address recipientAddress,
      final boolean accountIsWarm) {
    final long inputDataMemoryExpansionCost =
        memoryExpansionGasCost(frame, inputDataOffset, inputDataLength);
    final long outputDataMemoryExpansionCost =
        memoryExpansionGasCost(frame, outputDataOffset, outputDataLength);
    final long memoryExpansionCost =
        Math.max(inputDataMemoryExpansionCost, outputDataMemoryExpansionCost);

    long cost = clampedAdd(clampedAdd(callOperationBaseGasCost(), stipend), memoryExpansionCost);

    if (!transferValue.isZero()) {
      cost = clampedAdd(cost, callValueTransferGasCost());
    }

    return cost;
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
    long cost = staticCallCost;

    final Account recipient = frame.getWorldUpdater().get(recipientAddress);
    if (recipient == null) {
      cost = clampedAdd(cost, newAccountGasCost());
    }

    return cost;
  }

  /**
   * Returns the additional call stipend for calls with value transfers.
   *
   * @return the additional call stipend for calls with value transfers
   */
  @Override
  public long getAdditionalCallStipend() {
    return ADDITIONAL_CALL_STIPEND;
  }

  @Override
  public long gasAvailableForChildCall(
      final MessageFrame frame, final long stipend, final boolean transfersValue) {
    if (transfersValue) {
      return stipend + getAdditionalCallStipend();
    } else {
      return stipend;
    }
  }

  @Override
  public long getMinRetainedGas() {
    return 0;
  }

  @Override
  public long getMinCalleeGas() {
    return 0;
  }

  @Override
  public long txCreateCost() {
    return CREATE_OPERATION_GAS_COST;
  }

  @Override
  public long createKeccakCost(final int initCodeLength) {
    return clampedMultiply(KECCAK256_OPERATION_WORD_GAS_COST, numWords(initCodeLength));
  }

  @Override
  public long initcodeCost(final int initCodeLength) {
    return 0;
  }

  @Override
  public long gasAvailableForChildCreate(final long stipend) {
    return stipend;
  }

  @Override
  public long dataCopyOperationGasCost(
      final MessageFrame frame, final long offset, final long length) {
    return copyWordsToMemoryGasCost(
        frame, VERY_LOW_TIER_GAS_COST, COPY_WORD_GAS_COST, offset, length);
  }

  @Override
  public long memoryExpansionGasCost(
      final MessageFrame frame, final long offset, final long length) {

    final long pre = memoryCost(frame.memoryWordSize());
    final long post = memoryCost(frame.calculateMemoryExpansion(offset, length));
    if (post == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return post - pre;
  }

  @Override
  public long getBalanceOperationGasCost() {
    return BALANCE_OPERATION_GAS_COST;
  }

  @Override
  public long getBlockHashOperationGasCost() {
    return BLOCKHASH_OPERATION_GAS_COST;
  }

  /**
   * Returns the gas cost for a byte in the {@link ExpOperation}.
   *
   * @return the gas cost for a byte in the exponent operation
   */
  protected long expOperationByteGasCost() {
    return EXP_OPERATION_BYTE_GAS_COST;
  }

  @Override
  public long expOperationGasCost(final int numBytes) {
    return expOperationByteGasCost() * numBytes + EXP_OPERATION_BASE_GAS_COST;
  }

  /**
   * Returns the base gas cost for external code accesses.
   *
   * @return the base gas cost for external code accesses
   */
  protected long extCodeBaseGasCost() {
    return EXT_CODE_BASE_GAS_COST;
  }

  @Override
  public long extCodeCopyOperationGasCost(
      final MessageFrame frame, final long offset, final long length) {
    return copyWordsToMemoryGasCost(
        frame, extCodeBaseGasCost(), COPY_WORD_GAS_COST, offset, length);
  }

  @Override
  public long extCodeHashOperationGasCost() {
    throw new UnsupportedOperationException(
        "EXTCODEHASH not supported by " + getClass().getSimpleName());
  }

  @Override
  public long getExtCodeSizeOperationGasCost() {
    return extCodeBaseGasCost();
  }

  @Override
  public long getJumpDestOperationGasCost() {
    return JUMPDEST_OPERATION_GAS_COST;
  }

  @Override
  public long logOperationGasCost(
      final MessageFrame frame, final long dataOffset, final long dataLength, final int numTopics) {
    return clampedAdd(
        LOG_OPERATION_BASE_GAS_COST,
        clampedAdd(
            clampedMultiply(LOG_OPERATION_DATA_BYTE_GAS_COST, dataLength),
            clampedAdd(
                clampedMultiply(LOG_OPERATION_TOPIC_GAS_COST, numTopics),
                memoryExpansionGasCost(frame, dataOffset, dataLength))));
  }

  @Override
  public long mLoadOperationGasCost(final MessageFrame frame, final long offset) {
    return clampedAdd(VERY_LOW_TIER_GAS_COST, memoryExpansionGasCost(frame, offset, 32));
  }

  @Override
  public long mStoreOperationGasCost(final MessageFrame frame, final long offset) {
    return clampedAdd(VERY_LOW_TIER_GAS_COST, memoryExpansionGasCost(frame, offset, 32));
  }

  @Override
  public long mStore8OperationGasCost(final MessageFrame frame, final long offset) {
    return clampedAdd(VERY_LOW_TIER_GAS_COST, memoryExpansionGasCost(frame, offset, 1));
  }

  @Override
  public long keccak256OperationGasCost(
      final MessageFrame frame, final long offset, final long length) {
    return copyWordsToMemoryGasCost(
        frame,
        KECCAK256_OPERATION_BASE_GAS_COST,
        KECCAK256_OPERATION_WORD_GAS_COST,
        offset,
        length);
  }

  @Override
  public long getSloadOperationGasCost() {
    return SLOAD_OPERATION_GAS_COST;
  }

  @Override
  public long calculateStorageCost(
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    return !newValue.isZero() && currentValue.get().isZero()
        ? STORAGE_SET_GAS_COST
        : STORAGE_RESET_GAS_COST;
  }

  @Override
  public long calculateStorageRefundAmount(
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    return newValue.isZero() && !currentValue.get().isZero() ? STORAGE_RESET_REFUND_AMOUNT : 0L;
  }

  @Override
  public long getSelfDestructRefundAmount() {
    return SELF_DESTRUCT_REFUND_AMOUNT;
  }

  /**
   * Copy words to memory gas cost.
   *
   * @param frame the frame
   * @param baseGasCost the base gas cost
   * @param wordGasCost the word gas cost
   * @param offset the offset
   * @param length the length
   * @return the cost
   */
  protected long copyWordsToMemoryGasCost(
      final MessageFrame frame,
      final long baseGasCost,
      final long wordGasCost,
      final long offset,
      final long length) {
    final long copyCost =
        clampedAdd(clampedMultiply(wordGasCost, numWords(clampedToInt(length))), baseGasCost);
    final long memoryCost = memoryExpansionGasCost(frame, offset, length);

    return clampedAdd(copyCost, memoryCost);
  }

  /**
   * Memory cost.
   *
   * @param length the length
   * @return the cost
   */
  static long memoryCost(final long length) {
    final long lengthSquare = clampedMultiply(length, length);
    final long base =
        (lengthSquare == Long.MAX_VALUE)
            ? clampedMultiply(length / 512, length)
            : lengthSquare / 512;

    return clampedAdd(clampedMultiply(MEMORY_WORD_GAS_COST, length), base);
  }

  @Override
  public long getMinimumTransactionCost() {
    return TX_BASE_COST;
  }

  @Override
  public long calculateGasRefund(
      final Transaction transaction,
      final MessageFrame initialFrame,
      final long ignoredCodeDelegationRefund) {

    final long refundAllowance = calculateRefundAllowance(transaction, initialFrame);

    return initialFrame.getRemainingGas() + refundAllowance;
  }

  private long calculateRefundAllowance(
      final Transaction transaction, final MessageFrame initialFrame) {
    final long selfDestructRefund =
        getSelfDestructRefundAmount() * initialFrame.getSelfDestructs().size();
    final long executionRefund = initialFrame.getGasRefund() + selfDestructRefund;
    // Integer truncation takes care of the floor calculation needed after the divide.
    final long maxRefundAllowance =
        (transaction.getGasLimit() - initialFrame.getRemainingGas()) / getMaxRefundQuotient();
    return Math.min(executionRefund, maxRefundAllowance);
  }
}
