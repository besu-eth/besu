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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.evm.account.Account.MAX_NONCE;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates a transaction based on Frontier protocol runtime requirements.
 *
 * <p>The {@link MainnetTransactionValidator} performs the intrinsic gas cost check on the given
 * {@link Transaction}.
 */
public class MainnetTransactionValidator implements TransactionValidator {
  private static final Logger LOG = LoggerFactory.getLogger(MainnetTransactionValidator.class);

  public static final BigInteger TWO_POW_256 = BigInteger.TWO.pow(256);

  private final GasCalculator gasCalculator;
  private final GasLimitCalculator gasLimitCalculator;
  private final FeeMarket feeMarket;

  private final boolean disallowSignatureMalleability;

  private final Optional<BigInteger> chainId;

  private final Set<TransactionType> acceptedTransactionTypes;

  private final int maxInitcodeSize;
  private final MainnetBlobsValidator blobsValidator;

  public MainnetTransactionValidator(
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final FeeMarket feeMarket,
      final boolean checkSignatureMalleability,
      final Optional<BigInteger> chainId,
      final Set<TransactionType> acceptedTransactionTypes,
      final Set<BlobType> acceptedBlobVersions,
      final int maxInitcodeSize) {
    this.gasCalculator = gasCalculator;
    this.gasLimitCalculator = gasLimitCalculator;
    this.feeMarket = feeMarket;
    this.disallowSignatureMalleability = checkSignatureMalleability;
    this.chainId = chainId;
    this.acceptedTransactionTypes = acceptedTransactionTypes;
    this.maxInitcodeSize = maxInitcodeSize;
    this.blobsValidator =
        new MainnetBlobsValidator(acceptedBlobVersions, gasLimitCalculator, gasCalculator);
  }

  @Override
  public ValidationResult<TransactionInvalidReason> validate(
      final Transaction transaction,
      final Optional<Wei> baseFee,
      final Optional<Wei> blobFee,
      final TransactionValidationParams transactionValidationParams) {
    final ValidationResult<TransactionInvalidReason> signatureResult =
        validateTransactionSignature(transaction);
    if (!signatureResult.isValid()) {
      return signatureResult;
    }

    final TransactionType transactionType = transaction.getType();
    if (!acceptedTransactionTypes.contains(transactionType)) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
          String.format(
              "Transaction type %s is invalid, accepted transaction types are %s",
              transactionType, acceptedTransactionTypes));
    }

    if (!transactionValidationParams.isAllowFutureNonce() && transaction.getNonce() == MAX_NONCE) {
      return ValidationResult.invalid(
          TransactionInvalidReason.NONCE_OVERFLOW, "Nonce must be less than 2^64-1");
    }

    if (!transactionValidationParams.isAllowExceedingGasLimit()
        && transaction.getGasLimit() > gasLimitCalculator.transactionGasLimitCap()) {
      return ValidationResult.invalid(
          TransactionInvalidReason.EXCEEDS_TRANSACTION_GAS_LIMIT,
          "Transaction gas limit must be at most " + gasLimitCalculator.transactionGasLimitCap());
    }

    if (transactionType.supportsBlob()) {
      final ValidationResult<TransactionInvalidReason> blobTransactionResult =
          blobsValidator.validate(transaction);
      if (!blobTransactionResult.isValid()) {
        LOG.debug(
            "Blob transaction {} validation failed: {}",
            transaction.getHash().getBytes().toHexString(),
            blobTransactionResult.getErrorMessage());
        return blobTransactionResult;
      }
    }

    if (transaction.isContractCreation() && transaction.getPayload().size() > maxInitcodeSize) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INITCODE_TOO_LARGE,
          String.format(
              "Initcode size of %d exceeds maximum size of %s",
              transaction.getPayload().size(), maxInitcodeSize));
    }

    if (transactionType == TransactionType.DELEGATE_CODE) {
      ValidationResult<TransactionInvalidReason> codeDelegationValidation =
          validateCodeDelegation(transaction);
      if (!codeDelegationValidation.isValid()) {
        return codeDelegationValidation;
      }
    }

    return validateCostAndFee(transaction, baseFee, blobFee, transactionValidationParams);
  }

  private static ValidationResult<TransactionInvalidReason> validateCodeDelegation(
      final Transaction transaction) {
    if (isDelegateCodeEmpty(transaction)) {
      return ValidationResult.invalid(
          TransactionInvalidReason.EMPTY_CODE_DELEGATION,
          "transaction code delegation transactions must have a non-empty code delegation list");
    }

    if (transaction.getTo().isEmpty()) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
          "transaction code delegation transactions must have a to address");
    }

    final Optional<ValidationResult<TransactionInvalidReason>> validationResult =
        transaction
            .getCodeDelegationList()
            .map(
                codeDelegations -> {
                  for (CodeDelegation codeDelegation : codeDelegations) {
                    if (codeDelegation.chainId().compareTo(TWO_POW_256) >= 0) {
                      throw new IllegalArgumentException(
                          "Invalid 'chainId' value, should be < 2^256 but got "
                              + codeDelegation.chainId());
                    }

                    if (codeDelegation.r().compareTo(TWO_POW_256) >= 0) {
                      throw new IllegalArgumentException(
                          "Invalid 'r' value, should be < 2^256 but got " + codeDelegation.r());
                    }

                    if (codeDelegation.s().compareTo(TWO_POW_256) >= 0) {
                      throw new IllegalArgumentException(
                          "Invalid 's' value, should be < 2^256 but got " + codeDelegation.s());
                    }
                  }

                  return ValidationResult.valid();
                });

    if (validationResult.isPresent() && !validationResult.get().isValid()) {
      return validationResult.get();
    }

    return ValidationResult.valid();
  }

  private static boolean isDelegateCodeEmpty(final Transaction transaction) {
    return transaction.getCodeDelegationList().isEmpty()
        || transaction.getCodeDelegationList().get().isEmpty();
  }

  private ValidationResult<TransactionInvalidReason> validateCostAndFee(
      final Transaction transaction,
      final Optional<Wei> maybeBaseFee,
      final Optional<Wei> maybeBlobFee,
      final TransactionValidationParams transactionValidationParams) {

    if (maybeBaseFee.isPresent()) {
      final Wei price = feeMarket.getTransactionPriceCalculator().price(transaction, maybeBaseFee);
      if (!transactionValidationParams.allowUnderpriced()
          && !transactionValidationParams.isPreserveCallerGasPricing()
          && price.compareTo(maybeBaseFee.orElseThrow()) < 0) {
        return ValidationResult.invalid(
            TransactionInvalidReason.GAS_PRICE_BELOW_CURRENT_BASE_FEE,
            "gasPrice is less than the current BaseFee");
      }

      // assert transaction.max_fee_per_gas >= transaction.max_priority_fee_per_gas
      if (transaction.getType().supports1559FeeMarket()
          && transaction
                  .getMaxPriorityFeePerGas()
                  .get()
                  .getAsBigInteger()
                  .compareTo(transaction.getMaxFeePerGas().get().getAsBigInteger())
              > 0) {
        return ValidationResult.invalid(
            TransactionInvalidReason.MAX_PRIORITY_FEE_PER_GAS_EXCEEDS_MAX_FEE_PER_GAS,
            "max priority fee per gas cannot be greater than max fee per gas");
      }
    }

    if (transaction.getType().supportsBlob()) {
      final long txTotalBlobGas = gasCalculator.blobGasCost(transaction.getBlobCount());
      if (txTotalBlobGas > gasLimitCalculator.currentBlobGasLimit()) {
        return ValidationResult.invalid(
            TransactionInvalidReason.TOTAL_BLOB_GAS_TOO_HIGH,
            String.format(
                "total blob gas %d exceeds max blob gas per block %d",
                txTotalBlobGas, gasLimitCalculator.currentBlobGasLimit()));
      }
      if (maybeBlobFee.isEmpty()) {
        throw new IllegalArgumentException(
            "blob fee must be provided from blocks containing blobs");
        // tx.getMaxFeePerBlobGas can be empty for eth_call
      } else if (!transactionValidationParams.allowUnderpriced()
          && maybeBlobFee.get().compareTo(transaction.getMaxFeePerBlobGas().get()) > 0) {
        return ValidationResult.invalid(
            TransactionInvalidReason.BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE,
            String.format(
                "tx max fee per blob gas less than block blob gas fee: address %s blobGasFeeCap: %s, blobBaseFee: %s",
                transaction.getSender().getBytes().toHexString(),
                transaction.getMaxFeePerBlobGas().get().toHumanReadableString(),
                maybeBlobFee.get().toHumanReadableString()));
      }
    }

    final long baselineGas =
        clampedAdd(
            transaction.getAccessList().map(gasCalculator::accessListGasCost).orElse(0L),
            gasCalculator.delegateCodeGasCost(transaction.codeDelegationListSize()));
    final long intrinsicGasCostOrFloor =
        Math.max(
            gasCalculator.transactionIntrinsicGasCost(transaction, baselineGas),
            gasCalculator.transactionFloorCost(
                transaction.getPayload(), transaction.getPayloadZeroBytes()));

    if (Long.compareUnsigned(intrinsicGasCostOrFloor, transaction.getGasLimit()) > 0) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT,
          String.format(
              "intrinsic gas cost %s exceeds gas limit %s",
              intrinsicGasCostOrFloor, transaction.getGasLimit()));
    }

    if (transaction.calculateUpfrontGasCost(transaction.getMaxGasPrice(), Wei.ZERO, 0).bitLength()
        > 256) {
      return ValidationResult.invalid(
          TransactionInvalidReason.UPFRONT_COST_EXCEEDS_UINT256,
          "Upfront gas cost cannot exceed 2^256 Wei");
    }

    return ValidationResult.valid();
  }

  @Override
  public ValidationResult<TransactionInvalidReason> validateForSender(
      final Transaction transaction,
      final Account sender,
      final TransactionValidationParams validationParams) {
    Wei senderBalance = Account.DEFAULT_BALANCE;
    long senderNonce = Account.DEFAULT_NONCE;
    Hash codeHash = Hash.EMPTY;

    if (sender != null) {
      senderBalance = sender.getBalance();
      senderNonce = sender.getNonce();
      if (sender.getCodeHash() != null) codeHash = sender.getCodeHash();
    }

    final Wei upfrontCost =
        transaction.getUpfrontCost(gasCalculator.blobGasCost(transaction.getBlobCount()));
    if (!validationParams.allowUnderpriced() && upfrontCost.compareTo(senderBalance) > 0) {
      return ValidationResult.invalid(
          TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE,
          String.format(
              "transaction up-front cost %s exceeds transaction sender account balance %s for sender %s",
              upfrontCost.toQuantityHexString(),
              senderBalance.toQuantityHexString(),
              transaction.getSender()));
    }

    if (Long.compareUnsigned(transaction.getNonce(), senderNonce) < 0) {
      return ValidationResult.invalid(
          TransactionInvalidReason.NONCE_TOO_LOW,
          String.format(
              "transaction nonce %s below sender account nonce %s for sender %s",
              transaction.getNonce(), senderNonce, transaction.getSender()));
    }

    if (!validationParams.isAllowFutureNonce() && senderNonce != transaction.getNonce()) {
      return ValidationResult.invalid(
          TransactionInvalidReason.NONCE_TOO_HIGH,
          String.format(
              "transaction nonce %s does not match sender account nonce %s for sender %s",
              transaction.getNonce(), senderNonce, transaction.getSender()));
    }

    if (!validationParams.isAllowContractAddressAsSender()
        && !canSendTransaction(sender, codeHash)) {
      return ValidationResult.invalid(
          TransactionInvalidReason.TX_SENDER_NOT_AUTHORIZED,
          String.format(
              "Sender %s has deployed code and so is not authorized to send transactions",
              transaction.getSender()));
    }

    return ValidationResult.valid();
  }

  private static boolean canSendTransaction(final Account sender, final Hash codeHash) {
    return codeHash.equals(Hash.EMPTY) || hasCodeDelegation(sender.getCode());
  }

  private ValidationResult<TransactionInvalidReason> validateTransactionSignature(
      final Transaction transaction) {
    if (chainId.isPresent()
        && (transaction.getChainId().isPresent() && !transaction.getChainId().equals(chainId))) {
      return ValidationResult.invalid(
          TransactionInvalidReason.WRONG_CHAIN_ID,
          String.format(
              "transaction was meant for chain id %s and not this chain id %s",
              transaction.getChainId().get(), chainId.get()));
    }

    if (chainId.isEmpty() && transaction.getChainId().isPresent()) {
      return ValidationResult.invalid(
          TransactionInvalidReason.REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED,
          "replay protected signatures is not supported");
    }

    final SECPSignature signature = transaction.getSignature();
    final BigInteger halfCurveOrder = SignatureAlgorithmFactory.getInstance().getHalfCurveOrder();
    if (disallowSignatureMalleability && signature.getS().compareTo(halfCurveOrder) > 0) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_SIGNATURE,
          String.format(
              "Signature s value should be less than %s, but got %s",
              halfCurveOrder, signature.getS()));
    }

    // org.bouncycastle.math.ec.ECCurve.AbstractFp.decompressPoint throws an
    // IllegalArgumentException for "Invalid point compression" for bad signatures.
    try {
      transaction.getSender();
    } catch (final IllegalArgumentException e) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_SIGNATURE,
          "sender could not be extracted from transaction signature");
    }
    return ValidationResult.valid();
  }
}
