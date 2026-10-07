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

    final long txGasLimitCap = gasLimitCalculator.transactionGasLimitCap();
    if (!transactionValidationParams.isAllowExceedingGasLimit()
        // Long.MAX_VALUE is the sentinel for "no cap" (pre-Osaka). Only apply unsigned
        // comparison when a real cap is in effect; unsigned would incorrectly reject
        // transactions with gas >= 2^63 on pre-Osaka forks where no cap applies.
        && txGasLimitCap != Long.MAX_VALUE
        && Long.compareUnsigned(transaction.getGasLimit(), txGasLimitCap) > 0) {
      return ValidationResult.invalid(
          TransactionInvalidReason.EXCEEDS_TRANSACTION_GAS_LIMIT,
          "Transaction gas limit must be at most " + txGasLimitCap);
    }

    // Frame transactions are not required to carry blobs; blob rules apply only when they do.
    if (transactionType.supportsBlob()
        && (transactionType == TransactionType.BLOB || transaction.getBlobCount() > 0)) {
      final ValidationResult<TransactionInvalidReason> blobTransactionResult =
          blobsValidator.validate(transaction);
      if (!blobTransactionResult.isValid()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Blob transaction {} validation failed: {}",
              transaction.getHash().getBytes().toHexString(),
              blobTransactionResult.getErrorMessage());
        }
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

    if (transactionType.supportsFrames()) {
      final ValidationResult<TransactionInvalidReason> frameValidation =
          validateFrameFields(transaction);
      if (!frameValidation.isValid()) {
        return frameValidation;
      }
    }

    return validateCostAndFee(transaction, baseFee, blobFee, transactionValidationParams);
  }

  /** The EIP-8141 expiry verifier contract address. */
  public static final org.hyperledger.besu.datatypes.Address EXPIRY_VERIFIER =
      org.hyperledger.besu.datatypes.Address.fromHexString("0x8141");

  private static final int MAX_FRAMES = 64;

  /** EIP-8141 static validity constraints on the frames and signature entries. */
  private static ValidationResult<TransactionInvalidReason> validateFrameFields(
      final Transaction transaction) {
    final var frames = transaction.getFrames().orElseThrow();
    if (frames.isEmpty() || frames.size() > MAX_FRAMES) {
      return frameInvalid("frame count must be between 1 and " + MAX_FRAMES);
    }

    if (transaction.getBlobCount() == 0
        && !transaction.getMaxFeePerBlobGas().orElse(Wei.ZERO).isZero()) {
      return frameInvalid("max fee per blob gas must be zero without blob versioned hashes");
    }

    for (final var signature : transaction.getFrameSignatures().orElse(java.util.List.of())) {
      if (signature.scheme() < 0
          || signature.scheme() > org.hyperledger.besu.ethereum.core.FrameSignature.SCHEME_P256) {
        return frameInvalid("unknown signature scheme " + signature.scheme());
      }
      if (signature.scheme() == org.hyperledger.besu.ethereum.core.FrameSignature.SCHEME_ARBITRARY
          && !signature.signer().isEmpty()) {
        return frameInvalid("ARBITRARY signature entries must not declare a signer");
      }
      if (signature.msg().size() == 32 && signature.msg().isZero()) {
        return frameInvalid("explicit zero signature digest is invalid");
      }
    }

    long totalFrameGas = 0L;
    int expiryFrames = 0;
    for (int i = 0; i < frames.size(); i++) {
      final var frame = frames.get(i);
      if (frame.mode() < 0 || frame.mode() > org.hyperledger.besu.ethereum.core.Frame.MODE_SENDER) {
        return frameInvalid("unknown frame mode " + frame.mode());
      }
      if (frame.flags() < 0 || frame.flags() > 7) {
        return frameInvalid("reserved frame flag bits set: " + frame.flags());
      }
      // The EIP permits a single limit up to 2^64-1, but Besu holds frame gas in a signed long and
      // the execution path subtracts and compares it as one -- MessageFrame.initialGas, the state
      // gas reservoir, and the used-gas arithmetic in FrameTransactionProcessor all assume a
      // non-negative value. A limit at or above 2^63 is therefore rejected as unexecutable rather
      // than accepted and mishandled.
      if (frame.executionGasLimit() < 0 || frame.stateGasLimit() < 0) {
        return frameInvalid("frame gas limits exceed 2^63-1");
      }
      // The running total is only bounded by the EIP's 2^64, which is above what a signed long can
      // express, so it is accumulated and compared as unsigned. Each addend is at most 2^63-1 by
      // the check above, so a single frame's pair cannot wrap; only the running total can, which
      // shows up as a sum that is unsigned-smaller than what was just added to it.
      final long frameGas = frame.executionGasLimit() + frame.stateGasLimit();
      totalFrameGas += frameGas;
      if (Long.compareUnsigned(totalFrameGas, frameGas) < 0) {
        return frameInvalid("total frame gas exceeds 2^64-1");
      }
      if (frame.mode() != org.hyperledger.besu.ethereum.core.Frame.MODE_SENDER
          && !frame.value().isZero()) {
        return frameInvalid("frame " + i + " carries value outside SENDER mode");
      }
      if ((frame.flags() & org.hyperledger.besu.ethereum.core.Frame.FLAG_APPROVE_EXECUTION) != 0
          && frame.target().isPresent()
          && !frame.target().get().getBytes().equals(transaction.getSender().getBytes())) {
        return frameInvalid(
            "frame " + i + " allows execution approval with a target other than the sender");
      }
      if (frame.isAtomicBatch()) {
        if (frame.mode() == org.hyperledger.besu.ethereum.core.Frame.MODE_VERIFY) {
          return frameInvalid("frame " + i + " is a batched VERIFY frame");
        }
        if (i + 1 >= frames.size()) {
          return frameInvalid("last frame cannot carry the atomic batch flag");
        }
        if (frames.get(i + 1).mode() == org.hyperledger.besu.ethereum.core.Frame.MODE_VERIFY) {
          return frameInvalid("frame " + (i + 1) + " is a batched VERIFY frame");
        }
      }
      final boolean inBatch = frame.isAtomicBatch() || (i > 0 && frames.get(i - 1).isAtomicBatch());
      if (inBatch && frame.allowedApprovalScope() != 0) {
        return frameInvalid("batched frame " + i + " must not carry an approval scope");
      }
      // Expiry verifier frames.
      if (frame.mode() == org.hyperledger.besu.ethereum.core.Frame.MODE_VERIFY
          && frame.target().isPresent()
          && frame.target().get().getBytes().equals(EXPIRY_VERIFIER.getBytes())) {
        expiryFrames++;
        if (expiryFrames > 1) {
          return frameInvalid("at most one expiry verifier frame is allowed");
        }
        if (frame.flags() != 0
            || !frame.value().isZero()
            || frame.stateGasLimit() != 0
            || frame.data().size() != 8) {
          return frameInvalid("malformed expiry verifier frame " + i);
        }
      }
    }
    return ValidationResult.valid();
  }

  private static ValidationResult<TransactionInvalidReason> frameInvalid(final String message) {
    return ValidationResult.invalid(TransactionInvalidReason.INVALID_FRAMES, message);
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

    for (CodeDelegation codeDelegation : transaction.getCodeDelegationList().orElseThrow()) {
      if (codeDelegation.chainId().compareTo(TWO_POW_256) >= 0) {
        return ValidationResult.invalid(
            TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
            "Invalid 'chainId' value, should be < 2^256 but got " + codeDelegation.chainId());
      }

      if (codeDelegation.r().compareTo(TWO_POW_256) >= 0) {
        return ValidationResult.invalid(
            TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
            "Invalid 'r' value, should be < 2^256 but got " + codeDelegation.r());
      }

      if (codeDelegation.s().compareTo(TWO_POW_256) >= 0) {
        return ValidationResult.invalid(
            TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
            "Invalid 's' value, should be < 2^256 but got " + codeDelegation.s());
      }
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
      if (!transactionValidationParams.allowUnderpricedGas()
          && !transactionValidationParams.isAllowExceedingBalance()
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

    // EIP-8141: frame transactions are only blob-carrying when they declare versioned hashes;
    // the blob fee rules apply only in that case.
    if (transaction.getType().supportsBlob() && transaction.getBlobCount() > 0) {
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
      } else if (!transactionValidationParams.allowUnderpricedGas()
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

    final long intrinsicGasCostOrFloor;
    if (transaction.getType().supportsFrames()) {
      // EIP-8141: the EIP-7825 cap binds max(intrinsic + total frame execution gas, calldata
      // floor); the declared state gas is exempt.
      final var frames = transaction.getFrames().orElseThrow();
      final var signatures = transaction.getFrameSignatures().orElse(java.util.List.of());
      intrinsicGasCostOrFloor =
          Math.max(
              clampedAdd(
                  org.hyperledger.besu.ethereum.core.FrameTransactionGas.intrinsicGas(
                      frames, signatures, transaction.getSender()),
                  org.hyperledger.besu.ethereum.core.FrameTransactionGas.totalExecutionGasLimit(
                      frames)),
              org.hyperledger.besu.ethereum.core.FrameTransactionGas.calldataFloorGas(
                  frames, signatures, transaction.getSender()));
    } else {
      final long baselineGas =
          clampedAdd(
              transaction.getAccessList().map(gasCalculator::accessListGasCost).orElse(0L),
              gasCalculator.delegateCodeGasCost(transaction.codeDelegationListSize()));
      intrinsicGasCostOrFloor =
          Math.max(
              gasCalculator.transactionIntrinsicGasCost(transaction, baselineGas),
              gasCalculator.transactionFloorCost(transaction));
    }

    // EIP-8037: cap max(intrinsic_execution, calldata_floor) rather than tx.gas itself.
    final long intrinsicGasLimitCap = gasLimitCalculator.transactionIntrinsicGasLimitCap();
    if (!transactionValidationParams.isAllowExceedingGasLimit()
        && Long.compareUnsigned(intrinsicGasCostOrFloor, intrinsicGasLimitCap) > 0) {
      // Exceeding the protocol cap is a different failure from exceeding the transaction's own
      // gas limit, checked immediately below. Report it the way the plain gas limit cap above
      // does, so the two stay distinguishable rather than sharing one message.
      return ValidationResult.invalid(
          TransactionInvalidReason.EXCEEDS_TRANSACTION_GAS_LIMIT,
          "Transaction gas limit must be at most " + intrinsicGasLimitCap);
    }

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

    // EIP-8141: the payer (chosen by APPROVE during execution) covers the fees, not necessarily
    // the sender, so no sender balance requirement applies.
    final Wei maxUpfrontGasCost =
        transaction.getType().supportsFrames()
            ? Wei.ZERO
            : transaction.getMaxUpfrontGasCost(
                gasCalculator.blobGasCost(transaction.getBlobCount()));
    if (!validationParams.allowUnderpricedGas() && maxUpfrontGasCost.compareTo(senderBalance) > 0) {
      return ValidationResult.invalid(
          TransactionInvalidReason.UPFRONT_GAS_COST_EXCEEDS_BALANCE,
          String.format(
              "transaction up-front gas cost %s exceeds transaction sender account balance %s for sender %s",
              maxUpfrontGasCost.toQuantityHexString(),
              senderBalance.toQuantityHexString(),
              transaction.getSender()));
    }

    // then check if the sender has enough balance to pay to the value transfer if present
    if (!transaction.getValue().isZero()) {
      final Wei actualCompareBalance =
          validationParams.allowUnderpricedGas()
              ? senderBalance // ignore gas cost when underpriced gas is allowed
              : senderBalance.subtract(maxUpfrontGasCost);
      if (transaction.getValue().compareTo(actualCompareBalance) > 0) {
        return ValidationResult.invalid(
            TransactionInvalidReason.INSUFFICIENT_FUNDS_FOR_TRANSFER,
            String.format(
                "transfer value %s exceeds transaction sender account balance %s for sender %s",
                transaction.getValue().toQuantityHexString(),
                senderBalance.toQuantityHexString(),
                transaction.getSender()));
      }
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

    // EIP-8141 explicitly lifts the EIP-3607 restriction for frame transactions.
    if (!transaction.getType().supportsFrames()
        && !validationParams.isAllowContractAddressAsSender()
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

    // EIP-8141: frame transactions declare their sender and have no outer signature; the
    // signature entries are validated by the frame transaction processor.
    if (transaction.getType().supportsFrames()) {
      return ValidationResult.valid();
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
