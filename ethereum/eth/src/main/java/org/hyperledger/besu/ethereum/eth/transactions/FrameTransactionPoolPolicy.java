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
package org.hyperledger.besu.ethereum.eth.transactions;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.FrameTransactionGas;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.KeyedNonces;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;

/**
 * The EIP-8141 public-mempool policy for frame transactions, limited to the transactions whose
 * validation prefix a node can evaluate directly, without running the EVM (EIP-8141, "Direct
 * Evaluation of Protocol-Defined Frames").
 *
 * <p>The admitted class is {@code [expiry_verify?, self_verify, (SENDER|DEFAULT|ASSERT)*]}: a
 * sender with the empty code hash, so its {@code self_verify} frame runs the protocol default code;
 * one secp256k1 signature by the sender over the canonical signature hash, which that default code
 * checks; a legacy nonce equal to the sender's account nonce; and no blobs. An optional first
 * expiry verifier frame calls the canonical code at {@link
 * MainnetTransactionValidator#EXPIRY_VERIFIER}, whose deadline must be at least {@link
 * #EXPIRY_ADMISSION_MARGIN_SECONDS} after the chain head. The prefix then depends only on
 * transaction fields, the sender's code hash, nonce and balance, the expiry verifier's code hash
 * and the block timestamp. Frames after it run once payment is approved and are outside mempool
 * validation. Every other shape is rejected with {@link
 * TransactionInvalidReason#FRAME_MEMPOOL_POLICY_VIOLATION}.
 *
 * <p>Direct evaluation applies the same limits as simulation (EIP-8141, structural rule 6): the
 * validation prefix's execution limits, an expiry verifier frame's included, plus the cost of
 * validating the signature must not exceed {@link #MAX_VERIFY_GAS}, and its state limits must not
 * exceed {@link #MAX_VERIFY_STATE_GAS}.
 *
 * <p>A pool that admits expiry verifier frames must drop a pending transaction once its deadline is
 * below the chain head's timestamp (EIP-8141, "Expiry Verifier Frame"); see {@link
 * #expiryDeadline}.
 */
public final class FrameTransactionPoolPolicy {

  /**
   * EIP-8141 {@code MAX_VERIFY_GAS}: gas for signature validation plus the validation prefix's
   * execution limits.
   */
  static final long MAX_VERIFY_GAS = 300_000L;

  /**
   * How far past the chain head's timestamp an expiry deadline must be for admission: one 12-second
   * slot to reach the next block's timestamp, plus one more slot so that a transaction which misses
   * that block, because it arrived after the payload was built or the slot was missed, can still be
   * included in the block after it. A shorter deadline would mostly propagate a transaction that
   * expires before any builder sees it.
   */
  static final long EXPIRY_ADMISSION_MARGIN_SECONDS = 24L;

  // EIP-8141 EXPIRY_DATA_LENGTH: the frame data is an 8-byte big-endian unsigned timestamp.
  private static final int EXPIRY_DATA_LENGTH = 8;

  // The canonical expiry verifier runtime code from EIP-8141, "Expiry Verifier Frame".
  private static final Hash EXPIRY_VERIFIER_CODE_HASH =
      Hash.hash(Bytes.fromHexString("0x60083614600a575f5ffd5b5f3560c01c4211601657005b5f5ffd"));

  // Gas the canonical expiry verifier code uses on its success path.
  private static final long EXPIRY_VERIFIER_EXECUTION_GAS = 51L;

  /** EIP-8141 {@code MAX_VERIFY_STATE_GAS}: state gas the validation prefix may budget. */
  static final long MAX_VERIFY_STATE_GAS = 500_000L;

  private FrameTransactionPoolPolicy() {}

  /**
   * Checks the parts of the policy that read no state and verify no signature, so it can run before
   * the transaction validator verifies the signature entries.
   *
   * @param transaction the frame transaction
   * @param implementation the configured transaction pool implementation
   * @param gasCalculator the gas calculator of the chain head's protocol spec
   * @param headTimestamp the chain head's timestamp, which an expiry deadline must exceed by {@link
   *     #EXPIRY_ADMISSION_MARGIN_SECONDS}
   * @return valid if the transaction has the admitted shape
   */
  static ValidationResult<TransactionInvalidReason> checkStatic(
      final Transaction transaction,
      final TransactionPoolConfiguration.Implementation implementation,
      final GasCalculator gasCalculator,
      final long headTimestamp) {
    checkArgument(transaction.getType().supportsFrames(), "not a frame transaction");
    if (implementation != TransactionPoolConfiguration.Implementation.LAYERED) {
      return violation("frame transactions need the layered transaction pool");
    }
    if (transaction.getBlobCount() > 0) {
      return violation("blob-carrying frame transactions are not admitted");
    }
    if (!KeyedNonces.isLegacy(transaction.getNonceKeys().orElse(KeyedNonces.LEGACY_NONCE_KEYS))) {
      return violation("keyed-nonce frame transactions are not admitted");
    }

    final Address sender = transaction.getSender();
    final List<FrameSignature> signatures = transaction.getFrameSignatures().orElse(List.of());
    if (signatures.size() != 1) {
      return violation(
          "a frame transaction needs exactly one signature entry, found " + signatures.size());
    }
    final FrameSignature signature = signatures.getFirst();
    if (signature.scheme() != FrameSignature.SCHEME_SECP256K1
        || !signature.signsCanonicalHash()
        || !signature.resolvedSigner(sender).equals(sender)) {
      return violation(
          "the signature entry must be a secp256k1 signature by the sender over the canonical"
              + " signature hash");
    }

    final List<Frame> frames = transaction.getFrames().orElseThrow();
    final boolean hasExpiryFrame = hasExpiryFrame(frames);
    if (hasExpiryFrame) {
      final ValidationResult<TransactionInvalidReason> expiryResult =
          checkExpiryFrame(frames.getFirst(), gasCalculator, headTimestamp);
      if (!expiryResult.isValid()) {
        return expiryResult;
      }
    }
    final int selfVerifyIndex = hasExpiryFrame ? 1 : 0;
    if (frames.size() <= selfVerifyIndex || !isSelfVerify(frames.get(selfVerifyIndex), sender)) {
      return violation(
          "the first frame, after an optional expiry verifier frame, must be a self_verify frame"
              + " targeting the sender");
    }
    final Frame selfVerify = frames.get(selfVerifyIndex);
    // The frame entry charges the warm access to the sender, so a smaller limit halts the frame
    // and invalidates the transaction.
    final long minExecutionGas = gasCalculator.getWarmStorageReadCost();
    if (Long.compareUnsigned(selfVerify.executionGasLimit(), minExecutionGas) < 0) {
      return violation("the self_verify execution limit is below " + minExecutionGas);
    }
    // EIP-8141: signature validation counts against MAX_VERIFY_GAS. Each unsigned limit is
    // compared with the gas that remains before it is subtracted, so the subtraction cannot wrap.
    long remainingVerifyGas = MAX_VERIFY_GAS - FrameTransactionGas.SECP256K1_SIGNATURE_GAS;
    for (int i = 0; i <= selfVerifyIndex; i++) {
      final long executionGas = frames.get(i).executionGasLimit();
      if (Long.compareUnsigned(executionGas, remainingVerifyGas) > 0) {
        return violation(
            "the validation prefix's execution limits plus signature validation exceed "
                + MAX_VERIFY_GAS);
      }
      remainingVerifyGas -= executionGas;
    }
    // An expiry verifier frame has a state limit of 0, so this is the prefix's sum.
    if (Long.compareUnsigned(selfVerify.stateGasLimit(), MAX_VERIFY_STATE_GAS) > 0) {
      return violation("the self_verify state limit exceeds " + MAX_VERIFY_STATE_GAS);
    }
    for (int i = selfVerifyIndex + 1; i < frames.size(); i++) {
      if (frames.get(i).mode() == Frame.MODE_VERIFY) {
        return violation("VERIFY frame " + i + " follows the validation prefix");
      }
    }
    return ValidationResult.valid();
  }

  /**
   * Checks the policy against the sender's account at the chain head.
   *
   * <p>The sender's existence also matters: approving payment increments the sender nonce, and when
   * that creates the account, {@code APPROVE} charges the new-account state gas from the {@code
   * self_verify} frame's state limit.
   *
   * @param transaction the frame transaction, which passed {@link #checkStatic}
   * @param senderAccount the sender's account, or null if it does not exist
   * @param gasCalculator the gas calculator of the chain head's protocol spec
   * @return valid if the sender has no code, the transaction nonce is the account nonce, the
   *     balance covers the transaction's max fee, and a sender that does not exist or is empty
   *     leaves a state limit that covers its creation
   */
  static ValidationResult<TransactionInvalidReason> checkSender(
      final Transaction transaction,
      final @Nullable Account senderAccount,
      final GasCalculator gasCalculator) {
    final Hash codeHash = senderAccount == null ? Hash.EMPTY : senderAccount.getCodeHash();
    final long accountNonce = senderAccount == null ? 0L : senderAccount.getNonce();
    final Wei balance = senderAccount == null ? Wei.ZERO : senderAccount.getBalance();
    if (!Hash.EMPTY.equals(codeHash)) {
      return violation("the sender has code or a delegation, so its validation needs simulation");
    }
    if (transaction.getNonce() != accountNonce) {
      return violation(
          "frame transaction nonce "
              + Long.toUnsignedString(transaction.getNonce())
              + " is not the sender account nonce "
              + Long.toUnsignedString(accountNonce));
    }
    final Wei maxFee = transaction.getMaxFee().orElseThrow();
    if (balance.lessThan(maxFee)) {
      return violation(
          "sender balance "
              + balance.toQuantityHexString()
              + " is below the max fee "
              + maxFee.toQuantityHexString());
    }
    // EIP-8141 "Direct Evaluation": the sender-existence check decides the APPROVE account-creation
    // charge, which only the self_verify frame's state limit can pay, or the frame halts.
    if (senderAccount == null || senderAccount.isEmpty()) {
      final long newAccountStateGas = gasCalculator.stateGasCostCalculator().newAccountStateGas();
      final List<Frame> frames = transaction.getFrames().orElseThrow();
      final long stateGasLimit = frames.get(hasExpiryFrame(frames) ? 1 : 0).stateGasLimit();
      if (Long.compareUnsigned(stateGasLimit, newAccountStateGas) < 0) {
        return violation(
            "the sender account is missing or empty, and the self_verify state limit is below the "
                + newAccountStateGas
                + " state gas that creating it costs");
      }
    }
    return ValidationResult.valid();
  }

  /**
   * Checks that the code at {@link MainnetTransactionValidator#EXPIRY_VERIFIER} is the canonical
   * expiry verifier, which the policy evaluates directly, when the transaction has an expiry
   * verifier frame.
   *
   * @param transaction the frame transaction, which passed {@link #checkStatic}
   * @param worldView the world state at the chain head
   * @return valid if the transaction has no expiry verifier frame or the verifier code is canonical
   */
  static ValidationResult<TransactionInvalidReason> checkExpiryVerifier(
      final Transaction transaction, final WorldView worldView) {
    if (expiryDeadline(transaction).isEmpty()) {
      return ValidationResult.valid();
    }
    final Account verifier = worldView.get(MainnetTransactionValidator.EXPIRY_VERIFIER);
    if (verifier == null || !EXPIRY_VERIFIER_CODE_HASH.equals(verifier.getCodeHash())) {
      return violation(
          "the code at the expiry verifier address is not the canonical expiry verifier");
    }
    return ValidationResult.valid();
  }

  /**
   * Returns the deadline of a frame transaction whose first frame is a well-formed expiry verifier
   * frame, the only position the policy admits one in.
   *
   * @param transaction any transaction
   * @return the deadline as an unsigned timestamp, or empty if the transaction has no such frame
   */
  public static OptionalLong expiryDeadline(final Transaction transaction) {
    final Optional<Frame> first =
        transaction.getFrames().flatMap(frames -> frames.stream().findFirst());
    if (first.isEmpty()
        || !isExpiryVerifier(first.get())
        || first.get().data().size() != EXPIRY_DATA_LENGTH) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(first.get().data().toLong());
  }

  private static ValidationResult<TransactionInvalidReason> checkExpiryFrame(
      final Frame expiryFrame, final GasCalculator gasCalculator, final long headTimestamp) {
    if (expiryFrame.flags() != 0
        || !expiryFrame.value().isZero()
        || expiryFrame.stateGasLimit() != 0
        || expiryFrame.data().size() != EXPIRY_DATA_LENGTH) {
      return violation("malformed expiry verifier frame");
    }
    // The frame entry charges the cold access to the verifier, then the canonical code runs.
    final long minExecutionGas =
        gasCalculator.getColdAccountAccessCost() + EXPIRY_VERIFIER_EXECUTION_GAS;
    if (Long.compareUnsigned(expiryFrame.executionGasLimit(), minExecutionGas) < 0) {
      return violation("the expiry verifier execution limit is below " + minExecutionGas);
    }
    final long deadline = expiryFrame.data().toLong();
    final long minDeadline = headTimestamp + EXPIRY_ADMISSION_MARGIN_SECONDS;
    if (Long.compareUnsigned(deadline, minDeadline) < 0) {
      return violation(
          "the expiry deadline "
              + Long.toUnsignedString(deadline)
              + " is less than "
              + EXPIRY_ADMISSION_MARGIN_SECONDS
              + " seconds after the chain head timestamp "
              + headTimestamp);
    }
    return ValidationResult.valid();
  }

  private static boolean hasExpiryFrame(final List<Frame> frames) {
    return !frames.isEmpty() && isExpiryVerifier(frames.getFirst());
  }

  private static boolean isExpiryVerifier(final Frame frame) {
    // EIP-8141 identifies the frame by its explicit target; a null target resolves to the sender.
    return frame.mode() == Frame.MODE_VERIFY
        && frame.target().filter(MainnetTransactionValidator.EXPIRY_VERIFIER::equals).isPresent();
  }

  private static boolean isSelfVerify(final Frame frame, final Address sender) {
    return frame.mode() == Frame.MODE_VERIFY
        && frame.flags() == Frame.APPROVE_SCOPE_MASK
        && frame.resolvedTarget(sender).equals(sender);
  }

  private static ValidationResult<TransactionInvalidReason> violation(final String message) {
    return ValidationResult.invalid(
        TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION, message);
  }
}
