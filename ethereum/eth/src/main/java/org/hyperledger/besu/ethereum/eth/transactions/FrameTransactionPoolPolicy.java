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
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.KeyedNonces;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * The EIP-8141 public-mempool policy for frame transactions, limited to the transactions whose
 * validation prefix a node can evaluate directly, without running the EVM (EIP-8141, "Direct
 * Evaluation of Protocol-Defined Frames").
 *
 * <p>The admitted class is {@code [self_verify, (SENDER|DEFAULT|ASSERT)*]}: a sender with the empty
 * code hash, so its {@code self_verify} frame runs the protocol default code; one secp256k1
 * signature by the sender over the canonical signature hash, which that default code checks; a
 * legacy nonce equal to the sender's account nonce; and no blobs. The prefix then depends only on
 * transaction fields and the sender's code hash, nonce and balance. Frames after it run once
 * payment is approved and are outside mempool validation. Every other shape is rejected with {@link
 * TransactionInvalidReason#FRAME_MEMPOOL_POLICY_VIOLATION}.
 *
 * <p>Direct evaluation applies the same limits as simulation (EIP-8141, structural rule 6): the
 * validation prefix's execution limits plus the cost of validating the signature must not exceed
 * {@link #MAX_VERIFY_GAS}, and its state limits must not exceed {@link #MAX_VERIFY_STATE_GAS}.
 */
final class FrameTransactionPoolPolicy {

  /**
   * EIP-8141 {@code MAX_VERIFY_GAS}: gas for signature validation plus the validation prefix's
   * execution limits.
   */
  static final long MAX_VERIFY_GAS = 100_000L;

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
   * @return valid if the transaction has the admitted shape
   */
  static ValidationResult<TransactionInvalidReason> checkStatic(
      final Transaction transaction,
      final TransactionPoolConfiguration.Implementation implementation,
      final GasCalculator gasCalculator) {
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
    if (!isSelfVerify(frames.getFirst(), sender)) {
      return violation("the first frame must be a self_verify frame targeting the sender");
    }
    final Frame selfVerify = frames.getFirst();
    // EIP-8141: signature validation counts against MAX_VERIFY_GAS. The frame entry charges the
    // warm access to the sender, so a smaller limit halts the frame and invalidates the
    // transaction.
    final long minExecutionGas = gasCalculator.getWarmStorageReadCost();
    final long maxExecutionGas = MAX_VERIFY_GAS - FrameTransactionGas.SECP256K1_SIGNATURE_GAS;
    final long executionGas = selfVerify.executionGasLimit();
    if (Long.compareUnsigned(executionGas, minExecutionGas) < 0
        || Long.compareUnsigned(executionGas, maxExecutionGas) > 0) {
      return violation(
          "the self_verify execution limit must be between "
              + minExecutionGas
              + " and "
              + maxExecutionGas);
    }
    if (Long.compareUnsigned(selfVerify.stateGasLimit(), MAX_VERIFY_STATE_GAS) > 0) {
      return violation("the self_verify state limit exceeds " + MAX_VERIFY_STATE_GAS);
    }
    for (int i = 1; i < frames.size(); i++) {
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
      final long stateGasLimit = transaction.getFrames().orElseThrow().getFirst().stateGasLimit();
      if (Long.compareUnsigned(stateGasLimit, newAccountStateGas) < 0) {
        return violation(
            "the sender account is missing or empty, and the self_verify state limit is below the "
                + newAccountStateGas
                + " state gas that creating it costs");
      }
    }
    return ValidationResult.valid();
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
