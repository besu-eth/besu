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
package org.hyperledger.besu.ethereum.eth.transactions.frame;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.FrameTransactionGas;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionValidator;

import java.util.List;
import java.util.OptionalLong;

/**
 * The EIP-8141 public-mempool validation prefix of a frame transaction: the shortest prefix of
 * frames whose execution sets the payer, recognized structurally before any state is read.
 *
 * <p>The public mempool recognizes four shapes, each optionally preceded by an expiry verifier
 * frame: {@code [self_verify]}, {@code [deploy, self_verify]}, {@code [only_verify, pay]} and
 * {@code [deploy, only_verify, pay]}.
 *
 * @param length the number of frames in the prefix, including any expiry verifier frame
 * @param deployFrame the index of the deploy frame, or -1
 * @param payFrame the index of the frame that must approve payment
 * @param paymaster whether payment is approved by a separate {@code pay} frame
 * @param expiryDeadline the expiry verifier deadline, when the prefix carries one
 */
public record ValidationPrefix(
    int length, int deployFrame, int payFrame, boolean paymaster, OptionalLong expiryDeadline) {

  /** Gas a node may spend validating signatures and simulating the prefix. */
  public static final long MAX_VERIFY_GAS = 100_000L;

  /** State gas that may be budgeted across the prefix. */
  public static final long MAX_VERIFY_STATE_GAS = 500_000L;

  private static final int EXPIRY_DATA_LENGTH = 8;

  /** Thrown when a transaction's frames match no recognized prefix. */
  public static final class UnrecognizedPrefixException extends Exception {
    UnrecognizedPrefixException(final String message) {
      super(message);
    }
  }

  /**
   * Whether the transaction's prefix carries a deploy frame.
   *
   * @return true if a deploy frame precedes verification
   */
  public boolean hasDeploy() {
    return deployFrame >= 0;
  }

  /**
   * Recognizes a transaction's validation prefix.
   *
   * @param transaction the frame transaction
   * @return the recognized prefix
   * @throws UnrecognizedPrefixException if the frames match no recognized shape, or the prefix
   *     exceeds the public-mempool work limits
   */
  public static ValidationPrefix classify(final Transaction transaction)
      throws UnrecognizedPrefixException {
    final List<Frame> frames = transaction.getFrames().orElseThrow();
    final Address sender = transaction.getSender();
    int i = 0;

    OptionalLong deadline = OptionalLong.empty();
    if (isExpiryVerify(frames.get(0))) {
      deadline = OptionalLong.of(frames.get(0).data().toLong());
      i++;
    }

    int deployFrame = -1;
    if (i < frames.size() && isDeploy(frames.get(i))) {
      deployFrame = i++;
    }

    final int payFrame;
    final boolean paymaster;
    if (i < frames.size() && isVerify(frames.get(i), Frame.APPROVE_SCOPE_MASK, sender)) {
      payFrame = i++; // self_verify approves execution and payment
      paymaster = false;
    } else if (i + 1 < frames.size()
        && isVerify(frames.get(i), Frame.FLAG_APPROVE_EXECUTION, sender)
        && isVerify(frames.get(i + 1), Frame.FLAG_APPROVE_PAYMENT, null)) {
      payFrame = i + 1; // only_verify then pay
      paymaster = true;
      i += 2;
    } else {
      throw new UnrecognizedPrefixException("validation prefix matches no recognized shape");
    }
    final int length = i;

    for (int j = length; j < frames.size(); j++) {
      if (frames.get(j).mode() == Frame.MODE_VERIFY) {
        throw new UnrecognizedPrefixException("VERIFY frame " + j + " after the validation prefix");
      }
    }

    long verifyGas = 0L;
    long verifyStateGas = 0L;
    for (int j = 0; j < length; j++) {
      final Frame frame = frames.get(j);
      if (frame.isAtomicBatch()) {
        throw new UnrecognizedPrefixException("atomic batch frame " + j + " in validation prefix");
      }
      verifyGas = saturatingAdd(verifyGas, frame.executionGasLimit());
      verifyStateGas = saturatingAdd(verifyStateGas, frame.stateGasLimit());
    }
    for (final FrameSignature signature : transaction.getFrameSignatures().orElse(List.of())) {
      final long signatureGas = FrameTransactionGas.signatureGas(signature.scheme());
      verifyGas = saturatingAdd(verifyGas, signatureGas < 0 ? Long.MAX_VALUE : signatureGas);
    }
    if (verifyGas > MAX_VERIFY_GAS) {
      throw new UnrecognizedPrefixException(
          "validation work " + verifyGas + " exceeds " + MAX_VERIFY_GAS);
    }
    if (verifyStateGas > MAX_VERIFY_STATE_GAS) {
      throw new UnrecognizedPrefixException(
          "validation state gas " + verifyStateGas + " exceeds " + MAX_VERIFY_STATE_GAS);
    }
    return new ValidationPrefix(length, deployFrame, payFrame, paymaster, deadline);
  }

  private static boolean isExpiryVerify(final Frame frame) {
    return frame.mode() == Frame.MODE_VERIFY
        && frame.flags() == 0
        && frame.target().map(MainnetTransactionValidator.EXPIRY_VERIFIER::equals).orElse(false)
        && frame.data().size() == EXPIRY_DATA_LENGTH;
  }

  private static boolean isDeploy(final Frame frame) {
    return frame.mode() == Frame.MODE_DEFAULT && frame.flags() == 0;
  }

  /** A VERIFY frame with exactly these flags, targeting the sender when one is given. */
  private static boolean isVerify(final Frame frame, final int flags, final Address sender) {
    return frame.mode() == Frame.MODE_VERIFY
        && frame.flags() == flags
        && (sender == null || frame.resolvedTarget(sender).equals(sender));
  }

  private static long saturatingAdd(final long a, final long b) {
    final long sum = a + b;
    return ((a ^ sum) & (b ^ sum)) < 0 ? Long.MAX_VALUE : sum;
  }
}
