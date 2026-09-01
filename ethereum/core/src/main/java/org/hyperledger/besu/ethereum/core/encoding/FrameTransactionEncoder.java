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
package org.hyperledger.besu.ethereum.core.encoding;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * Encoder for EIP-8141 frame transactions.
 *
 * <p>Payload: {@code [chain_id, nonce, sender, frames, signatures, fees, blob_versioned_hashes]}
 * with {@code frames = [[mode, flags, target, [execution, state], value, data], ...]}, {@code
 * signatures = [[scheme, signer, msg, signature], ...]} and {@code fees =
 * [max_priority_fee_per_gas, max_fee_per_gas, max_fee_per_blob_gas]}.
 */
public class FrameTransactionEncoder {

  private FrameTransactionEncoder() {}

  /**
   * Encodes the canonical frame transaction payload (without the type byte).
   *
   * @param transaction the frame transaction
   * @param out the RLP output
   */
  public static void encode(final Transaction transaction, final RLPOutput out) {
    encode(transaction, out, false);
  }

  /**
   * Encodes the signing payload of a frame transaction (without the type byte): signature entries
   * with an empty msg have their raw signature bytes elided.
   *
   * @param transaction the frame transaction
   * @param out the RLP output
   */
  public static void encodeForSigning(final Transaction transaction, final RLPOutput out) {
    encode(transaction, out, true);
  }

  private static void encode(
      final Transaction transaction, final RLPOutput out, final boolean forSigning) {
    final List<Frame> frames = transaction.getFrames().orElse(List.of());
    final List<FrameSignature> signatures = transaction.getFrameSignatures().orElse(List.of());
    out.startList();
    out.writeBigIntegerScalar(transaction.getChainId().orElseThrow());
    out.writeLongScalar(transaction.getNonce());
    out.writeBytes(transaction.getSender().getBytes());
    out.writeList(frames, FrameTransactionEncoder::encodeFrame);
    out.writeList(
        signatures, (signature, rlpOutput) -> encodeSignature(signature, rlpOutput, forSigning));
    out.startList();
    out.writeUInt256Scalar(transaction.getMaxPriorityFeePerGas().orElseThrow());
    out.writeUInt256Scalar(transaction.getMaxFeePerGas().orElseThrow());
    out.writeUInt256Scalar(transaction.getMaxFeePerBlobGas().orElse(Wei.ZERO));
    out.endList();
    BlobTransactionEncoder.writeBlobVersionedHashes(
        out, transaction.getVersionedHashes().orElse(List.of()));
    out.endList();
  }

  private static void encodeFrame(final Frame frame, final RLPOutput out) {
    out.startList();
    out.writeIntScalar(frame.mode());
    out.writeIntScalar(frame.flags());
    out.writeBytes(frame.target().map(Address::getBytes).orElse(Bytes.EMPTY));
    out.startList();
    out.writeLongScalar(frame.executionGasLimit());
    out.writeLongScalar(frame.stateGasLimit());
    out.endList();
    out.writeUInt256Scalar(frame.value());
    out.writeBytes(frame.data());
    out.endList();
  }

  private static void encodeSignature(
      final FrameSignature signature, final RLPOutput out, final boolean forSigning) {
    out.startList();
    out.writeIntScalar(signature.scheme());
    out.writeBytes(signature.signer());
    out.writeBytes(signature.msg());
    // The canonical signature hash elides the raw signature bytes of every entry signing it.
    out.writeBytes(
        forSigning && signature.signsCanonicalHash() ? Bytes.EMPTY : signature.signature());
    out.endList();
  }
}
