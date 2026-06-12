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
package org.hyperledger.besu.ethereum.core.encoding;

import org.hyperledger.besu.datatypes.BytesHolder;
import org.hyperledger.besu.datatypes.FrameTransactionFrame;
import org.hyperledger.besu.datatypes.FrameTransactionSignature;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * Encoder for EIP-8141 Frame Transactions (type 0x06).
 *
 * <p>RLP payload: [chain_id, nonce, sender, frames, signatures, max_priority_fee_per_gas,
 * max_fee_per_gas, max_fee_per_blob_gas, blob_versioned_hashes]
 */
public class FrameTransactionEncoder {

  private FrameTransactionEncoder() {}

  public static void encode(final Transaction transaction, final RLPOutput out) {
    out.startList();
    out.writeBigIntegerScalar(transaction.getChainId().orElseThrow());
    out.writeLongScalar(transaction.getNonce());
    out.writeBytes(transaction.getSender().getBytes().copy());

    encodeFrames(
        transaction
            .getFrames()
            .orElseThrow(() -> new IllegalStateException("Frame transaction must have frames")),
        out);

    encodeSignatures(
        transaction
            .getFrameSignatures()
            .orElseThrow(() -> new IllegalStateException("Frame transaction must have signatures")),
        out);

    out.writeUInt256Scalar(transaction.getMaxPriorityFeePerGas().orElseThrow());
    out.writeUInt256Scalar(transaction.getMaxFeePerGas().orElseThrow());
    out.writeUInt256Scalar(
        transaction.getMaxFeePerBlobGas().orElse(org.hyperledger.besu.datatypes.Wei.ZERO));

    writeBlobVersionedHashes(out, transaction.getVersionedHashes().orElse(List.of()));

    out.endList();
  }

  static void encodeFrames(final List<FrameTransactionFrame> frames, final RLPOutput out) {
    out.startList();
    for (FrameTransactionFrame frame : frames) {
      encodeSingleFrame(frame, out);
    }
    out.endList();
  }

  static void encodeSingleFrame(final FrameTransactionFrame frame, final RLPOutput out) {
    out.startList();
    out.writeIntScalar(frame.mode());
    out.writeIntScalar(frame.flags());
    out.writeBytes(frame.target().map(BytesHolder::getBytes).map(Bytes::copy).orElse(Bytes.EMPTY));
    out.writeLongScalar(frame.gasLimit());
    out.writeUInt256Scalar(frame.value());
    out.writeBytes(frame.data());
    out.endList();
  }

  static void encodeSignatures(
      final List<FrameTransactionSignature> signatures, final RLPOutput out) {
    out.startList();
    for (FrameTransactionSignature sig : signatures) {
      encodeSingleSignature(sig, out);
    }
    out.endList();
  }

  static void encodeSingleSignature(final FrameTransactionSignature sig, final RLPOutput out) {
    out.startList();
    out.writeIntScalar(sig.scheme());
    out.writeBytes(sig.signer().getBytes().copy());
    out.writeBytes(sig.msg());
    out.writeBytes(sig.signature());
    out.endList();
  }

  static void writeBlobVersionedHashes(
      final RLPOutput out, final List<VersionedHash> versionedHashes) {
    out.startList();
    for (VersionedHash hash : versionedHashes) {
      out.writeBytes(hash.getBytes());
    }
    out.endList();
  }
}
