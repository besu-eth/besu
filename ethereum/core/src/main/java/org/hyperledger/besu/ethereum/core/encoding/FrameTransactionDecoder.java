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
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Decoder for EIP-8141 frame transactions.
 *
 * <p>Payload: {@code [chain_id, nonce, sender, frames, signatures, fees, blob_versioned_hashes]}
 * with {@code frames = [[mode, flags, target, [execution, state], value, data], ...]}, {@code
 * signatures = [[scheme, signer, msg, signature], ...]} and {@code fees =
 * [max_priority_fee_per_gas, max_fee_per_gas, max_fee_per_blob_gas]}.
 */
public class FrameTransactionDecoder {

  private FrameTransactionDecoder() {}

  /**
   * Decodes a frame transaction from its opaque bytes (including the type byte).
   *
   * @param input the full transaction bytes
   * @return the decoded transaction
   */
  public static Transaction decode(final Bytes input) {
    final Transaction.Builder builder = Transaction.builder();
    final RLPInput txRlp = RLP.input(input.slice(1)); // Skip the transaction type byte
    builder
        .sizeForBlockInclusion(input.size())
        .sizeForAnnouncement(input.size())
        .hash(Hash.hash(input));
    readTransactionPayloadInner(builder, txRlp);
    builder.rawRlp(txRlp.raw());
    return builder.build();
  }

  static void readTransactionPayloadInner(final Transaction.Builder builder, final RLPInput input) {
    input.enterList();
    builder
        .type(TransactionType.FRAME)
        .chainId(input.readBigIntegerScalar())
        .nonce(input.readLongScalar())
        .sender(readAddress(input, "Frame transaction sender"))
        .frames(input.readList(FrameTransactionDecoder::decodeFrame))
        .frameSignatures(input.readList(FrameTransactionDecoder::decodeSignature));
    input.enterList();
    builder
        .maxPriorityFeePerGas(Wei.of(input.readUInt256Scalar()))
        .maxFeePerGas(Wei.of(input.readUInt256Scalar()))
        .maxFeePerBlobGas(Wei.of(input.readUInt256Scalar()));
    input.leaveList();
    builder.versionedHashes(
        input.readList(versionedHashes -> new VersionedHash(versionedHashes.readBytes32())));
    input.leaveList();
  }

  private static Address readAddress(final RLPInput input, final String description) {
    final Bytes bytes = input.readBytes();
    if (bytes.size() != Address.SIZE) {
      throw new RLPException(description + " must be a 20-byte address");
    }
    return Address.wrap(bytes);
  }

  private static Frame decodeFrame(final RLPInput input) {
    input.enterList();
    final int mode = input.readIntScalar();
    final int flags = input.readIntScalar();
    final Bytes targetBytes = input.readBytes();
    final Optional<Address> target;
    if (targetBytes.isEmpty()) {
      target = Optional.empty();
    } else if (targetBytes.size() == Address.SIZE) {
      target = Optional.of(Address.wrap(targetBytes));
    } else {
      throw new RLPException("Frame target must be empty or a 20-byte address");
    }
    input.enterList();
    final long executionGasLimit = input.readLongScalar();
    final long stateGasLimit = input.readLongScalar();
    input.leaveList();
    final Wei value = Wei.of(input.readUInt256Scalar());
    final Bytes data = input.readBytes();
    input.leaveList();
    return new Frame(mode, flags, target, executionGasLimit, stateGasLimit, value, data);
  }

  private static FrameSignature decodeSignature(final RLPInput input) {
    input.enterList();
    final int scheme = input.readIntScalar();
    final Bytes signer = input.readBytes();
    if (!signer.isEmpty() && signer.size() != Address.SIZE) {
      throw new RLPException("Frame signature signer must be empty or a 20-byte address");
    }
    final Bytes msg = input.readBytes();
    if (!msg.isEmpty() && msg.size() != 32) {
      throw new RLPException("Frame signature msg must be empty or a 32-byte digest");
    }
    final Bytes signature = input.readBytes();
    input.leaveList();
    return new FrameSignature(scheme, signer, msg, signature);
  }
}
