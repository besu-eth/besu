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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.FrameTransactionFrame;
import org.hyperledger.besu.datatypes.FrameTransactionSignature;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Decoder for EIP-8141 Frame Transactions (type 0x06).
 *
 * <p>RLP payload: [chain_id, nonce, sender, frames, signatures, max_priority_fee_per_gas,
 * max_fee_per_gas, max_fee_per_blob_gas, blob_versioned_hashes]
 *
 * <p>frame = [mode, flags, target, gas_limit, value, data]
 *
 * <p>signature = [scheme, signer, msg, signature]
 */
public class FrameTransactionDecoder {

  private FrameTransactionDecoder() {}

  public static Transaction decode(final Bytes input) {
    final RLPInput txRlp = RLP.input(input.slice(1));
    txRlp.enterList();

    final BigInteger chainId = txRlp.readBigIntegerScalar();
    final long nonce = txRlp.readLongScalar();
    final Address sender = Address.wrap(txRlp.readBytes());

    final var frames = txRlp.readList(FrameTransactionDecoder::decodeFrame);
    final var signatures = txRlp.readList(FrameTransactionDecoder::decodeSignature);

    final Wei maxPriorityFeePerGas = Wei.of(txRlp.readUInt256Scalar());
    final Wei maxFeePerGas = Wei.of(txRlp.readUInt256Scalar());
    final Wei maxFeePerBlobGas = Wei.of(txRlp.readUInt256Scalar());

    final var blobVersionedHashes = txRlp.readList(rlp -> new VersionedHash(rlp.readBytes32()));

    txRlp.leaveList();

    // Compute total gas limit from all frames
    long totalGasLimit = 0;
    for (FrameTransactionFrame frame : frames) {
      totalGasLimit += frame.gasLimit();
    }

    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(chainId)
        .nonce(nonce)
        .sender(sender)
        .frames(frames)
        .frameSignatures(signatures)
        .maxPriorityFeePerGas(maxPriorityFeePerGas)
        .maxFeePerGas(maxFeePerGas)
        .maxFeePerBlobGas(blobVersionedHashes.isEmpty() ? null : maxFeePerBlobGas)
        .versionedHashes(blobVersionedHashes.isEmpty() ? null : blobVersionedHashes)
        .gasLimit(totalGasLimit)
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .rawRlp(txRlp.raw())
        .sizeForBlockInclusion(input.size())
        .sizeForAnnouncement(input.size())
        .hash(Hash.hash(input))
        .build();
  }

  static FrameTransactionFrame decodeFrame(final RLPInput input) {
    input.enterList();
    final int mode = input.readIntScalar();
    final int flags = input.readIntScalar();
    final Bytes targetBytes = input.readBytes();
    final Optional<Address> target =
        targetBytes.isEmpty() ? Optional.empty() : Optional.of(Address.wrap(targetBytes));
    final long gasLimit = input.readLongScalar();
    final Wei value = Wei.of(input.readUInt256Scalar());
    final Bytes data = input.readBytes();
    input.leaveList();
    return new FrameTransactionFrame(mode, flags, target, gasLimit, value, data);
  }

  static FrameTransactionSignature decodeSignature(final RLPInput input) {
    input.enterList();
    final int scheme = input.readIntScalar();
    final Address signer = Address.wrap(input.readBytes());
    final Bytes msg = input.readBytes();
    final Bytes signature = input.readBytes();
    input.leaveList();
    return new FrameTransactionSignature(scheme, signer, msg, signature);
  }
}
