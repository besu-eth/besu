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

import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import org.apache.tuweni.bytes.Bytes;

public class BlobTransactionDecoder {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private BlobTransactionDecoder() {
    // private constructor
  }

  /**
   * Decodes a blob transaction from the provided RLP input.
   *
   * @param input the RLP input to decode
   * @return the decoded transaction
   */
  public static Transaction decode(final Bytes input) {
    Transaction transaction;
    transaction = readTransactionPayload(input);
    return transaction;
  }

  private static Transaction readTransactionPayload(final Bytes input) {
    final Transaction.Builder builder = Transaction.builder();
    final RLPInput txRlp = RLP.input(input.slice(1)); // Skip the transaction type byte
    builder.sizeForBlockInclusion(input.size()).hash(Hash.hash(input));
    // blob tx without blobs cannot be announced, so sizeForAnnouncement is not set
    readTransactionPayloadInner(builder, txRlp);
    return builder.build();
  }

  /**
   * Reads the payload of a blob transaction from the provided RLP input.
   *
   * <p>[chain_id, nonce, max_priority_fee_per_gas, max_fee_per_gas, gas_limit, to, value, data,
   * access_list, max_fee_per_blob_gas, blob_versioned_hashes, y_parity, r, s]
   *
   * @param builder the transaction builder
   * @param input the RLP input to read from
   */
  static void readTransactionPayloadInner(final Transaction.Builder builder, final RLPInput input) {
    input.enterList();
    builder
        .type(TransactionType.BLOB)
        .chainId(input.readBigIntegerScalar())
        .nonce(input.readLongScalar())
        .maxPriorityFeePerGas(Wei.of(input.readUInt256Scalar()))
        .maxFeePerGas(Wei.of(input.readUInt256Scalar()))
        .gasLimit(input.readLongScalar())
        .to(input.readBytes(v -> v.isEmpty() ? null : Address.wrap(v)))
        .value(Wei.of(input.readUInt256Scalar()))
        .payload(input.readBytes())
        .accessList(
            input.readList(
                accessListEntryRLPInput -> {
                  accessListEntryRLPInput.enterList();
                  final AccessListEntry accessListEntry =
                      new AccessListEntry(
                          Address.wrap(accessListEntryRLPInput.readBytes()),
                          accessListEntryRLPInput.readList(RLPInput::readBytes32));
                  accessListEntryRLPInput.leaveList();
                  return accessListEntry;
                }))
        .maxFeePerBlobGas(Wei.of(input.readUInt256Scalar()))
        .versionedHashes(
            input.readList(versionedHashes -> new VersionedHash(versionedHashes.readBytes32())));

    final byte recId = (byte) input.readUnsignedByteScalar();
    builder.signature(
        SIGNATURE_ALGORITHM.createSignature(
            input.readUInt256Scalar().toUnsignedBigInteger(),
            input.readUInt256Scalar().toUnsignedBigInteger(),
            recId));
    input.leaveList();
  }
}
