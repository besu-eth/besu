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

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * Decodes EIP-8141 frame transactions in their network form, the one PooledTransactions responses
 * and {@code eth_sendRawTransaction} carry.
 *
 * <p>A frame transaction with blobs travels with its sidecar, its EIP-2718 TransactionPayload
 * wrapped as {@code rlp([tx_payload_body, wrapper_version, blobs, commitments, cell_proofs])},
 * exactly as an EIP-4844 blob transaction is. A frame transaction without blobs uses the plain
 * TransactionPayload.
 */
public class FramePooledTransactionDecoder {

  private FramePooledTransactionDecoder() {
    // no instances
  }

  /**
   * Decodes a frame transaction in either network form.
   *
   * @param input the transaction bytes, including the type byte
   * @return the decoded transaction
   * @throws RLPException if the input is malformed, if a transaction with blobs comes without its
   *     sidecar, or if the sidecar does not match the transaction's versioned hashes
   */
  public static Transaction decode(final Bytes input) {
    final RLPInput txRlp = RLP.input(input.slice(1)); // Skip the transaction type byte
    txRlp.enterList();
    // The plain payload starts with chain_id, a scalar, so a leading list is the wrapped payload.
    if (txRlp.nextIsList()) {
      return decodeWrapped(input, txRlp);
    }
    final Transaction transaction = FrameTransactionDecoder.decode(input);
    if (transaction.getBlobCount() > 0) {
      throw new RLPException("Frame transaction with blobs is missing its blob sidecar");
    }
    return transaction;
  }

  private static Transaction decodeWrapped(final Bytes input, final RLPInput txRlp) {
    final RLPInput payloadRlp = txRlp.readAsRlp();
    final Bytes canonicalBytes =
        Bytes.concatenate(Bytes.of(TransactionType.FRAME.getSerializedType()), payloadRlp.raw());
    final Transaction.Builder builder = Transaction.builder();
    FrameTransactionDecoder.readTransactionPayloadInner(builder, payloadRlp);

    int versionId = 0;
    final boolean hasVersionId = !txRlp.nextIsList();
    if (hasVersionId) {
      versionId = txRlp.readIntScalar();
    }
    final List<Blob> blobs = txRlp.readList(Blob::readFrom);
    final List<KZGCommitment> commitments = txRlp.readList(KZGCommitment::readFrom);
    final List<KZGProof> proofs = txRlp.readList(KZGProof::readFrom);
    txRlp.leaveList();

    // The sidecar factory checks the counts against the versioned hashes and the wrapper version's
    // proof rule. What it rejects is malformed input, so it surfaces as an RLPException.
    try {
      builder.kzgBlobs(BlobType.of(versionId), commitments, blobs, proofs);
    } catch (final IllegalArgumentException e) {
      throw new RLPException("Invalid frame transaction blob sidecar: " + e.getMessage());
    }
    final Transaction transaction =
        builder
            .sizeForAnnouncement(input.size())
            .sizeForBlockInclusion(canonicalBytes.size())
            .hash(org.hyperledger.besu.datatypes.Hash.hash(canonicalBytes))
            .build();

    final List<VersionedHash> versionedHashes = transaction.getVersionedHashes().orElseThrow();
    for (int i = 0; i < commitments.size(); i++) {
      final VersionedHash expected =
          new VersionedHash(
              VersionedHash.SHA256_VERSION_ID,
              org.hyperledger.besu.datatypes.Hash.wrap(Hash.sha256(commitments.get(i).getData())));
      if (!expected.equals(versionedHashes.get(i))) {
        throw new RLPException(
            "Commitment at index " + i + " does not match versioned hash in transaction body");
      }
    }

    return transaction;
  }
}
