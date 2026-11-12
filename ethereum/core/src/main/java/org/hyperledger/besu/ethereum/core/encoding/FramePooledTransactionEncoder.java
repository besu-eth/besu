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

import static org.hyperledger.besu.datatypes.BlobType.KZG_CELL_PROOFS;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.security.InvalidParameterException;
import java.util.Optional;

/**
 * Encodes EIP-8141 frame transactions in their network form, the inverse of {@link
 * FramePooledTransactionDecoder}.
 */
public class FramePooledTransactionEncoder {
  private FramePooledTransactionEncoder() {
    // no instances
  }

  /**
   * Encodes a frame transaction without the type byte: wrapped with its sidecar as {@code
   * [tx_payload_body, wrapper_version, blobs, commitments, cell_proofs]} when it has one, and as
   * the plain payload otherwise.
   *
   * @param transaction the frame transaction
   * @param out the RLP output
   * @throws InvalidParameterException if the sidecar holds cells rather than blobs
   */
  public static void encode(final Transaction transaction, final RLPOutput out) {
    final Optional<BlobsWithCommitments> blobsWithCommitments =
        transaction.getBlobsWithCommitments();
    if (blobsWithCommitments.isEmpty()) {
      FrameTransactionEncoder.encode(transaction, out);
      return;
    }
    if (!blobsWithCommitments.get().hasBlobData()) {
      // A sidecar holding only cells has no blob payload to write.
      throw new InvalidParameterException(
          "Frame transaction with no blobs data cannot be encoded for Pooled Transaction");
    }
    out.startList();
    FrameTransactionEncoder.encode(transaction, out);
    if (blobsWithCommitments.get().getBlobType() == KZG_CELL_PROOFS) {
      out.writeIntScalar(blobsWithCommitments.get().getBlobType().getVersionId());
    }
    out.writeList(blobsWithCommitments.get().getBlobs(), Blob::writeTo);
    out.writeList(blobsWithCommitments.get().getKzgCommitments(), KZGCommitment::writeTo);
    out.writeList(blobsWithCommitments.get().getKzgProofs(), KZGProof::writeTo);
    out.endList();
  }
}
