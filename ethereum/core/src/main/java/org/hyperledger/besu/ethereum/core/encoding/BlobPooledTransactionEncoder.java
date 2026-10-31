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
import static org.slf4j.LoggerFactory.getLogger;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.security.InvalidParameterException;

import org.slf4j.Logger;

public class BlobPooledTransactionEncoder {
  private static final Logger LOG = getLogger(BlobPooledTransactionEncoder.class);
  static final String NO_BLOBS_WITH_COMMITMENTS_ERROR =
      "Transaction with no blobsWithCommitments cannot be encoded for Pooled Transaction";
  static final String NO_BLOBS_ERROR =
      "Transaction with no blobs data cannot be encoded for Pooled Transaction";

  public static void encode(
      final Transaction transaction, final boolean elideBlobs, final RLPOutput out) {
    LOG.trace("Encoding transaction with blobs {}, blobs elision {}", transaction, elideBlobs);
    var blobsWithCommitments = transaction.getBlobsWithCommitments();
    if (blobsWithCommitments.isEmpty()) {
      throw new InvalidParameterException(NO_BLOBS_WITH_COMMITMENTS_ERROR);
    }
    // Without elision the payloads are written out, so they must actually be held: a sidecar
    // holding only cells has none, and getBlobs() is empty for it. With elision, as eth/72 sends
    // it, there is nothing of the payloads to write, so cells are enough. Callers decide whether a
    // transaction is encodable with EncodingContext#canEncode; this is the backstop.
    if (!elideBlobs && !blobsWithCommitments.get().hasBlobData()) {
      throw new InvalidParameterException(NO_BLOBS_ERROR);
    }
    out.startList();
    BlobTransactionEncoder.encode(transaction, out);
    if (blobsWithCommitments.get().getBlobType() == KZG_CELL_PROOFS) {
      out.writeIntScalar(blobsWithCommitments.get().getBlobType().getVersionId());
    }
    if (elideBlobs) {
      out.startList();
      out.endList();
    } else {
      out.writeList(blobsWithCommitments.get().getBlobs(), Blob::writeTo);
    }
    out.writeList(blobsWithCommitments.get().getKzgCommitments(), KZGCommitment::writeTo);
    out.writeList(blobsWithCommitments.get().getKzgProofs(), KZGProof::writeTo);
    out.endList();
  }
}
