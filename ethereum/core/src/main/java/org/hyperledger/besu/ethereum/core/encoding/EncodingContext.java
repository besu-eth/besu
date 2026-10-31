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

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.plugin.data.p2p.Capability;

/**
 * Enum representing the context in which a transaction is being encoded. This context is used to
 * determine the appropriate encoding strategy for a transaction.
 *
 * <p>The context can be one of the following:
 *
 * <ul>
 *   <li>{@link #BLOCK_BODY}: The transaction is part of a block body. This context is used when
 *       encoding transactions for inclusion in a block.
 *   <li>{@link #POOLED_TRANSACTION}: The transaction is part of a transaction pool. This context is
 *       used when encoding transactions that are currently in the transaction pool, waiting to be
 *       included in a block. It is also used when encoding transactions for RPC calls related to
 *       the transaction pool.
 * </ul>
 */
public enum EncodingContext {
  /** Represents the context where the transaction is part of a block body. */
  BLOCK_BODY(true),

  /**
   * Represents the context where the transaction is part of a transaction pool. This context is
   * also used when encoding transactions for RPC calls related to the transaction pool.
   */
  POOLED_TRANSACTION(false),
  POOLED_TRANSACTION_ETH_72(false, true);

  private final boolean encodeForBlock;
  private final boolean elideBlobs;

  EncodingContext(final boolean encodeForBlock) {
    this(encodeForBlock, false);
  }

  EncodingContext(final boolean encodeForBlock, final boolean elideBlobs) {
    this.encodeForBlock = encodeForBlock;
    this.elideBlobs = elideBlobs;
  }

  public boolean encodeForBlock() {
    return encodeForBlock;
  }

  public boolean elideBlobs() {
    return elideBlobs;
  }

  /**
   * Whether this context can encode the given transaction.
   *
   * <p>Only a context that writes blob payloads needs more than the transaction body: a block body
   * carries no sidecar at all, and eth/72 replaces the payloads with an empty list. So the single
   * way to fail is a blob transaction whose blobs this node does not hold — which is every eth/72
   * transaction on arrival, and permanently so for one this node only ever samples.
   *
   * <p>Callers serving or announcing to a peer should skip a transaction this rejects, rather than
   * discovering the gap while encoding: the blob list of such a transaction is empty, so encoding
   * it unguarded would send a malformed transaction rather than fail, and the encoder's guard only
   * fails well away from the cause. Deciding it here also keeps serving and announcing consistent —
   * announcing a transaction to a peer whose protocol version we could not then serve claims an
   * availability we do not have.
   *
   * @param transaction the transaction to encode
   * @return true if this context can encode it
   */
  public boolean canEncode(final Transaction transaction) {
    if (encodeForBlock || elideBlobs) {
      return true;
    }
    if (!transaction.getType().supportsBlob()) {
      return true;
    }
    return transaction
        .getBlobsWithCommitments()
        .map(BlobsWithCommitments::hasBlobData)
        .orElse(false);
  }

  public static EncodingContext pooledTransactionByCapability(final Capability cap) {
    if (cap.getVersion() >= 72) {
      return POOLED_TRANSACTION_ETH_72;
    }
    return POOLED_TRANSACTION;
  }
}
