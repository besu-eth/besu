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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonPropertyOrder({"commitments", "proofs", "blobs"})
public sealed class BlobsBundleV1 permits BlobsBundleV2 {

  private static final Logger LOG = LoggerFactory.getLogger(BlobsBundleV1.class);
  private final List<KZGCommitment> commitments;

  private final List<KZGProof> proofs;

  private final List<Blob> blobs;

  public BlobsBundleV1(final List<Transaction> transactions) {
    this(transactions, UnaryOperator.identity());
  }

  protected BlobsBundleV1(
      final List<Transaction> transactions,
      final UnaryOperator<BlobsWithCommitments> blobsWithCommitmentsProcessor) {
    final List<BlobsWithCommitments> blobsWithCommitments =
        transactions.stream()
            .map(Transaction::getBlobsWithCommitments)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(blobsWithCommitmentsProcessor)
            .toList();

    this.commitments =
        blobsWithCommitments.stream()
            .map(BlobsWithCommitments::getKzgCommitments)
            .flatMap(List::stream)
            .toList();

    this.proofs =
        blobsWithCommitments.stream()
            .map(BlobsWithCommitments::getKzgProofs)
            .flatMap(List::stream)
            .toList();

    this.blobs =
        blobsWithCommitments.stream()
            .map(BlobsWithCommitments::getBlobs)
            .flatMap(List::stream)
            .toList();

    LOG.atDebug()
        .setMessage(
            "BlobsBundleV1: totalTxs: {}, blobTxs: {}, commitments: {}, proofs: {}, blobs: {}")
        .addArgument(transactions::size)
        .addArgument(blobsWithCommitments::size)
        .addArgument(commitments::size)
        .addArgument(proofs::size)
        .addArgument(blobs::size)
        .log();
  }

  @JsonGetter("commitments")
  public List<KZGCommitment> getCommitments() {
    return commitments;
  }

  @JsonGetter("proofs")
  public List<KZGProof> getProofs() {
    return proofs;
  }

  @JsonGetter("blobs")
  public List<Blob> getBlobs() {
    return blobs;
  }
}
