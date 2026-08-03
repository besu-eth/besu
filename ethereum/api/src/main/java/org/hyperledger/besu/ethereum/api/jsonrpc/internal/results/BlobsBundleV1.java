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
import org.hyperledger.besu.util.HexUtils;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonPropertyOrder({"commitments", "proofs", "blobs"})
public sealed class BlobsBundleV1 permits BlobsBundleV2 {

  private static final Logger LOG = LoggerFactory.getLogger(BlobsBundleV1.class);
  private final List<String> commitments;

  private final List<String> proofs;

  private final List<String> blobs;

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
        blobsWithCommitments.parallelStream()
            .flatMap(b -> b.getKzgCommitments().stream())
            .map(KZGCommitment::getData)
            .map(b -> HexUtils.toFastHex(b, true))
            .collect(Collectors.toList());

    this.proofs =
        blobsWithCommitments.parallelStream()
            .flatMap(b -> b.getKzgProofs().stream())
            .map(KZGProof::getData)
            .map(b -> HexUtils.toFastHex(b, true))
            .collect(Collectors.toList());

    this.blobs =
        blobsWithCommitments.parallelStream()
            .flatMap(b -> b.getBlobs().stream())
            .map(Blob::getData)
            .map(b -> HexUtils.toFastHex(b, true))
            .collect(Collectors.toList());

    LOG.debug(
        "{}: totalTxs: {}, blobTxs: {}, commitments: {}, proofs: {}, blobs: {}",
        getClass().getSimpleName(),
        transactions.size(),
        blobsWithCommitments.size(),
        commitments.size(),
        proofs.size(),
        blobs.size());
  }

  public BlobsBundleV1(
      final List<String> commitments, final List<String> proofs, final List<String> blobs) {
    if (blobs.size() != commitments.size() || blobs.size() != proofs.size()) {
      throw new InvalidParameterException(
          "There must be an equal number of blobs, commitments and proofs");
    }
    this.commitments = commitments;
    this.proofs = proofs;
    this.blobs = blobs;
  }

  @JsonGetter("commitments")
  public List<String> getCommitments() {
    return commitments;
  }

  @JsonGetter("proofs")
  public List<String> getProofs() {
    return proofs;
  }

  @JsonGetter("blobs")
  public List<String> getBlobs() {
    return blobs;
  }
}
