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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.KZGProof;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * The result of the eth_getBlobAndProofV1 JSON-RPC method contains an array of BlobAndProofV1.
 * BlobAndProofV1 contains the blob data and the kzg proof for the blob.
 */
@JsonPropertyOrder({"blob", "proof"})
public sealed class BlobAndProofV1 permits BlobAndProofV2 {

  private final Blob blob;

  private final KZGProof proof;

  public BlobAndProofV1(final Blob blob) {
    this(blob, null);
  }

  public BlobAndProofV1(final BlobProofBundle blobProofBundle) {
    checkArgument(
        blobProofBundle.getBlobType() == BlobType.KZG_PROOF,
        "Blob type must be KZG_PROOF for BlobAndProofV1");
    this(blobProofBundle.getBlob(), blobProofBundle.getKzgProof().getFirst());
  }

  public BlobAndProofV1(final Blob blob, final KZGProof proof) {
    this.blob = blob;
    this.proof = proof;
  }

  @JsonGetter("blob")
  public Blob getBlob() {
    return blob;
  }

  @JsonGetter("proof")
  public KZGProof getProof() {
    return proof;
  }
}
