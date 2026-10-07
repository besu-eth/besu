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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.ethereum.core.kzg.KZGProof;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.tuweni.bytes.Bytes;

/**
 * The result of the engine_getBlobsV4 JSON-RPC method contains an array of BlobCellsAndProofsV1.
 * BlobCellsAndProofsV1 contains the cells selected by the requested indices bitarray and their
 * corresponding KZG proofs.
 */
@JsonPropertyOrder({"blob_cells", "proofs"})
public class BlobCellsAndProofsV1 {

  private final List<Bytes> blobCells;

  private final List<KZGProof> proofs;

  public BlobCellsAndProofsV1(final List<Bytes> blobCells, final List<KZGProof> proofs) {
    this.blobCells = blobCells;
    this.proofs = proofs;
  }

  @JsonProperty("blob_cells")
  public List<Bytes> getBlobCells() {
    return blobCells;
  }

  @JsonProperty("proofs")
  public List<KZGProof> getProofs() {
    return proofs;
  }
}
