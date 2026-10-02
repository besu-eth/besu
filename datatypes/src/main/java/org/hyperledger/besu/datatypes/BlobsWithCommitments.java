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
package org.hyperledger.besu.datatypes;

import java.util.List;

/** A class to hold the blobs, commitments, proofs and versioned hashes for a set of blobs. */
public interface BlobsWithCommitments {

  /**
   * Get the blob type.
   *
   * @return the blob type
   */
  BlobType getBlobType();

  /**
   * Get the blobs if present.
   *
   * @return the blobs or empty list if not present
   */
  List<? extends Blob> getBlobs();

  /**
   * Get the commitments.
   *
   * @return the commitments
   */
  List<? extends KZGCommitment> getKzgCommitments();

  /**
   * Get the proofs.
   *
   * @return the proofs
   */
  List<? extends KZGProof> getKzgProofs();

  /**
   * Get the hashes.
   *
   * @return the hashes
   */
  List<VersionedHash> getVersionedHashes();
}
