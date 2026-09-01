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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper.verify4844Kzg;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.crypto.digests.SHA256Digest;

/**
 * Validator for blob transactions
 *
 * <p>This class enforces blob-related transaction rules, including:
 *
 * <ul>
 *   <li>Checking that required fields are present (recipient, versioned hashes, blobs).
 *   <li>Ensuring the number of blobs does not exceed the transaction gas limit cap.
 *   <li>Validating versioned hashes and their supported formats.
 *   <li>Verifying blob commitments against their hashes and KZG proofs.
 * </ul>
 *
 * <p>The validator separates generic blob transaction checks from blob/commitment validation,
 * ensuring clear responsibilities between transaction-level and blob-level rules.
 */
public class MainnetBlobsValidator {
  final Set<BlobType> acceptedBlobVersions;
  final GasLimitCalculator gasLimitCalculator;
  final GasCalculator gasCalculator;

  /**
   * Creates a new {@link MainnetBlobsValidator}.
   *
   * @param acceptedBlobVersions the set of blob types accepted on this network
   * @param gasLimitCalculator calculator for transaction blob gas caps
   * @param gasCalculator calculator for gas usage of blobs
   */
  public MainnetBlobsValidator(
      final Set<BlobType> acceptedBlobVersions,
      final GasLimitCalculator gasLimitCalculator,
      final GasCalculator gasCalculator) {
    this.acceptedBlobVersions = acceptedBlobVersions;
    this.gasLimitCalculator = gasLimitCalculator;
    this.gasCalculator = gasCalculator;
  }

  /**
   * Validates a transaction for compliance with blob-related rules.
   *
   * <p>Performs transaction-level validation (recipient, versioned hashes, gas cap, supported hash
   * versions) and, if present, validates the blobs and commitments themselves.
   *
   * @param transaction the transaction to validate
   * @return a {@link ValidationResult} indicating validity or the reason for rejection
   */
  public ValidationResult<TransactionInvalidReason> validate(final Transaction transaction) {
    ValidationResult<TransactionInvalidReason> blobTransactionResult =
        validateBlobTransaction(transaction);
    if (!blobTransactionResult.isValid()) {
      return blobTransactionResult;
    }
    if (transaction.getBlobsWithCommitments().isPresent()) {
      return validateBlobsWithCommitments(transaction);
    }
    return ValidationResult.valid();
  }

  /**
   * Validates transaction-level blob rules (independent of blob data itself).
   *
   * <ul>
   *   <li>Blob transactions must have a recipient address.
   *   <li>Versioned hashes must be present and valid.
   *   <li>Blob gas cost must not exceed the configured cap.
   *   <li>Only supported versioned hash IDs are accepted.
   * </ul>
   *
   * @param transaction the blob transaction to validate
   * @return a {@link ValidationResult} indicating success or reason for failure
   */
  private ValidationResult<TransactionInvalidReason> validateBlobTransaction(
      final Transaction transaction) {

    // Blob transactions must target a recipient account (frame transactions have no top-level
    // recipient by design)
    if (transaction.getType() == org.hyperledger.besu.datatypes.TransactionType.BLOB
        && transaction.getTo().isEmpty()) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
          "transaction blob transactions must have a to address");
    }

    // Versioned hashes must be present
    if (transaction.getVersionedHashes().isEmpty()) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_BLOBS,
          "transaction blob transactions must specify one or more versioned hashes");
    }

    // Check blob gas cost against configured cap
    final List<VersionedHash> versionedHashes = transaction.getVersionedHashes().get();
    final long blobGasCost = gasCalculator.blobGasCost(versionedHashes.size());
    if (blobGasCost > gasLimitCalculator.transactionBlobGasLimitCap()) {
      final String error =
          String.format("Blob transaction has too many blobs: %d", versionedHashes.size());
      return ValidationResult.invalid(TransactionInvalidReason.TOTAL_BLOB_GAS_TOO_HIGH, error);
    }

    // Ensure all versioned hashes use a supported version ID
    for (final VersionedHash versionedHash : versionedHashes) {
      if (versionedHash.getVersionId() != VersionedHash.SHA256_VERSION_ID) {
        return ValidationResult.invalid(
            TransactionInvalidReason.INVALID_BLOBS,
            "transaction blobs commitment version is not supported. Expected "
                + VersionedHash.SHA256_VERSION_ID
                + ", found "
                + versionedHash.getVersionId());
      }
    }
    return ValidationResult.valid();
  }

  /**
   * Validates a transaction's blobs and their commitments.
   *
   * <ul>
   *   <li>Ensures blobs and commitments are present and sizes match.
   *   <li>Validates that commitment hashes match the provided versioned hashes.
   *   <li>Verifies KZG proofs for all commitments.
   * </ul>
   *
   * @param transaction the transaction containing blobs and commitments
   * @return a {@link ValidationResult} indicating success or reason for failure
   */
  private ValidationResult<TransactionInvalidReason> validateBlobsWithCommitments(
      final Transaction transaction) {

    if (transaction.getBlobsWithCommitments().isEmpty()) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_BLOBS,
          "transaction blobs are empty, cannot verify without blobs");
    }

    BlobsWithCommitments blobsWithCommitments = transaction.getBlobsWithCommitments().get();

    // Reject unsupported blob types
    if (!acceptedBlobVersions.contains(blobsWithCommitments.getBlobType())) {
      String error =
          String.format(
              "Unsupported blob type: %s. Supported types: %s.",
              blobsWithCommitments.getBlobType(), acceptedBlobVersions);
      return ValidationResult.invalid(TransactionInvalidReason.INVALID_BLOBS, error);
    }

    // Blobs and commitments must be the same size
    if (blobsWithCommitments.getBlobs().size() != blobsWithCommitments.getKzgCommitments().size()) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_BLOBS,
          "transaction blobs and commitments are not the same size");
    }

    // Versioned hashes and commitments must be the same size
    if (transaction.getVersionedHashes().get().size()
        != blobsWithCommitments.getKzgCommitments().size()) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_BLOBS,
          "transaction versioned hashes and commitments are not the same size");
    }
    final List<VersionedHash> versionedHashes = transaction.getVersionedHashes().get();

    // Validate that each commitment matches its versioned hash
    for (int i = 0; i < versionedHashes.size(); i++) {
      final KZGCommitment commitment = blobsWithCommitments.getKzgCommitments().get(i);
      final VersionedHash versionedHash = versionedHashes.get(i);

      final VersionedHash calculatedVersionedHash = hashCommitment(commitment);
      if (!calculatedVersionedHash.equals(versionedHash)) {
        return ValidationResult.invalid(
            TransactionInvalidReason.INVALID_BLOBS,
            "transaction blobs commitment hash does not match commitment");
      }
    }

    // Verify KZG proofs for the blobs
    if (!verify4844Kzg(blobsWithCommitments)) {
      return ValidationResult.invalid(
          TransactionInvalidReason.INVALID_BLOBS,
          "transaction blobs kzg proof verification failed");
    }
    return ValidationResult.valid();
  }

  /**
   * Computes a {@link VersionedHash} from a {@link KZGCommitment}.
   *
   * <p>The commitment data is hashed with SHA-256 and then versioned by setting the first byte to
   * {@link VersionedHash#SHA256_VERSION_ID}.
   *
   * @param commitment the KZG commitment to hash
   * @return the corresponding {@link VersionedHash}
   */
  public static VersionedHash hashCommitment(final KZGCommitment commitment) {
    final SHA256Digest digest = new SHA256Digest();
    digest.update(commitment.getData().toArrayUnsafe(), 0, commitment.getData().size());

    final byte[] dig = new byte[digest.getDigestSize()];
    digest.doFinal(dig, 0);

    // Prefix with supported version ID
    dig[0] = VersionedHash.SHA256_VERSION_ID;
    return new VersionedHash(Bytes32.wrap(dig));
  }
}
