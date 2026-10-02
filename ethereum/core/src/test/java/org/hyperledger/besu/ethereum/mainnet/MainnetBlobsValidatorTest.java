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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes48;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MainnetBlobsValidatorTest {

  private MainnetBlobsValidator blobsValidator;
  private Transaction transaction;
  private BlobsWithCommitments blobsWithCommitments;

  /**
   * Every case here is rejected before the completeness check, so any params work; the mempool ones
   * are used because that is the path blob validation matters most on.
   */
  private final TransactionValidationParams transactionValidationParams =
      TransactionValidationParams.transactionPool();

  @BeforeEach
  void setUp() {
    blobsValidator =
        new MainnetBlobsValidator(
            Set.of(BlobType.KZG_PROOF, BlobType.KZG_CELL_PROOFS),
            mock(GasLimitCalculator.class),
            mock(GasCalculator.class));

    transaction = mock(Transaction.class);
    blobsWithCommitments = mock(BlobsWithCommitments.class);
  }

  @Test
  void shouldRejectWhenBlobAndCommitmentCountsDiffer() {
    when(transaction.getType()).thenReturn(TransactionType.BLOB);
    VersionedHash versionedHash = mock(VersionedHash.class);
    when(versionedHash.getVersionId()).thenReturn((byte) 1);
    when(transaction.getTo())
        .thenReturn(Optional.of(mock(org.hyperledger.besu.datatypes.Address.class)));
    when(transaction.getVersionedHashes()).thenReturn(Optional.of(List.of(versionedHash)));
    when(transaction.getBlobsWithCommitments()).thenReturn(Optional.of(blobsWithCommitments));
    when(blobsWithCommitments.getBlobType()).thenReturn(BlobType.KZG_CELL_PROOFS);
    // the count is only asked of a transaction that carries its blobs
    when(blobsWithCommitments.hasBlobData()).thenReturn(true);
    when(blobsWithCommitments.getBlobs()).thenReturn(List.of(mock(Blob.class)));
    when(blobsWithCommitments.getKzgCommitments()).thenReturn(List.of());

    var result = blobsValidator.validate(transaction, transactionValidationParams);

    assertInvalidResult(
        result,
        TransactionInvalidReason.INVALID_BLOBS,
        "transaction blobs and commitments are not the same size");
  }

  @Test
  void shouldNotAskForTheBlobCountOfATransactionThatHoldsCellsOnly() {
    // getBlobs() is empty for a sidecar holding cells, so asking it for a count would report a
    // size mismatch. Reaching the completeness check at all means the count was not asked for.
    setUpOneWellFormedBlob();
    when(blobsWithCommitments.hasBlobData()).thenReturn(false);
    when(blobsWithCommitments.getBlobs()).thenReturn(List.of());
    when(blobsWithCommitments.allCellsPresent()).thenReturn(false);

    var result =
        blobsValidator.validate(transaction, TransactionValidationParams.processingBlock());

    assertInvalidResult(result, TransactionInvalidReason.INVALID_BLOBS, "not all cells present");
  }

  @Test
  void shouldRejectBlobTransactionWithoutRecipient() {
    when(transaction.getType()).thenReturn(TransactionType.BLOB);
    when(transaction.getTo()).thenReturn(Optional.empty());
    var result = blobsValidator.validate(transaction, transactionValidationParams);
    assertInvalidResult(
        result,
        TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
        "transaction blob transactions must have a to address");
  }

  @Test
  void shouldRejectBlobTransactionWithoutVersionedHashes() {
    when(transaction.getType()).thenReturn(TransactionType.BLOB);
    when(transaction.getTo())
        .thenReturn(Optional.of(mock(org.hyperledger.besu.datatypes.Address.class)));
    when(transaction.getVersionedHashes()).thenReturn(Optional.empty());
    var result = blobsValidator.validate(transaction, transactionValidationParams);
    assertInvalidResult(
        result,
        TransactionInvalidReason.INVALID_BLOBS,
        "transaction blob transactions must specify one or more versioned hashes");
  }

  @Test
  void shouldRejectWhenVersionedHashesHaveUnsupportedVersion() {
    when(transaction.getType()).thenReturn(TransactionType.BLOB);
    VersionedHash invalidVersionedHash = mock(VersionedHash.class);
    when(invalidVersionedHash.getVersionId()).thenReturn((byte) 9);
    when(transaction.getTo())
        .thenReturn(Optional.of(mock(org.hyperledger.besu.datatypes.Address.class)));
    when(transaction.getVersionedHashes()).thenReturn(Optional.of(List.of(invalidVersionedHash)));
    var result = blobsValidator.validate(transaction, transactionValidationParams);
    assertInvalidResult(
        result,
        TransactionInvalidReason.INVALID_BLOBS,
        "transaction blobs commitment version is not supported. Expected 1, found 9");
  }

  @Test
  void shouldRejectWhenVersionedHashesAndCommitmentsCountsDiffer() {
    when(transaction.getType()).thenReturn(TransactionType.BLOB);
    when(transaction.getTo()).thenReturn(Optional.of(mock(Address.class)));
    Blob blob = mock(Blob.class);
    KZGCommitment commitment = mock(KZGCommitment.class);
    when(commitment.getData()).thenReturn(Bytes48.random());
    when(blobsWithCommitments.getBlobType()).thenReturn(BlobType.KZG_CELL_PROOFS);
    when(blobsWithCommitments.getBlobs()).thenReturn(List.of(blob));
    when(blobsWithCommitments.getKzgCommitments()).thenReturn(List.of(commitment));
    VersionedHash hash1 = MainnetBlobsValidator.hashCommitment(commitment);
    // Add the same hash twice to create a size mismatch
    when(transaction.getVersionedHashes()).thenReturn(Optional.of(List.of(hash1, hash1)));
    when(transaction.getBlobsWithCommitments()).thenReturn(Optional.of(blobsWithCommitments));
    var result = blobsValidator.validate(transaction, transactionValidationParams);
    assertInvalidResult(
        result,
        TransactionInvalidReason.INVALID_BLOBS,
        "transaction versioned hashes and commitments are not the same size");
  }

  @Test
  void shouldRejectWhenBlobCountExceedsTransactionLimit() {
    when(transaction.getType()).thenReturn(TransactionType.BLOB);
    when(transaction.getTo())
        .thenReturn(Optional.of(mock(org.hyperledger.besu.datatypes.Address.class)));
    when(transaction.getBlobsWithCommitments()).thenReturn(Optional.of(blobsWithCommitments));
    when(blobsWithCommitments.getBlobType()).thenReturn(BlobType.KZG_CELL_PROOFS);
    when(blobsWithCommitments.getBlobs()).thenReturn(List.of(mock(Blob.class)));
    when(blobsWithCommitments.getKzgCommitments()).thenReturn(List.of(mock(KZGCommitment.class)));
    when(transaction.getVersionedHashes())
        .thenReturn(Optional.of(List.of(mock(VersionedHash.class))));

    GasLimitCalculator gasLimitCalculator = mock(GasLimitCalculator.class);
    GasCalculator gasCalculator = mock(GasCalculator.class);
    when(gasCalculator.blobGasCost(1)).thenReturn(100L);
    when(gasLimitCalculator.transactionBlobGasLimitCap()).thenReturn(50L);
    blobsValidator =
        new MainnetBlobsValidator(
            Set.of(BlobType.KZG_PROOF, BlobType.KZG_CELL_PROOFS),
            gasLimitCalculator,
            gasCalculator);
    var result = blobsValidator.validate(transaction, transactionValidationParams);
    assertInvalidResult(
        result,
        TransactionInvalidReason.TOTAL_BLOB_GAS_TOO_HIGH,
        "Blob transaction has too many blobs: 1");
  }

  @Test
  void shouldRejectUnsupportedBlobType() {
    when(transaction.getType()).thenReturn(TransactionType.BLOB);
    when(transaction.getTo())
        .thenReturn(Optional.of(mock(org.hyperledger.besu.datatypes.Address.class)));
    when(transaction.getBlobsWithCommitments()).thenReturn(Optional.of(blobsWithCommitments));
    when(blobsWithCommitments.getBlobType()).thenReturn(BlobType.KZG_CELL_PROOFS);
    when(blobsWithCommitments.getBlobs()).thenReturn(List.of(mock(Blob.class)));
    when(blobsWithCommitments.getKzgCommitments()).thenReturn(List.of(mock(KZGCommitment.class)));
    when(transaction.getVersionedHashes())
        .thenReturn(Optional.of(List.of(VersionedHash.DEFAULT_VERSIONED_HASH)));

    blobsValidator =
        new MainnetBlobsValidator(
            Set.of(BlobType.KZG_PROOF), // Only accept KZG_PROOF
            mock(GasLimitCalculator.class),
            mock(GasCalculator.class));
    var result = blobsValidator.validate(transaction, transactionValidationParams);
    assertInvalidResult(
        result,
        TransactionInvalidReason.INVALID_BLOBS,
        "Unsupported blob type: KZG_CELL_PROOFS. Supported types: [KZG_PROOF].");
  }

  @Test
  void shouldRejectAPartiallySampledTransactionWhenProcessingABlock() {
    setUpOneWellFormedBlob();
    when(blobsWithCommitments.allCellsPresent()).thenReturn(false);

    var result =
        blobsValidator.validate(transaction, TransactionValidationParams.processingBlock());

    assertInvalidResult(result, TransactionInvalidReason.INVALID_BLOBS, "not all cells present");
  }

  @Test
  void shouldAcceptAPartiallySampledTransactionInTheTransactionPool() {
    // The eth/72 case: a transaction arrives with its blobs elided, and the pool holds it while the
    // cells are sampled. Nothing is verifiable while no cell is held.
    setUpOneWellFormedBlob();
    when(blobsWithCommitments.allCellsPresent()).thenReturn(false);
    when(blobsWithCommitments.getCellMask()).thenReturn(CellMask.EMPTY);

    var result =
        blobsValidator.validate(transaction, TransactionValidationParams.transactionPool());

    assertTrue(result.isValid());
  }

  /**
   * One blob whose commitment matches its versioned hash, so validation reaches the cell checks.
   */
  private void setUpOneWellFormedBlob() {
    final KZGCommitment commitment = mock(KZGCommitment.class);
    when(commitment.getData()).thenReturn(Bytes48.random());
    // hashCommitment reads the commitment, so it cannot be called inside a when(...) argument
    final VersionedHash versionedHash = MainnetBlobsValidator.hashCommitment(commitment);
    when(transaction.getType()).thenReturn(TransactionType.BLOB);
    when(transaction.getTo()).thenReturn(Optional.of(mock(Address.class)));
    when(transaction.getVersionedHashes()).thenReturn(Optional.of(List.of(versionedHash)));
    when(transaction.getBlobsWithCommitments()).thenReturn(Optional.of(blobsWithCommitments));
    when(blobsWithCommitments.getBlobType()).thenReturn(BlobType.KZG_CELL_PROOFS);
    when(blobsWithCommitments.getBlobs()).thenReturn(List.of(mock(Blob.class)));
    when(blobsWithCommitments.getKzgCommitments()).thenReturn(List.of(commitment));
  }

  private void assertInvalidResult(
      final ValidationResult<TransactionInvalidReason> result,
      final TransactionInvalidReason expectedReason,
      final String expectedMessage) {

    assertFalse(result.isValid());
    assertEquals(expectedReason, result.getInvalidReason());
    assertEquals(expectedMessage, result.getErrorMessage());
  }
}
