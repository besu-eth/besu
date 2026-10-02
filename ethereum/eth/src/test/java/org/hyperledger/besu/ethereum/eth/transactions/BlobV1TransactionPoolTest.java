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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.Cell;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.mainnet.transactionpool.OsakaTransactionPoolPreProcessor;
import org.hyperledger.besu.testutil.TestClock;

import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes48;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class BlobV1TransactionPoolTest extends AbstractTransactionPoolTestBase {

  @Override
  protected PendingTransactions createPendingTransactions(
      final TransactionPoolConfiguration poolConfig,
      final BiFunction<PendingTransaction, PendingTransaction, Boolean>
          transactionReplacementTester) {

    return new BaseFeePendingTransactionsSorter(
        poolConfig,
        TestClock.system(ZoneId.systemDefault()),
        metricsSystem,
        protocolContext.getBlockchain()::getChainHeadHeader);
  }

  @Override
  protected Transaction createTransaction(final int transactionNumber, final Wei maxPrice) {
    return createTransactionBaseFeeMarket(transactionNumber, maxPrice);
  }

  @Override
  protected TransactionTestFixture createBaseTransaction(final int transactionNumber) {
    return createBaseTransactionBaseFeeMarket(transactionNumber);
  }

  @Override
  protected ExecutionContextTestFixture createExecutionContextTestFixture() {
    return createExecutionContextTestFixtureBaseFeeMarket();
  }

  @Override
  protected FeeMarket getFeeMarket() {
    return FeeMarket.london(0L, Optional.of(BASE_FEE_FLOOR));
  }

  @Override
  protected Block appendBlock(
      final Difficulty difficulty,
      final BlockHeader parentBlock,
      final Transaction... transactionsToAdd) {
    return appendBlockBaseFeeMarket(difficulty, parentBlock, transactionsToAdd);
  }

  @Test
  public void shouldReturnBlobWhenTransactionAddedToPool() {
    givenTransactionIsValid(transactionWithBlobs);

    addAndAssertRemoteTransactionsValid(transactionWithBlobs);

    assertTransactionPending(transactionWithBlobs);
    // assert that the blobs are returned from the tx pool
    final List<BlobProofBundle> expectedBlobProofBundles =
        transactionWithBlobs.getBlobsWithCommitments().get().getBlobProofBundles();

    expectedBlobProofBundles.forEach(
        bq -> assertThat(transactionPool.getBlobProofBundle(bq.getVersionedHash())).isEqualTo(bq));
  }

  @Test
  public void shouldNotReturnBlobsWhenAllTxsContainingBlobsHaveBeenReplaced() {
    givenTransactionIsValid(transactionWithBlobs);
    givenTransactionIsValid(transactionWithBlobsReplacement);
    givenTransactionIsValid(transactionWithSameBlobs); // contains same blobs as transactionBlob
    givenTransactionIsValid(transactionWithSameBlobsReplacement);

    addAndAssertRemoteTransactionsValid(transactionWithBlobs);
    assertTransactionPending(transactionWithBlobs);

    final List<BlobProofBundle> expectedBlobProofBundles =
        transactionWithBlobs.getBlobsWithCommitments().get().getBlobProofBundles();

    // assert that the blobs are returned from the tx pool
    expectedBlobProofBundles.forEach(
        bq -> assertThat(transactionPool.getBlobProofBundle(bq.getVersionedHash())).isEqualTo(bq));

    // add different transaction that contains the same blobs
    addAndAssertRemoteTransactionsValid(transactionWithSameBlobs);

    assertTransactionPending(transactionWithBlobs);
    assertTransactionPending(transactionWithSameBlobs);
    // assert that the blobs are still returned from the tx pool
    expectedBlobProofBundles.forEach(
        bq -> assertThat(transactionPool.getBlobProofBundle(bq.getVersionedHash())).isEqualTo(bq));

    // replace the second blob transaction with tx with different blobs
    addAndAssertRemoteTransactionsValid(transactionWithSameBlobsReplacement);
    assertTransactionPending(transactionWithSameBlobsReplacement);
    assertTransactionNotPending(transactionWithSameBlobs);

    // assert that the blob is still returned from the tx pool
    expectedBlobProofBundles.forEach(
        bq -> assertThat(transactionPool.getBlobProofBundle(bq.getVersionedHash())).isEqualTo(bq));

    // replace the first blob transaction with tx with different blobs
    addAndAssertRemoteTransactionsValid(transactionWithBlobsReplacement);
    assertTransactionPending(transactionWithBlobsReplacement);
    assertTransactionNotPending(transactionWithBlobs);

    // All txs containing the expected blobs have been replaced,
    // so the blobs should no longer be returned from the tx pool
    expectedBlobProofBundles.forEach(
        bq -> assertThat(transactionPool.getBlobProofBundle(bq.getVersionedHash())).isNull());
  }

  @Test
  public void shouldNotReturnABundleWhoseBlobIsNotHeld() {
    // What a transaction received over eth/72 looks like in the pool: the bundle is there, the blob
    // is not. engine_getBlobsV1/V2/V3 serve blobs, so for them this is as good as absent.
    final BlobProofBundle full = fullCellProofBundle();
    final Transaction cellsOnly = blobTransactionHolding(2, full, LOWER_HALF);

    givenTransactionIsValid(cellsOnly);
    addAndAssertRemoteTransactionsValid(cellsOnly);

    assertThat(transactionPool.getBlobProofBundle(full.getVersionedHash())).isNull();
  }

  @Test
  public void shouldReturnTheBundleHoldingEveryRequestedCell() {
    final BlobProofBundle full = fullCellProofBundle();
    final Transaction cellsOnly = blobTransactionHolding(2, full, LOWER_HALF);

    givenTransactionIsValid(cellsOnly);
    addAndAssertRemoteTransactionsValid(cellsOnly);

    final BlobProofBundle found =
        transactionPool.getBlobProofBundle(full.getVersionedHash(), List.of(0, 63));

    assertThat(found).isNotNull();
    assertThat(found.getCellsWithMask().orElseThrow().getCellMask()).isEqualTo(LOWER_HALF);
  }

  @Test
  public void shouldMergeCellsHeldByDifferentTransactionsOfTheSameBlob() {
    // The same blob can be carried by several transactions, each holding a different part of its
    // cells, so answering for one cell index is no reason to stop looking for the next.
    final BlobProofBundle full = fullCellProofBundle();
    final Transaction lowerHalf = blobTransactionHolding(2, full, LOWER_HALF);
    final Transaction upperHalf = blobTransactionHolding(3, full, UPPER_HALF);

    givenTransactionIsValid(lowerHalf);
    givenTransactionIsValid(upperHalf);
    addAndAssertRemoteTransactionsValid(lowerHalf, upperHalf);

    final BlobProofBundle merged =
        transactionPool.getBlobProofBundle(full.getVersionedHash(), List.of(0, 127));

    assertThat(merged).isNotNull();
    final CellsWithMask mergedCells = merged.getCellsWithMask().orElseThrow();
    assertThat(mergedCells.getCellMask()).isEqualTo(CellMask.FULL);
    final CellsWithMask allCells = full.getCellsWithMask().orElseThrow();
    assertThat(mergedCells.getCell(0)).isEqualTo(allCells.getCell(0));
    assertThat(mergedCells.getCell(127)).isEqualTo(allCells.getCell(127));
  }

  @Test
  public void shouldTakeEachMergedProofFromTheTransactionHoldingItsCell() {
    // A transaction holding only some cells carries every proof, but only those of its cells have
    // been verified, so a merged bundle must not pair a cell with another transaction's proof.
    final BlobProofBundle full = fullCellProofBundle();
    final List<KZGProof> fullProofs = full.getKzgProof();
    final Transaction lowerHalf =
        blobTransactionHolding(
            2,
            full,
            LOWER_HALF,
            proofsVerifiedOnlyWhereHeld(fullProofs, LOWER_HALF, unverifiedProof(0xAA)));
    final Transaction upperHalf =
        blobTransactionHolding(
            3,
            full,
            UPPER_HALF,
            proofsVerifiedOnlyWhereHeld(fullProofs, UPPER_HALF, unverifiedProof(0xBB)));

    givenTransactionIsValid(lowerHalf);
    givenTransactionIsValid(upperHalf);
    addAndAssertRemoteTransactionsValid(lowerHalf, upperHalf);

    final BlobProofBundle merged =
        transactionPool.getBlobProofBundle(full.getVersionedHash(), List.of(0, 127));

    assertThat(merged).isNotNull();
    assertThat(merged.getCellsWithMask().orElseThrow().getCellMask()).isEqualTo(CellMask.FULL);
    assertThat(merged.getKzgProof()).containsExactlyElementsOf(fullProofs);
  }

  @Test
  public void shouldReturnTheHeldCellsWhenSomeRequestedCellsAreNotHeld() {
    // engine_getBlobsV4 answers a cell it cannot serve with a null in place, so one missing cell is
    // no reason to withhold the others.
    final BlobProofBundle full = fullCellProofBundle();
    final Transaction lowerHalf = blobTransactionHolding(2, full, LOWER_HALF);

    givenTransactionIsValid(lowerHalf);
    addAndAssertRemoteTransactionsValid(lowerHalf);

    // 0 is held, 127 is not
    final BlobProofBundle found =
        transactionPool.getBlobProofBundle(full.getVersionedHash(), List.of(0, 127));

    assertThat(found).isNotNull();
    final CellsWithMask foundCells = found.getCellsWithMask().orElseThrow();
    assertThat(foundCells.hasCell(0)).isTrue();
    assertThat(foundCells.hasCell(127)).isFalse();
  }

  @Test
  public void shouldNotReturnABundleWhenNoneOfTheRequestedCellsIsHeld() {
    final BlobProofBundle full = fullCellProofBundle();
    final Transaction lowerHalf = blobTransactionHolding(2, full, LOWER_HALF);

    givenTransactionIsValid(lowerHalf);
    addAndAssertRemoteTransactionsValid(lowerHalf);

    // neither is held, which is no different from not knowing the blob
    assertThat(transactionPool.getBlobProofBundle(full.getVersionedHash(), List.of(64, 127)))
        .isNull();
  }

  @Test
  public void shouldNotReturnABundleForAnUnknownHashOrAnEmptyCellRequest() {
    final BlobProofBundle full = fullCellProofBundle();
    final Transaction cellsOnly = blobTransactionHolding(2, full, LOWER_HALF);

    givenTransactionIsValid(cellsOnly);
    addAndAssertRemoteTransactionsValid(cellsOnly);

    final VersionedHash unknown = new VersionedHash((byte) 1, Hash.ZERO);
    assertThat(transactionPool.getBlobProofBundle(unknown, List.of(0))).isNull();
    assertThat(transactionPool.getBlobProofBundle(unknown)).isNull();
    assertThat(transactionPool.getBlobProofBundle(full.getVersionedHash(), List.of())).isNull();
  }

  /** Cell indexes 0 to 63. */
  private static final CellMask LOWER_HALF = halfMask(0xFF, 0x00);

  /** Cell indexes 64 to 127. */
  private static final CellMask UPPER_HALF = halfMask(0x00, 0xFF);

  private static CellMask halfMask(final int lowBytes, final int highBytes) {
    return CellMask.fromBytes(
        Bytes.concatenate(
            Bytes.repeat((byte) lowBytes, CellMask.BYTE_LENGTH / 2),
            Bytes.repeat((byte) highBytes, CellMask.BYTE_LENGTH / 2)));
  }

  private static BlobProofBundle fullCellProofBundle() {
    return new BlobTestFixture().createBlobProofBundle(BlobType.KZG_CELL_PROOFS);
  }

  /** A transaction carrying one blob of which it holds only the cells of {@code mask}. */
  private Transaction blobTransactionHolding(
      final int nonce, final BlobProofBundle full, final CellMask mask) {
    return blobTransactionHolding(nonce, full, mask, full.getKzgProof());
  }

  private Transaction blobTransactionHolding(
      final int nonce,
      final BlobProofBundle full,
      final CellMask mask,
      final List<KZGProof> proofs) {
    final CellsWithMask allCells = full.getCellsWithMask().orElseThrow();
    final List<Cell> held = mask.streamIndexes().mapToObj(allCells::getCell).toList();
    return createBlobTransactionWithSameBlobs(
        nonce,
        BlobsWithCommitments.createFromBlobCells(
            List.of(full.getKzgCommitment()),
            List.of(new CellsWithMask(held, mask)),
            proofs,
            List.of(full.getVersionedHash())));
  }

  /** A proof no real cell has, so it shows wherever it leaks into a result. */
  private static KZGProof unverifiedProof(final int fill) {
    return new KZGProof(Bytes48.wrap(Bytes.repeat((byte) fill, 48)));
  }

  /**
   * The proofs a peer holding the cells of {@code mask} could send: right where it holds the cell,
   * and {@code wrongProof} elsewhere, since nothing checks those.
   */
  private static List<KZGProof> proofsVerifiedOnlyWhereHeld(
      final List<KZGProof> fullProofs, final CellMask mask, final KZGProof wrongProof) {
    return IntStream.range(0, fullProofs.size())
        .mapToObj(i -> mask.contains(i) ? fullProofs.get(i) : wrongProof)
        .toList();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBroadcastPooledRepresentationOfLocallySubmittedV0BlobTransaction() {
    // From Osaka on, a locally submitted blob transaction carrying a version 0 (blob proof) wrapper
    // is upgraded to a version 1 (cell proofs, EIP-7594) wrapper before it is pooled. The pooled
    // form is what GetPooledTransactions serves, so it is also what has to be broadcast and
    // announced: announcing the pre-upgrade size makes every peer that checks the announced size
    // against the delivered transaction (e.g. go-ethereum) treat us as a protocol violator.
    when(protocolSpec.getTransactionPoolPreProcessor())
        .thenReturn(Optional.of(new OsakaTransactionPoolPreProcessor()));

    givenTransactionIsValid(transactionWithBlobs);
    assertThat(transactionWithBlobs.getBlobsWithCommitments().orElseThrow().getBlobType())
        .isEqualTo(BlobType.KZG_PROOF);

    assertThat(transactionPool.addTransactionViaApi(transactionWithBlobs).isValid()).isTrue();

    final ArgumentCaptor<Collection<Transaction>> broadcast = forClass(Collection.class);
    verify(transactionBroadcaster).onTransactionsAdded(broadcast.capture());
    final Transaction broadcastTx = broadcast.getValue().iterator().next();

    final Transaction pooledTx =
        transactionPool.getTransactionByHash(transactionWithBlobs.getHash()).orElseThrow();
    assertThat(pooledTx.getBlobsWithCommitments().orElseThrow().getBlobType())
        .isEqualTo(BlobType.KZG_CELL_PROOFS);

    // the broadcast transaction must be the pooled one, so that the announced size matches what we
    // will serve
    assertThat(broadcastTx.getBlobsWithCommitments().orElseThrow().getBlobType())
        .isEqualTo(BlobType.KZG_CELL_PROOFS);
    assertThat(broadcastTx.getSizeForAnnouncement())
        .isEqualTo(pooledTx.getSizeForAnnouncement())
        .isNotEqualTo(transactionWithBlobs.getSizeForAnnouncement());
  }
}
