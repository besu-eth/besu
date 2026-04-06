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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.eth.transactions.layered.BaseFeePrioritizedTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.EndLayer;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredPendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.ReadyTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SparseTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.TransactionsLayer;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionPoolSaveRestoreTest extends AbstractTransactionPoolTestBase {
  @TempDir static Path tempDir;
  Path saveFilePath;

  @BeforeEach
  public void setup() {
    saveFilePath = tempDir.resolve("txpool.dump");
  }

  @AfterEach
  public void cleanup() {
    saveFilePath.toFile().delete();
  }

  @Override
  protected PendingTransactions createPendingTransactions(
      final TransactionPoolConfiguration poolConfig,
      final BiFunction<PendingTransaction, PendingTransaction, Boolean>
          transactionReplacementTester) {

    final var txPoolMetrics = new TransactionPoolMetrics(metricsSystem);
    final TransactionsLayer sparseLayer =
        new SparseTransactions(
            poolConfig,
            ethScheduler,
            new EndLayer(txPoolMetrics),
            txPoolMetrics,
            transactionReplacementTester,
            new BlobCache());
    final TransactionsLayer readyLayer =
        new ReadyTransactions(
            poolConfig,
            ethScheduler,
            sparseLayer,
            txPoolMetrics,
            transactionReplacementTester,
            new BlobCache());
    return new LayeredPendingTransactions(
        poolConfig,
        new BaseFeePrioritizedTransactions(
            poolConfig,
            protocolContext.getBlockchain()::getChainHeadHeader,
            ethScheduler,
            readyLayer,
            txPoolMetrics,
            transactionReplacementTester,
            FeeMarket.london(0L),
            new BlobCache(),
            MiningConfiguration.newDefault(),
            senderBalanceChecker),
        ethScheduler);
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

  @Override
  protected TransactionTestFixture createBaseTransaction(final int nonce) {
    return createBaseTransactionBaseFeeMarket(nonce);
  }

  @Override
  protected Transaction createTransaction(final int nonce, final Wei maxPrice) {
    return createTransactionBaseFeeMarket(nonce, maxPrice);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void localTransactionIsSavedAndRestored(final boolean noLocalPriority)
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    transactionIsSavedAndRestored(
        createTransaction(noLocalPriority ? 0 : 1), true, noLocalPriority);
  }

  @Test
  public void remoteTransactionIsSavedAndRestored()
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    transactionIsSavedAndRestored(createTransaction(2), false, true);
  }

  @Test
  public void blobTransactionIsSavedAndRestored()
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    transactionIsSavedAndRestored(createBlobTransaction(0), false, true);
  }

  private void transactionIsSavedAndRestored(
      final Transaction transaction, final boolean isLocal, final boolean noLocalPriority)
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    // create a txpool with save and restore enabled
    this.transactionPool =
        createTransactionPool(
            b ->
                b.noLocalPriority(noLocalPriority)
                    .enableSaveRestore(true)
                    .saveFile(saveFilePath.toFile()));

    givenTransactionIsValid(transaction);

    if (isLocal) {
      addAndAssertTransactionViaApiValid(transaction, noLocalPriority);
    } else {
      addAndAssertRemoteTransactionsValid(transaction);
    }

    // record blob map size before disabling
    int originalBlobMapSize = transactionPool.getBlobMapSize();

    // disabling the txpool, forces a save to file
    transactionPool.setDisabled().get(10, TimeUnit.SECONDS);

    // after being disabled the txpool must be empty
    assertThat(transactionPool.getPendingTransactions()).isEmpty();

    // after being disabled the blob map size must be zero
    assertThat(transactionPool.getBlobMapSize()).isEqualTo(0);

    final var savedContent = Files.readString(saveFilePath, StandardCharsets.US_ASCII);

    assertThat(savedContent)
        .isEqualToIgnoringNewLines(
            "127" + ((isLocal) ? "l" : "r") + transaction2Base64(transaction));

    // re-enabling the txpool restores from file
    transactionPool.setEnabled().get(10, TimeUnit.SECONDS);

    assertThat(transactionPool.getPendingTransactions()).size().isEqualTo(1);
    // after restore, blob map size must match (no leaks/duplicates)
    assertThat(transactionPool.getBlobMapSize()).isEqualTo(originalBlobMapSize);

    final var restoredPendingTx = transactionPool.getPendingTransactions().iterator().next();

    assertThat(restoredPendingTx.getTransaction()).isEqualTo(transaction);
    assertThat(restoredPendingTx.isReceivedFromLocalSource()).isEqualTo(isLocal);
    assertThat(restoredPendingTx.hasPriority()).isNotEqualTo(noLocalPriority);
  }

  @Test
  public void dumpFileWithoutScoreIsRestored() throws IOException {

    // create a save file with one local and one remote tx, both without score
    final var noScoreContent =
        """
        luFoC+FcBgIID6IITiIcf////////gASAwAGga1337/7O7cp7jaMTu9X230+6mLJciebaO5nrsgDRp1CgA5MCvzfmS4H3NqF0DIxJGl8atRTkKmFwLMZgPpkVTqQ=
        ruFoC+FcBgIID6IITiIcf////////gASAwAGglm0VMcNQmOS0aE5CJP1Lm7eBbFQIRvmwgUcfEka9sVagYWy/2d2tJHojo2smAIJgwLbud9Dr+f1lbxo1dSOBfmE=
        """;

    Files.writeString(saveFilePath, noScoreContent);

    givenAllTransactionsAreValid();

    // create a txpool with save and restore enabled
    this.transactionPool =
        createTransactionPool(b -> b.enableSaveRestore(true).saveFile(saveFilePath.toFile()));

    await().until(() -> transactionPool.getPendingTransactions().size() == 2);

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getScore)
        .allMatch(score -> score == Byte.MAX_VALUE);
    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::isReceivedFromLocalSource)
        .filteredOn(Boolean::booleanValue)
        .hasSize(1);
  }

  @Test
  public void dumpFileWithLowestScoreTxIsRestored() throws IOException {

    // create a save file with one remote tx with the lowest score
    final var noScoreContent =
        """
        -128ruFoC+FcBgIID6IITiIcf////////gASAwAGga1337/7O7cp7jaMTu9X230+6mLJciebaO5nrsgDRp1CgA5MCvzfmS4H3NqF0DIxJGl8atRTkKmFwLMZgPpkVTqQ=
        """;

    Files.writeString(saveFilePath, noScoreContent);

    givenAllTransactionsAreValid();

    // create a txpool with save and restore enabled
    this.transactionPool =
        createTransactionPool(b -> b.enableSaveRestore(true).saveFile(saveFilePath.toFile()));

    await().until(() -> transactionPool.getPendingTransactions().size() == 1);

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getScore)
        .allMatch(score -> score == Byte.MIN_VALUE);
    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::isReceivedFromLocalSource)
        .first()
        .isEqualTo(false);
  }

  private String transaction2Base64(final Transaction transaction) {
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    TransactionEncoder.encodeRLP(transaction, rlp, EncodingContext.POOLED_TRANSACTION);
    return rlp.encoded().toBase64String();
  }

  @Test
  public void diskLockTimeoutIsPropagatedNotSwallowed() throws InterruptedException {
    // Create a txpool with save and restore enabled and a short lock timeout
    // so the test does not block for 60 seconds
    this.transactionPool =
        createTransactionPool(
            b ->
                b.enableSaveRestore(true)
                    .saveFile(saveFilePath.toFile())
                    .unstable(
                        ImmutableTransactionPoolConfiguration.Unstable.builder()
                            .saveRestoreTimeout(Duration.ofMillis(100))
                            .build()));

    // Acquire the lock to simulate another operation holding it. Using acquire()
    // instead of drainPermits() ensures we wait for any in-progress async operation
    // (e.g. loadFromDisk triggered by pool creation) to release the permit first.
    final var lock = transactionPool.getSaveRestoreManager().getDiskAccessLock();
    lock.acquire();

    try {
      // Call loadFromDisk() directly on the SaveRestoreManager to test
      // serializeAndDedupOperation() in isolation, since setDisabled() wraps the
      // future with .exceptionally() which swallows the error for logging.
      // Before the fix, the failedFuture was not returned and the caller silently got
      // completedFuture(null). After the fix, the failed future must be propagated.
      final CompletableFuture<Void> result = transactionPool.getSaveRestoreManager().loadFromDisk();

      assertThatThrownBy(result::get)
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(TimeoutException.class);
    } finally {
      // Always restore the permit so cleanup does not hang
      lock.release();
    }
  }
}
