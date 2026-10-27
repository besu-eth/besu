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
package org.hyperledger.besu.ethereum.eth.transactions.frame;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredPendingTransactions;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/** The facade over the layered and frame pools, with both pools mocked. */
class CompositePendingTransactionsTest {

  private static final SignatureAlgorithm ALGO = SignatureAlgorithmFactory.getInstance();
  private static final Wei BASE_FEE = Wei.of(7);

  private final LayeredPendingTransactions layered = mock(LayeredPendingTransactions.class);
  private final FramePendingTransactions frames = mock(FramePendingTransactions.class);
  private final BlockHeader head =
      new BlockHeaderTestFixture().number(10).baseFeePerGas(BASE_FEE).buildHeader();
  private final CompositePendingTransactions composite =
      new CompositePendingTransactions(layered, frames, () -> head);

  private static Address sender(final int n) {
    return Address.fromHexString(String.format("0x%040x", n));
  }

  /** An EIP-1559 transaction whose effective priority fee at the head is {@code priorityFee}. */
  private static PendingTransaction layeredTx(final long priorityFee) {
    return layeredTx(ALGO.generateKeyPair(), 0, priorityFee);
  }

  private static PendingTransaction layeredTx(
      final KeyPair keys, final long nonce, final long priorityFee) {
    return pending(
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .nonce(nonce)
            .maxPriorityFeePerGas(Optional.of(Wei.of(priorityFee)))
            .maxFeePerGas(Optional.of(BASE_FEE.add(Wei.of(priorityFee))))
            .createTransaction(keys));
  }

  /** A frame transaction whose effective priority fee at the head is {@code priorityFee}. */
  private static PendingTransaction frameTx(final Address sender, final long priorityFee) {
    return pending(
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonce(0)
            .sender(sender)
            .frames(
                List.of(
                    new Frame(
                        Frame.MODE_VERIFY,
                        Frame.APPROVE_SCOPE_MASK,
                        Optional.empty(),
                        50_000L,
                        0L,
                        Wei.ZERO,
                        Bytes.EMPTY)))
            .frameSignatures(List.of())
            .maxPriorityFeePerGas(Wei.of(priorityFee))
            .maxFeePerGas(BASE_FEE.add(Wei.of(priorityFee)))
            .maxFeePerBlobGas(Wei.ZERO)
            .build());
  }

  private static PendingTransaction pending(final Transaction transaction) {
    return PendingTransaction.newPendingTransaction(transaction, false, false, (byte) 0);
  }

  @Test
  void holdsFrameTransactions() {
    assertThat(composite.supportsFrameTransactions()).isTrue();
  }

  @Test
  void routesEachTransactionToItsPool() {
    final PendingTransaction frameTx = frameTx(sender(1), 2);
    final PendingTransaction layeredTx = layeredTx(2);
    when(frames.addTransaction(frameTx, Optional.empty())).thenReturn(TransactionAddedResult.ADDED);
    when(layered.addTransaction(layeredTx, Optional.empty()))
        .thenReturn(TransactionAddedResult.ALREADY_KNOWN);
    when(frames.containsTransaction(frameTx.getTransaction())).thenReturn(true);
    when(frames.restoreBlob(frameTx.getTransaction())).thenReturn(Optional.empty());
    when(layered.restoreBlob(layeredTx.getTransaction()))
        .thenReturn(Optional.of(layeredTx.getTransaction()));

    assertThat(composite.addTransaction(frameTx, Optional.empty()))
        .isEqualTo(TransactionAddedResult.ADDED);
    assertThat(composite.addTransaction(layeredTx, Optional.empty()))
        .isEqualTo(TransactionAddedResult.ALREADY_KNOWN);
    verify(layered, never()).addTransaction(frameTx, Optional.empty());
    verify(frames, never()).addTransaction(layeredTx, Optional.empty());

    assertThat(composite.containsTransaction(frameTx.getTransaction())).isTrue();
    assertThat(composite.containsTransaction(layeredTx.getTransaction())).isFalse();
    verify(layered, never()).containsTransaction(frameTx.getTransaction());
    verify(frames, never()).containsTransaction(layeredTx.getTransaction());

    assertThat(composite.restoreBlob(frameTx.getTransaction())).isEmpty();
    assertThat(composite.restoreBlob(layeredTx.getTransaction()))
        .contains(layeredTx.getTransaction());
  }

  @Test
  void mergesFrameCandidatesIntoTheLayeredOrderByEffectivePriorityFee() {
    // Layered candidates come by score, which need not be by fee: their order is kept as is.
    final PendingTransaction layered10 = layeredTx(10);
    final PendingTransaction layered3 = layeredTx(3);
    final PendingTransaction layered5 = layeredTx(5);
    final PendingTransaction frame8 = frameTx(sender(1), 8);
    final PendingTransaction frame4 = frameTx(sender(2), 4);
    final PendingTransaction frame1 = frameTx(sender(3), 1);
    when(layered.selectionCandidates()).thenReturn(List.of(layered10, layered3, layered5));
    when(frames.selectionCandidates()).thenReturn(List.of(frame8, frame4, frame1));

    final List<List<PendingTransaction>> offered = new ArrayList<>();
    composite.selectTransactions(
        candidates -> {
          offered.add(candidates);
          return Map.of();
        });

    assertThat(offered).hasSize(1);
    assertThat(offered.getFirst())
        .containsExactly(layered10, frame8, frame4, layered3, layered5, frame1);
  }

  @Test
  void offersTheLayeredCandidatesUnchangedWithoutFrameCandidates() {
    final PendingTransaction first = layeredTx(3);
    final PendingTransaction second = layeredTx(5);
    when(layered.selectionCandidates()).thenReturn(List.of(first, second));
    when(frames.selectionCandidates()).thenReturn(List.of());

    final List<PendingTransaction> offered = new ArrayList<>();
    composite.selectTransactions(
        candidates -> {
          offered.addAll(candidates);
          return Map.of();
        });

    assertThat(offered).containsExactly(first, second);
    verify(layered).applySelectionResults(Map.of());
    verify(frames).applySelectionResults(Map.of());
  }

  @Test
  void splitsTheSelectionResultsBackToEachPool() {
    final PendingTransaction layeredSelected = layeredTx(9);
    final PendingTransaction layeredInvalid = layeredTx(2);
    final PendingTransaction frameSelected = frameTx(sender(1), 5);
    final PendingTransaction framePenalized = frameTx(sender(2), 1);
    when(layered.selectionCandidates()).thenReturn(List.of(layeredSelected, layeredInvalid));
    when(frames.selectionCandidates()).thenReturn(List.of(frameSelected, framePenalized));
    final Map<PendingTransaction, TransactionSelectionResult> results =
        Map.of(
            layeredSelected,
            TransactionSelectionResult.SELECTED,
            layeredInvalid,
            TransactionSelectionResult.invalid("UPFRONT_GAS_COST_EXCEEDS_BALANCE"),
            frameSelected,
            TransactionSelectionResult.SELECTED,
            framePenalized,
            TransactionSelectionResult.invalidPenalized("prefix did not approve"));

    final PendingTransactions.PendingTransactionsSelector selector =
        candidates ->
            candidates.stream().collect(Collectors.toMap(Function.identity(), results::get));
    composite.selectTransactions(selector);

    verify(layered)
        .applySelectionResults(
            Map.of(
                layeredSelected, results.get(layeredSelected),
                layeredInvalid, results.get(layeredInvalid)));
    verify(frames)
        .applySelectionResults(
            Map.of(
                frameSelected, results.get(frameSelected),
                framePenalized, results.get(framePenalized)));
    verify(layered, never()).selectTransactions(any());
    verify(frames, never()).selectTransactions(any());
  }

  @Test
  void splitsConfirmedAndReorgedTransactionsBetweenThePools() {
    final Transaction layeredConfirmed = layeredTx(1).getTransaction();
    final Transaction layeredReorged = layeredTx(1).getTransaction();
    final Transaction frameConfirmed = frameTx(sender(1), 1).getTransaction();
    final Transaction frameReorged = frameTx(sender(2), 1).getTransaction();
    final FeeMarket feeMarket = FeeMarket.london(0L);

    composite.manageBlockAdded(
        head,
        List.of(layeredConfirmed, frameConfirmed),
        List.of(frameReorged, layeredReorged),
        feeMarket);

    verify(layered)
        .manageBlockAdded(head, List.of(layeredConfirmed), List.of(layeredReorged), feeMarket);
    verify(frames)
        .manageBlockAdded(head, List.of(frameConfirmed), List.of(frameReorged), feeMarket);
  }

  @Test
  void resetsAndEvictsBothPools() {
    composite.reset();
    composite.evictOldTransactions();

    verify(layered).reset();
    verify(frames).reset();
    verify(layered).evictOldTransactions();
    verify(frames).evictOldTransactions();
  }

  @Test
  void sumsSizesAndCountsFrameTransactionsAsPending() {
    when(layered.size()).thenReturn(3);
    when(frames.size()).thenReturn(2);
    when(layered.getStatus()).thenReturn(new PendingTransactions.Status(2, 1));
    when(layered.maxSize()).thenReturn(4096L);
    when(frames.maxSize()).thenReturn(1024L);

    assertThat(composite.size()).isEqualTo(5);
    assertThat(composite.getStatus()).isEqualTo(new PendingTransactions.Status(4, 1));
    assertThat(composite.maxSize()).isEqualTo(4096L);
  }

  @Test
  void joinsTheListingsOfBothPools() {
    final PendingTransaction layeredLocal = layeredTx(1);
    final PendingTransaction frameLocal = frameTx(sender(1), 1);
    when(layered.getPendingTransactions()).thenReturn(List.of(layeredLocal));
    when(frames.getPendingTransactions()).thenReturn(List.of(frameLocal));
    when(layered.getLocalTransactions()).thenReturn(List.of(layeredLocal.getTransaction()));
    when(frames.getLocalTransactions()).thenReturn(List.of(frameLocal.getTransaction()));
    when(layered.getPriorityTransactions()).thenReturn(List.of());
    when(frames.getPriorityTransactions()).thenReturn(List.of(frameLocal.getTransaction()));
    when(layered.logStats()).thenReturn("Basefee Prioritized: 1");
    when(frames.logStats()).thenReturn("Frame 1/1024");
    when(layered.toTraceLog()).thenReturn("layered");
    when(frames.toTraceLog()).thenReturn("frames");

    assertThat(composite.getPendingTransactions()).containsExactly(layeredLocal, frameLocal);
    assertThat(composite.getLocalTransactions())
        .containsExactly(layeredLocal.getTransaction(), frameLocal.getTransaction());
    assertThat(composite.getPriorityTransactions()).containsExactly(frameLocal.getTransaction());
    assertThat(composite.logStats()).isEqualTo("Basefee Prioritized: 1 | Frame 1/1024");
    assertThat(composite.toTraceLog()).isEqualTo("layered | frames");
  }

  @Test
  void looksUpATransactionInTheLayeredPoolFirst() {
    final PendingTransaction layeredTx = layeredTx(1);
    final PendingTransaction frameTx = frameTx(sender(1), 1);
    when(layered.getTransactionByHash(any())).thenReturn(Optional.empty());
    when(layered.getTransactionByHash(layeredTx.getHash()))
        .thenReturn(Optional.of(layeredTx.getTransaction()));
    when(frames.getTransactionByHash(any())).thenReturn(Optional.empty());
    when(frames.getTransactionByHash(frameTx.getHash()))
        .thenReturn(Optional.of(frameTx.getTransaction()));

    assertThat(composite.getTransactionByHash(layeredTx.getHash()))
        .contains(layeredTx.getTransaction());
    assertThat(composite.getTransactionByHash(frameTx.getHash()))
        .contains(frameTx.getTransaction());
    verify(frames, never()).getTransactionByHash(layeredTx.getHash());
  }

  @Test
  void mergesTheTransactionsOfASenderPendingInBothPools() {
    final KeyPair keys = ALGO.generateKeyPair();
    final PendingTransaction layeredTx = layeredTx(keys, 4, 1);
    final Address both = layeredTx.getSender();
    final PendingTransaction frameTx = frameTx(both, 1);
    final Address layeredOnly = sender(1);
    final Address framesOnly = sender(2);
    final PendingTransaction framesOnlyTx = frameTx(framesOnly, 1);
    final SenderPendingTransactionsData layeredData =
        new SenderPendingTransactionsData(both, 5, List.of(layeredTx));
    final SenderPendingTransactionsData layeredOnlyData =
        new SenderPendingTransactionsData(layeredOnly, 3, List.of(layeredTx(2)));
    final SenderPendingTransactionsData framesData =
        new SenderPendingTransactionsData(both, 0, List.of(frameTx));
    final SenderPendingTransactionsData framesOnlyData =
        new SenderPendingTransactionsData(framesOnly, 0, List.of(framesOnlyTx));
    stubSender(both, layeredData, framesData);
    stubSender(layeredOnly, layeredOnlyData, SenderPendingTransactionsData.empty(layeredOnly));
    stubSender(framesOnly, SenderPendingTransactionsData.empty(framesOnly), framesOnlyData);
    final Map<Address, SenderPendingTransactionsData> layeredBySender = new HashMap<>();
    layeredBySender.put(both, layeredData);
    layeredBySender.put(layeredOnly, layeredOnlyData);
    when(layered.getPendingTransactionsBySender()).thenReturn(layeredBySender);
    when(frames.getPendingTransactionsBySender())
        .thenReturn(Map.of(both, framesData, framesOnly, framesOnlyData));

    final SenderPendingTransactionsData merged = composite.getPendingTransactionsFor(both);
    assertThat(merged.sender()).isEqualTo(both);
    assertThat(merged.nonce()).isEqualTo(5);
    assertThat(merged.pendingTransactions()).containsExactly(layeredTx, frameTx);
    assertThat(composite.getPendingTransactionsFor(layeredOnly)).isEqualTo(layeredOnlyData);
    assertThat(composite.getPendingTransactionsFor(framesOnly)).isEqualTo(framesOnlyData);

    final Map<Address, SenderPendingTransactionsData> bySender =
        composite.getPendingTransactionsBySender();
    assertThat(bySender).containsOnlyKeys(both, layeredOnly, framesOnly);
    assertThat(bySender.get(both).pendingTransactions()).containsExactly(layeredTx, frameTx);
    assertThat(bySender.get(layeredOnly)).isEqualTo(layeredOnlyData);
    assertThat(bySender.get(framesOnly)).isEqualTo(framesOnlyData);
  }

  private void stubSender(
      final Address sender,
      final SenderPendingTransactionsData fromLayered,
      final SenderPendingTransactionsData fromFrames) {
    when(layered.getPendingTransactionsFor(sender)).thenReturn(fromLayered);
    when(frames.getPendingTransactionsFor(sender)).thenReturn(fromFrames);
  }

  @Test
  void takesTheNextNonceFromTheLayeredPoolOnly() {
    final Address sender = sender(1);
    when(layered.getNextNonceForSender(sender)).thenReturn(OptionalLong.of(7));

    assertThat(composite.getNextNonceForSender(sender)).hasValue(7);
    verify(frames, never()).getNextNonceForSender(sender);
  }
}
