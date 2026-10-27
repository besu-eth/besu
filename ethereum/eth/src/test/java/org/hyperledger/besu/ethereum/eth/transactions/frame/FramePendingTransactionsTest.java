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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.RemovalReason;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionEventNotifier;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Pool policy, with admission stubbed so each rule can be driven directly. */
class FramePendingTransactionsTest {

  private static final Address PAYMASTER =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");
  private static final ValidationPrefix SELF_RELAY =
      new ValidationPrefix(1, -1, 0, false, OptionalLong.empty());

  private final FrameTransactionAdmission admission = mock(FrameTransactionAdmission.class);
  private final BlockHeader head =
      new BlockHeaderTestFixture()
          .number(10)
          .timestamp(1_000)
          .baseFeePerGas(Wei.of(7))
          .buildHeader();
  private final List<RemovalReason> drops = new ArrayList<>();
  private FramePendingTransactions pool;

  @BeforeEach
  void setUp() {
    pool = poolOfSize(16);
  }

  private FramePendingTransactions poolOfSize(final int size) {
    final TransactionEventNotifier notifier =
        new TransactionEventNotifier(new DeterministicEthScheduler());
    notifier.subscribePendingTransactionDropped((tx, reason) -> drops.add(reason));
    return new FramePendingTransactions(
        TransactionPoolConfiguration.DEFAULT,
        admission,
        () -> head,
        Clock.systemUTC(),
        size,
        notifier.newEmitter());
  }

  private static Address sender(final int n) {
    return Address.fromHexString(String.format("0x%040x", n));
  }

  private static Transaction tx(final Address sender, final long nonce, final long fee) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(nonce)
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
        .maxPriorityFeePerGas(Wei.of(fee))
        .maxFeePerGas(Wei.of(fee + 7))
        .maxFeePerBlobGas(Wei.ZERO)
        .build();
  }

  private void admit(
      final Transaction transaction,
      final Address payer,
      final Wei payerBalance,
      final boolean coded) {
    when(admission.admit(eq(transaction), any()))
        .thenReturn(
            new FrameTransactionAdmission.Result(
                Optional.of(
                    new FrameTransactionAdmission.Admitted(
                        SELF_RELAY,
                        payer,
                        FrameTransactionAdmission.maxCost(transaction),
                        payerBalance,
                        coded)),
                null,
                null));
  }

  private void admitSelfPaid(final Transaction transaction) {
    admit(transaction, transaction.getSender(), Wei.fromEth(100), false);
  }

  private TransactionAddedResult add(final Transaction transaction) {
    return pool.addTransaction(
        PendingTransaction.newPendingTransaction(transaction, false, false, (byte) 0),
        Optional.empty());
  }

  @Test
  void addsAnAdmittedTransaction() {
    final Transaction transaction = tx(sender(1), 0, 2);
    admitSelfPaid(transaction);
    assertThat(add(transaction)).isEqualTo(TransactionAddedResult.ADDED);
    assertThat(pool.size()).isEqualTo(1);
    assertThat(pool.getTransactionByHash(transaction.getHash())).contains(transaction);
    assertThat(add(transaction)).isEqualTo(TransactionAddedResult.ALREADY_KNOWN);
  }

  @Test
  void passesOnTheAdmissionRejection() {
    final Transaction transaction = tx(sender(1), 0, 2);
    when(admission.admit(eq(transaction), any()))
        .thenReturn(
            FrameTransactionAdmission.Result.reject(
                TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION, "banned opcode"));
    assertThat(add(transaction).maybeInvalidReason())
        .contains(TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION);
    assertThat(pool.size()).isZero();
  }

  @Test
  void keepsOneTransactionPerSender() {
    final Transaction first = tx(sender(1), 0, 2);
    final Transaction otherNonce = tx(sender(1), 1, 50);
    admitSelfPaid(first);
    admitSelfPaid(otherNonce);
    add(first);
    assertThat(add(otherNonce).maybeInvalidReason())
        .contains(TransactionInvalidReason.FRAME_SENDER_ALREADY_PENDING);
  }

  @Test
  void replacesOnlyWithBothFeesBumped() {
    final Transaction original = tx(sender(1), 0, 10); // priority 10, max fee 17
    final Transaction priorityNotBumped = withFees(original, 10, 19);
    final Transaction maxFeeNotBumped = withFees(original, 11, 18);
    final Transaction replacement = withFees(original, 11, 19); // both raised by 10%
    admitSelfPaid(original);
    admitSelfPaid(priorityNotBumped);
    admitSelfPaid(maxFeeNotBumped);
    admitSelfPaid(replacement);
    add(original);

    assertThat(add(priorityNotBumped))
        .isEqualTo(TransactionAddedResult.REJECTED_UNDERPRICED_REPLACEMENT);
    assertThat(add(maxFeeNotBumped))
        .isEqualTo(TransactionAddedResult.REJECTED_UNDERPRICED_REPLACEMENT);
    assertThat(add(replacement).isReplacement()).isTrue();
    assertThat(pool.getTransactionByHash(original.getHash())).isEmpty();
    assertThat(pool.getTransactionByHash(replacement.getHash())).contains(replacement);
    assertThat(drops).containsExactly(FrameRemovalReason.REPLACED);
  }

  private static Transaction withFees(
      final Transaction transaction, final long priorityFee, final long maxFee) {
    return Transaction.builder()
        .copiedFrom(transaction)
        .maxPriorityFeePerGas(Wei.of(priorityFee))
        .maxFeePerGas(Wei.of(maxFee))
        .build();
  }

  @Test
  void boundsAPayersExposureByItsBalance() {
    final Transaction first = tx(sender(1), 0, 2);
    final Transaction second = tx(sender(2), 0, 2);
    final Wei balance = FrameTransactionAdmission.maxCost(first); // covers exactly one
    admit(first, PAYMASTER, balance, false);
    admit(second, PAYMASTER, balance, false);
    assertThat(add(first)).isEqualTo(TransactionAddedResult.ADDED);
    assertThat(add(second).maybeInvalidReason())
        .contains(TransactionInvalidReason.FRAME_PAYER_EXPOSURE_EXCEEDED);
  }

  @Test
  void limitsACodedPaymasterToOnePendingTransaction() {
    final Transaction first = tx(sender(1), 0, 2);
    final Transaction second = tx(sender(2), 0, 2);
    admit(first, PAYMASTER, Wei.fromEth(100), true);
    admit(second, PAYMASTER, Wei.fromEth(100), true);
    assertThat(add(first)).isEqualTo(TransactionAddedResult.ADDED);
    assertThat(add(second).maybeInvalidReason())
        .contains(TransactionInvalidReason.FRAME_PAYMASTER_PENDING_LIMIT);
  }

  @Test
  void releasesTheReservationOfAConfirmedTransaction() {
    final Transaction first = tx(sender(1), 0, 2);
    final Transaction second = tx(sender(2), 0, 2);
    final Wei balance = FrameTransactionAdmission.maxCost(first);
    admit(first, PAYMASTER, balance, false);
    admit(second, PAYMASTER, balance, false);
    add(first);

    pool.manageBlockAdded(head, List.of(first), List.of(), null);
    assertThat(pool.size()).isZero();
    assertThat(drops).containsExactly(FrameRemovalReason.CONFIRMED);
    assertThat(add(second)).isEqualTo(TransactionAddedResult.ADDED);
  }

  @Test
  void dropsTransactionsNoLongerAdmissibleAtTheNewHead() {
    final Transaction transaction = tx(sender(1), 0, 2);
    admitSelfPaid(transaction);
    add(transaction);
    when(admission.admit(eq(transaction), any()))
        .thenReturn(
            FrameTransactionAdmission.Result.reject(
                TransactionInvalidReason.NONCE_TOO_LOW, "nonce consumed"));

    pool.manageBlockAdded(head, List.of(), List.of(), null);
    assertThat(pool.size()).isZero();
    assertThat(drops).containsExactly(FrameRemovalReason.INVALIDATED);
  }

  @Test
  void evictsTheLowestPayingTransactionWhenFull() {
    pool = poolOfSize(1);
    final Transaction low = tx(sender(1), 0, 2);
    final Transaction lower = tx(sender(2), 0, 1);
    final Transaction higher = tx(sender(3), 0, 5);
    admitSelfPaid(low);
    admitSelfPaid(lower);
    admitSelfPaid(higher);
    add(low);

    assertThat(add(lower).isSuccess()).isFalse();
    assertThat(add(higher)).isEqualTo(TransactionAddedResult.ADDED);
    assertThat(pool.getTransactionByHash(low.getHash())).isEmpty();
    assertThat(drops).containsExactly(FrameRemovalReason.EVICTED);
  }

  @Test
  void offersCandidatesByPriorityFeeAndBoundsUnpaidBuildAttempts() {
    final Transaction cheap = tx(sender(1), 0, 1);
    final Transaction rich = tx(sender(2), 0, 9);
    admitSelfPaid(cheap);
    admitSelfPaid(rich);
    add(cheap);
    add(rich);

    final List<PendingTransaction> candidates = pool.selectionCandidates();
    assertThat(candidates)
        .extracting(PendingTransaction::getTransaction)
        .containsExactly(rich, cheap);

    final PendingTransaction penalized = candidates.get(1);
    for (int attempt = 1; attempt < FramePendingTransactions.MAX_UNPAID_BUILD_ATTEMPTS; attempt++) {
      pool.applySelectionResults(
          Map.of(penalized, TransactionSelectionResult.invalidPenalized("prefix did not approve")));
      assertThat(pool.containsTransaction(cheap)).isTrue();
    }
    pool.applySelectionResults(
        Map.of(penalized, TransactionSelectionResult.invalidPenalized("prefix did not approve")));
    assertThat(pool.containsTransaction(cheap)).isFalse();

    pool.applySelectionResults(
        Map.of(candidates.get(0), TransactionSelectionResult.invalid("nonce too low")));
    assertThat(pool.size()).isZero();
  }
}
