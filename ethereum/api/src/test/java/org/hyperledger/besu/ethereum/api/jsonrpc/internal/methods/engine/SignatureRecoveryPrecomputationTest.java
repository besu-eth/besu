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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SignatureRecoveryPrecomputationTest {

  private final EthScheduler scheduler = mock(EthScheduler.class);
  private final List<Supplier<?>> scheduledTasks = new ArrayList<>();

  @BeforeEach
  void captureScheduledTasks() {
    when(scheduler.scheduleComputationTask(any()))
        .thenAnswer(
            invocation -> {
              scheduledTasks.add(invocation.getArgument(0));
              return new CompletableFuture<>();
            });
  }

  @Test
  void schedulesAtMostMaxWorkersTasksWhateverTheNumberOfTransactions() {
    SignatureRecoveryPrecomputation.start(scheduler, transactions(10_000));
    assertThat(scheduledTasks).hasSize(SignatureRecoveryPrecomputation.MAX_WORKERS);

    scheduledTasks.clear();
    SignatureRecoveryPrecomputation.start(scheduler, transactions(1));
    assertThat(scheduledTasks).hasSize(1);

    scheduledTasks.clear();
    SignatureRecoveryPrecomputation.start(scheduler, List.of());
    assertThat(scheduledTasks).isEmpty();
  }

  @Test
  void recoversEverySenderAndAuthorityExactlyOnce() {
    final List<CodeDelegation> authorizations = authorizations(3);
    final List<Transaction> transactions = new ArrayList<>(transactions(9));
    transactions.add(delegateCodeTransaction(authorizations));

    SignatureRecoveryPrecomputation.start(scheduler, transactions);
    runScheduledTasks();

    transactions.forEach(transaction -> verify(transaction, times(1)).getSender());
    authorizations.forEach(authorization -> verify(authorization, times(1)).authorizer());
  }

  @Test
  void recoversNothingWhenCancelledBeforeTheTasksRun() {
    // the payload is found invalid before the computation pool gets to the queued tasks
    final List<CodeDelegation> authorizations = authorizations(100);
    final List<Transaction> transactions = new ArrayList<>(transactions(100));
    transactions.add(delegateCodeTransaction(authorizations));

    SignatureRecoveryPrecomputation.start(scheduler, transactions).cancel();
    runScheduledTasks();

    transactions.forEach(transaction -> verify(transaction, never()).getSender());
    authorizations.forEach(authorization -> verify(authorization, never()).authorizer());
  }

  @Test
  void stopsBeforeTheNextTransactionOnceCancelled() {
    final List<Transaction> transactions = transactions(100);
    final AtomicReference<SignatureRecoveryPrecomputation> precomputation = new AtomicReference<>();
    when(transactions.get(3).getSender())
        .thenAnswer(
            invocation -> {
              precomputation.get().cancel();
              return Address.ZERO;
            });

    precomputation.set(SignatureRecoveryPrecomputation.start(scheduler, transactions));
    runScheduledTasks();

    for (int i = 0; i < transactions.size(); i++) {
      verify(transactions.get(i), times(i <= 3 ? 1 : 0)).getSender();
    }
  }

  @Test
  void stopsBetweenAuthoritiesOnceCancelled() {
    final List<CodeDelegation> authorizations = authorizations(50);
    final AtomicReference<SignatureRecoveryPrecomputation> precomputation = new AtomicReference<>();
    when(authorizations.get(1).authorizer())
        .thenAnswer(
            invocation -> {
              precomputation.get().cancel();
              return Optional.of(Address.ZERO);
            });

    precomputation.set(
        SignatureRecoveryPrecomputation.start(
            scheduler, List.of(delegateCodeTransaction(authorizations))));
    runScheduledTasks();

    for (int i = 0; i < authorizations.size(); i++) {
      verify(authorizations.get(i), times(i <= 1 ? 1 : 0)).authorizer();
    }
  }

  @Test
  void keepsRecoveringAfterARecoveryFails() {
    final List<Transaction> transactions = transactions(3);
    when(transactions.get(0).getSender()).thenThrow(new IllegalStateException("invalid signature"));
    final List<CodeDelegation> authorizations = authorizations(2);
    when(authorizations.get(0).authorizer()).thenThrow(new IllegalStateException("bad auth"));
    final List<Transaction> all = new ArrayList<>(transactions);
    all.add(delegateCodeTransaction(authorizations));

    SignatureRecoveryPrecomputation.start(scheduler, all);
    runScheduledTasks();

    all.forEach(transaction -> verify(transaction, times(1)).getSender());
    authorizations.forEach(authorization -> verify(authorization, times(1)).authorizer());
  }

  private void runScheduledTasks() {
    // run sequentially: each task keeps taking transactions until none is left or it is cancelled
    scheduledTasks.forEach(task -> assertThat(task.get()).isNull());
  }

  private static List<Transaction> transactions(final int count) {
    final List<Transaction> transactions = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final Transaction transaction = mock(Transaction.class);
      when(transaction.getType()).thenReturn(TransactionType.EIP1559);
      when(transaction.getSender()).thenReturn(Address.ZERO);
      transactions.add(transaction);
    }
    return transactions;
  }

  private static Transaction delegateCodeTransaction(final List<CodeDelegation> authorizations) {
    final Transaction transaction = mock(Transaction.class);
    when(transaction.getType()).thenReturn(TransactionType.DELEGATE_CODE);
    when(transaction.getSender()).thenReturn(Address.ZERO);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(authorizations));
    return transaction;
  }

  private static List<CodeDelegation> authorizations(final int count) {
    final List<CodeDelegation> authorizations = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final CodeDelegation authorization = mock(CodeDelegation.class);
      when(authorization.authorizer()).thenReturn(Optional.of(Address.ZERO));
      authorizations.add(authorization);
    }
    return authorizations;
  }
}
