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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.ethereum.core.TransactionTestFixture.createSignedCodeDelegation;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.eth.transactions.layered.BaseFeePrioritizedTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.EndLayer;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredPendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.ReadyTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SparseTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.TransactionsLayer;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.fluent.SimpleAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * DETER+ with EIP-7702: an account that is delegated can have its balance moved by a call from an
 * unrelated party, without consuming the account nonce, so an execution-blind txpool cannot know
 * that the transactions queued behind the first one are going to be unfundable. These tests cover
 * the admission-time cap that stops an account that is delegated from holding more than one
 * transaction in the pool, and one that is going to be delegated from holding more than the
 * transactions up to the next nonce after the delegation.
 */
public class DelegatedSenderTransactionPoolTest extends AbstractTransactionPoolTestBase {

  private static final Bytes DELEGATION_INDICATOR_PREFIX = Bytes.fromHexString("0xef0100");
  private static final Address DELEGATION_TARGET =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  private static final Wei ATTACK_MAX_FEE = Wei.of(1000);
  private static final Wei ATTACK_PRIORITY_FEE = Wei.of(500);

  private static final int ATTACKER_ACCOUNTS = 12;
  private static final int ATTACK_TXS_PER_ACCOUNT = 16;

  private PendingTransactions pendingTransactions;

  @Override
  protected PendingTransactions createPendingTransactions(
      final TransactionPoolConfiguration poolConfig,
      final BiFunction<PendingTransaction, PendingTransaction, Boolean>
          transactionReplacementTester) {
    final var txPoolMetrics = new TransactionPoolMetrics(metricsSystem);
    final TransactionsLayer sparseLayer =
        new SparseTransactions(
            poolConfig,
            new EndLayer(txPoolMetrics, transactionEventNotifier.newEmitter()),
            txPoolMetrics,
            transactionReplacementTester,
            new BlobCache(),
            transactionEventNotifier.newEmitter());
    final TransactionsLayer readyLayer =
        new ReadyTransactions(
            poolConfig,
            sparseLayer,
            txPoolMetrics,
            transactionReplacementTester,
            new BlobCache(),
            transactionEventNotifier.newEmitter());
    pendingTransactions =
        new LayeredPendingTransactions(
            protocolContext,
            poolConfig,
            new BaseFeePrioritizedTransactions(
                poolConfig,
                protocolContext.getBlockchain()::getChainHeadHeader,
                readyLayer,
                txPoolMetrics,
                transactionReplacementTester,
                FeeMarket.london(0L),
                new BlobCache(),
                MiningConfiguration.newDefault(),
                senderBalanceChecker,
                transactionEventNotifier.newEmitter()),
            ethScheduler);
    return pendingTransactions;
  }

  @Override
  protected Transaction createTransaction(final int nonce, final Wei maxPrice) {
    return createTransactionBaseFeeMarket(nonce, maxPrice);
  }

  @Override
  protected TransactionTestFixture createBaseTransaction(final int nonce) {
    return createBaseTransactionBaseFeeMarket(nonce);
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
  public void transactionsBeyondTheNextNonceAreRejectedForAccountsDelegatedOnChain() {
    givenAllTransactionsAreValid();

    // these accounts have code starting with the delegation indicator in the chain head state,
    final List<KeyPair> delegatedAccounts = generateKeyPairs(ATTACKER_ACCOUNTS);
    givenChainHeadWithDelegatedAccounts(delegatedAccounts);

    // the account nonce is 0, so for the delegated accounts only nonce 0 is accepted, and
    // everything else is rejected only because of the delegation
    for (final KeyPair delegatedAccount : delegatedAccounts) {
      assertThat(pendingNoncesOf(delegatedAccount)).isEmpty();
      for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
        final var result =
            transactionPool.addTransactionViaApi(attackTransaction(nonce, delegatedAccount));
        if (nonce == 0) {
          assertThat(result.isValid()).describedAs("nonce %s", nonce).isTrue();
        } else {
          assertThat(result.isValid()).describedAs("nonce %s", nonce).isFalse();
          assertThat(result.getInvalidReason())
              .describedAs("nonce %s", nonce)
              .isEqualTo(TransactionInvalidReason.NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER);
        }
      }
      assertThat(pendingNoncesOf(delegatedAccount)).containsExactly(0L);
    }
  }

  @Test
  public void senderWhoseFirstPendingTransactionDelegatesItselfIsLimitedToTheNextNonce() {
    givenAllTransactionsAreValid();
    final KeyPair sender = generateKeyPairs(1).get(0);

    // the account is still a plain EOA on chain, and its first pending transaction is a type 4
    // that delegates the account itself, so it is not delegated yet, but it is going to be
    // when it is executed. The authorization is processed after the nonce of the sender is
    // incremented, so its nonce must be the one after the nonce of the transaction (1), and
    // applying it increments the nonce of the account once more, so the account ends with nonce 2
    final Transaction delegationTx = delegationTransaction(0, sender, signedDelegation(sender, 1L));
    transactionPool.addRemoteTransactions(List.of(delegationTx));
    assertTransactionPending(delegationTx);

    final List<Transaction> followingTxs = new ArrayList<>();
    for (int nonce = 1; nonce <= ATTACK_TXS_PER_ACCOUNT; nonce++) {
      final Transaction tx = attackTransaction(nonce, sender);
      followingTxs.add(tx);
      transactionPool.addRemoteTransactions(List.of(tx));
    }

    // nonce 1 is consumed by the authorization, so after the delegation is executed a transaction
    // with that nonce is already too low and it can never be executed, then it is not accepted.
    // Nonce 2 is the first one that can be executed after the delegation, and it is the only one
    // accepted, so the pool only has the delegation and that transaction, everything else is
    // rejected and so it is not kept or propagated
    assertThat(pendingNoncesOf(sender)).hasSize(2).containsExactly(0L, 2L);
    assertTransactionNotPending(followingTxs.get(0));
    assertThat(transactionPool.addTransactionViaApi(followingTxs.get(0)).getInvalidReason())
        .isEqualTo(TransactionInvalidReason.NONCE_ALREADY_CONSUMED_FOR_SENDER);
    assertTransactionPending(followingTxs.get(1));
    assertThat(followingTxs.subList(2, followingTxs.size()))
        .noneSatisfy(tx -> assertTransactionPending(tx));
  }

  @Test
  public void authorityOfAPendingDelegationIsLimitedToTheNextNonce() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    // a third party sponsors the delegation of the authority, that has signed an authorization
    // for its current nonce (0). The authority transactions never went through the pool before
    final Transaction sponsoredDelegationTx =
        delegationTransaction(0, sponsor, signedDelegation(authority, 0L));
    transactionPool.addRemoteTransactions(List.of(sponsoredDelegationTx));
    assertTransactionPending(sponsoredDelegationTx);

    final List<Transaction> authorityTxs = new ArrayList<>();
    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      final Transaction tx = attackTransaction(nonce, authority);
      authorityTxs.add(tx);
      transactionPool.addRemoteTransactions(List.of(tx));
    }

    // the authority will be delegated by the time these are executed, so only the current nonce
    // and the one after it are accepted
    assertThat(pendingNoncesOf(authority)).containsExactly(0L, 1L);
    assertThat(authorityTxs.subList(2, authorityTxs.size()))
        .noneSatisfy(tx -> assertTransactionPending(tx));
  }

  @Test
  public void delegationOfAnAccountThatAlreadyHasPendingTransactionsIsRejected() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    // the authority is a plain EOA, so its transactions are accepted as usual, and then more of
    // them than it is allowed to have once delegated
    final List<Transaction> authorityTxs = new ArrayList<>();
    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      final Transaction tx = attackTransaction(nonce, authority);
      authorityTxs.add(tx);
      transactionPool.addRemoteTransactions(List.of(tx));
    }
    assertThat(authorityTxs).allSatisfy(this::assertTransactionPending);

    // only now a third party sponsors the delegation of the authority, this must not be accepted,
    // otherwise it is enough to add it last to delegate an account with a full queue
    final Transaction sponsoredDelegationTx =
        delegationTransaction(0, sponsor, signedDelegation(authority, 0L));
    transactionPool.addRemoteTransactions(List.of(sponsoredDelegationTx));

    assertTransactionNotPending(sponsoredDelegationTx);
    assertThat(authorityTxs).allSatisfy(this::assertTransactionPending);
  }

  @Test
  public void
      delegationConfirmedInABlockNeverSeenByThePoolLimitsTheNewTransactionsOfTheAccountToTheNextNonce() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    // the delegation was never in the public mempool, it arrives in a block. The tuple is applied,
    // so in the post block state the authority is delegated and its nonce is one more
    final Transaction unseenDelegationTx =
        delegationTransaction(0, sponsor, signedDelegation(authority, 0L));
    appendBlockWithState(
        List.of(unseenDelegationTx),
        List.of(new AccountSeed(sponsor, 1, false), new AccountSeed(authority, 1, true)));

    final List<Transaction> authorityTxs = new ArrayList<>();
    for (int nonce = 1; nonce <= ATTACK_TXS_PER_ACCOUNT; nonce++) {
      final Transaction tx = attackTransaction(nonce, authority);
      authorityTxs.add(tx);
      transactionPool.addRemoteTransactions(List.of(tx));
    }

    // the authority is delegated now, with nonce 1, so only the transaction with the current nonce
    // is accepted
    assertThat(pendingNoncesOf(authority)).containsExactly(1L);
    assertThat(authorityTxs.subList(1, authorityTxs.size()))
        .noneSatisfy(tx -> assertTransactionPending(tx));
  }

  @Test
  public void staleAuthorizationDoesNotLimitTheTransactionsOfItsAuthority() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair victim = generateKeyPairs(1).get(0);

    // the nonce of the victim is already 5, so an old authorization of the victim, with nonce 2,
    // that someone replays in a pending delegation can never be applied
    appendBlockWithState(emptyList(), List.of(new AccountSeed(victim, 5, false)));
    final Transaction replayedDelegationTx =
        delegationTransaction(0, sponsor, signedDelegation(victim, 2L));
    transactionPool.addRemoteTransactions(List.of(replayedDelegationTx));
    assertTransactionPending(replayedDelegationTx);

    for (int nonce = 5; nonce < 5 + ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, victim)));
    }

    assertThat(pendingNoncesOf(victim)).hasSize(ATTACK_TXS_PER_ACCOUNT);
  }

  @Test
  public void authorizationForAnotherChainDoesNotLimitTheTransactionsOfItsAuthority() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    // the authorization is for another chain, so it will be skipped when the transaction is
    // executed
    final Transaction otherChainDelegationTx =
        delegationTransaction(
            0,
            sponsor,
            createSignedCodeDelegation(BigInteger.valueOf(999), DELEGATION_TARGET, 0L, authority));
    transactionPool.addRemoteTransactions(List.of(otherChainDelegationTx));
    assertTransactionPending(otherChainDelegationTx);

    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authority)));
    }

    assertThat(pendingNoncesOf(authority)).hasSize(ATTACK_TXS_PER_ACCOUNT);
  }

  @Test
  public void authorizationWithMaxNonceDoesNotLimitTheTransactionsOfItsAuthority() {
    givenAllTransactionsAreValid();
    final KeyPair sender = generateKeyPairs(1).get(0);

    // an authorization with nonce 2^64-1 can never be applied
    final Transaction delegationTx =
        delegationTransaction(0, sender, signedDelegation(sender, -1L));
    transactionPool.addRemoteTransactions(List.of(delegationTx));
    assertTransactionPending(delegationTx);

    for (int nonce = 1; nonce <= ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, sender)));
    }

    assertThat(pendingNoncesOf(sender)).hasSize(ATTACK_TXS_PER_ACCOUNT + 1);
  }

  @Test
  public void delegationsOfTransactionsRemovedByReconcileAreNotTrackedAnymore() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    final Transaction sponsoredDelegationTx =
        delegationTransaction(0, sponsor, signedDelegation(authority, 0L));
    transactionPool.addRemoteTransactions(List.of(sponsoredDelegationTx));
    assertTransactionPending(sponsoredDelegationTx);

    // the sponsor transaction is confirmed in a block that the pool does not process, so the
    // sponsor nonce in the world state is ahead of the pool one, and the next transaction of the
    // sponsor makes the pool reconcile the sponsor, that removes the delegation transaction
    appendBlockWithState(emptyList(), List.of(new AccountSeed(sponsor, 1, false)));
    transactionPool.addRemoteTransactions(List.of(attackTransaction(1, sponsor)));
    assertTransactionNotPending(sponsoredDelegationTx);

    // the delegation is not pending anymore, so the authority is not limited by it
    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authority)));
    }

    assertThat(pendingNoncesOf(authority)).hasSize(ATTACK_TXS_PER_ACCOUNT);
  }

  @Test
  public void delegationWithSeveralAuthoritiesIsRejectedIfAnyOfThemHasDisallowedPendingTxs() {
    givenAllTransactionsAreValid();

    // the order of the authorities in the transaction must not matter, so the one with the
    // disallowed pending transactions is first in one case, and last in the other
    for (final boolean disallowedFirst : List.of(true, false)) {
      final KeyPair sponsor = generateKeyPairs(1).get(0);
      final KeyPair authorityWithFullQueue = generateKeyPairs(1).get(0);
      final KeyPair cleanAuthority = generateKeyPairs(1).get(0);

      for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
        transactionPool.addRemoteTransactions(
            List.of(attackTransaction(nonce, authorityWithFullQueue)));
      }
      assertThat(pendingNoncesOf(authorityWithFullQueue)).hasSize(ATTACK_TXS_PER_ACCOUNT);

      final CodeDelegation fullQueueDelegation = signedDelegation(authorityWithFullQueue, 0L);
      final CodeDelegation cleanDelegation = signedDelegation(cleanAuthority, 0L);
      final Transaction delegationTx =
          delegationTransaction(
              0,
              sponsor,
              disallowedFirst
                  ? List.of(fullQueueDelegation, cleanDelegation)
                  : List.of(cleanDelegation, fullQueueDelegation));

      assertThat(transactionPool.addTransactionViaApi(delegationTx).getInvalidReason())
          .describedAs("disallowedFirst %s", disallowedFirst)
          .isEqualTo(TransactionInvalidReason.DELEGATION_AUTHORITY_HAS_PENDING_TRANSACTIONS);
      assertTransactionNotPending(delegationTx);

      // the rejected transaction is not tracked, so it must not limit the clean authority
      for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
        transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, cleanAuthority)));
      }
      assertThat(pendingNoncesOf(cleanAuthority)).hasSize(ATTACK_TXS_PER_ACCOUNT);
    }
  }

  @Test
  public void delegationWithSeveralAuthoritiesIsAcceptedIfNoneOfThemHasDisallowedPendingTxs() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authorityA = generateKeyPairs(1).get(0);
    final KeyPair authorityB = generateKeyPairs(1).get(0);
    final KeyPair authorityWithoutTxs = generateKeyPairs(1).get(0);

    // up to the nonce after the current one is allowed once delegated
    for (int nonce = 0; nonce <= 1; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authorityA)));
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authorityB)));
    }

    final Transaction delegationTx =
        delegationTransaction(
            0,
            sponsor,
            List.of(
                signedDelegation(authorityA, 0L),
                signedDelegation(authorityWithoutTxs, 0L),
                signedDelegation(authorityB, 0L)));
    transactionPool.addRemoteTransactions(List.of(delegationTx));
    assertTransactionPending(delegationTx);

    // and now all of them are limited
    for (final KeyPair authority : List.of(authorityA, authorityB, authorityWithoutTxs)) {
      for (int nonce = 2; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
        transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authority)));
      }
    }
    assertThat(pendingNoncesOf(authorityA)).containsExactly(0L, 1L);
    assertThat(pendingNoncesOf(authorityB)).containsExactly(0L, 1L);
    assertThat(pendingNoncesOf(authorityWithoutTxs)).isEmpty();
  }

  @Test
  public void selfDelegationAndSponsoredAuthorityInTheSameTransactionAreBothChecked() {
    givenAllTransactionsAreValid();

    // the sender delegates itself, with the authorization nonce after the one of the transaction,
    // and also another account, that in the first case has a queue that is too long once delegated
    for (final boolean otherAuthorityHasFullQueue : List.of(true, false)) {
      final KeyPair sender = generateKeyPairs(1).get(0);
      final KeyPair otherAuthority = generateKeyPairs(1).get(0);

      final int otherAuthorityTxs = otherAuthorityHasFullQueue ? ATTACK_TXS_PER_ACCOUNT : 2;
      for (int nonce = 0; nonce < otherAuthorityTxs; nonce++) {
        transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, otherAuthority)));
      }

      final Transaction delegationTx =
          delegationTransaction(
              0,
              sender,
              List.of(signedDelegation(sender, 1L), signedDelegation(otherAuthority, 0L)));
      final var result = transactionPool.addTransactionViaApi(delegationTx);

      if (otherAuthorityHasFullQueue) {
        assertThat(result.isValid()).isFalse();
        assertThat(result.getInvalidReason())
            .isEqualTo(TransactionInvalidReason.DELEGATION_AUTHORITY_HAS_PENDING_TRANSACTIONS);
        assertTransactionNotPending(delegationTx);
      } else {
        assertThat(result.isValid()).isTrue();
        assertTransactionPending(delegationTx);
      }
    }
  }

  @Test
  public void delegationsOfATransactionDroppedWhileBeingAddedAreNotTracked() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    // a pool that has room for just one transaction in the prioritized layer, and none in the
    // other layers, and that one transaction is worth more than the delegation, so when the
    // delegation is added, it is the one evicted, from all the layers, before the add returns, and
    // the drop is notified while the transaction is being added
    transactionPool =
        createTransactionPool(
            b ->
                b.minGasPrice(Wei.of(2))
                    .maxPrioritizedTransactions(1)
                    .pendingTransactionsLayerMaxCapacityBytes(1L));
    final Transaction moreValuableTx = attackTransaction(0, generateKeyPairs(1).get(0));
    transactionPool.addTransactionViaApi(moreValuableTx);
    assertTransactionPending(moreValuableTx);

    final Transaction delegationTx =
        createBaseTransactionBaseFeeMarket(0)
            .type(TransactionType.DELEGATE_CODE)
            .gasLimit(100_000L)
            .maxFeePerGas(Optional.of(Wei.of(900)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(400)))
            .to(Optional.of(DELEGATION_TARGET))
            .codeDelegations(List.of(signedDelegation(authority, 0L)))
            .createTransaction(sponsor);
    transactionPool.addTransactionViaApi(delegationTx);

    // whatever the outcome of the add is, the transaction is not pending, so it can not be tracked
    assertTransactionNotPending(delegationTx);
    assertThat(transactionPool.trackedDelegationTransactionCount()).isZero();
  }

  @Test
  public void delegationsOfATransactionRejectedByThePoolAreNotTracked() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    // the sponsor has a pending transaction with nonce 0, so another one with the same nonce is a
    // replacement, and since it pays less, the pool rejects it, after the delegation checks passed.
    // Both are remote and are added to the pending transactions directly, so they end up in the
    // same layer, where the replacement rule is applied. A local replacement, as the ones sent via
    // API, can be added to a higher layer than the transaction it replaces, and in that case the
    // transaction is replaced without applying the rule
    final Transaction pendingTx = attackTransaction(0, sponsor);
    transactionPool.addRemoteTransactions(List.of(pendingTx));
    assertTransactionPending(pendingTx);

    final Transaction underpricedReplacement =
        createBaseTransactionBaseFeeMarket(0)
            .type(TransactionType.DELEGATE_CODE)
            .gasLimit(100_000L)
            .maxFeePerGas(Optional.of(Wei.of(900)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(400)))
            .to(Optional.of(DELEGATION_TARGET))
            .codeDelegations(List.of(signedDelegation(authority, 0L)))
            .createTransaction(sponsor);
    transactionPool.addRemoteTransactions(List.of(underpricedReplacement));
    assertTransactionPending(pendingTx);
    assertTransactionNotPending(underpricedReplacement);

    // the rejected transaction must not leave a pending delegation for the authority
    assertThat(transactionPool.trackedDelegationTransactionCount()).isZero();
    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authority)));
    }
    assertThat(pendingNoncesOf(authority)).hasSize(ATTACK_TXS_PER_ACCOUNT);
  }

  @Test
  public void readdingAPendingTransactionKeepsItsDelegationsTracked() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    final Transaction delegationTx =
        delegationTransaction(0, sponsor, signedDelegation(authority, 0L));
    transactionPool.addRemoteTransactions(List.of(delegationTx));
    assertTransactionPending(delegationTx);

    // adding the same transaction again is refused as already known, and since it is the
    // transaction that is pending, its delegations must stay tracked
    transactionPool.addRemoteTransactions(List.of(delegationTx));
    assertTransactionPending(delegationTx);
    assertThat(transactionPool.trackedDelegationTransactionCount()).isEqualTo(1);

    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authority)));
    }
    assertThat(pendingNoncesOf(authority)).containsExactly(0L, 1L);
  }

  @Test
  public void pendingTransactionsOfAnAuthorityDelegatedByAnImportedBlockArePurgedBeyondItsNonce() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);
    final KeyPair bystander = generateKeyPairs(1).get(0);

    // the authority is a plain EOA, so it can have a long queue, and so can an unrelated account
    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authority)));
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, bystander)));
    }
    assertThat(pendingNoncesOf(authority)).hasSize(ATTACK_TXS_PER_ACCOUNT);

    // the delegation never went through the txpool, it is in a block, where it is applied, so the
    // authority ends up delegated, with the nonce after the authorization one
    final Transaction unseenDelegationTx =
        delegationTransaction(0, sponsor, signedDelegation(authority, 0L));
    appendBlockWithState(
        List.of(unseenDelegationTx),
        List.of(new AccountSeed(sponsor, 1, false), new AccountSeed(authority, 1, true)));

    // the nonce 0 is consumed by the delegation, and the nonce 1 is the only one that a delegated
    // account can have in the pool, the other ones are purged, since they could be unfundable
    await().untilAsserted(() -> assertThat(pendingNoncesOf(authority)).containsExactly(1L));
    // while an unrelated account is not affected
    assertThat(pendingNoncesOf(bystander)).hasSize(ATTACK_TXS_PER_ACCOUNT);
  }

  @Test
  public void pendingTransactionsOfAnAuthorityWhoseAuthorizationWasNotAppliedAreKept() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair skippedAuthority = generateKeyPairs(1).get(0);
    final KeyPair appliedAuthority = generateKeyPairs(1).get(0);

    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, skippedAuthority)));
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, appliedAuthority)));
    }

    // one authorization was skipped when the block was executed, so its authority is not
    // delegated, and its nonce is unchanged, while the other one was applied
    final Transaction delegationTx =
        delegationTransaction(
            0,
            sponsor,
            List.of(
                signedDelegation(skippedAuthority, 0L), signedDelegation(appliedAuthority, 0L)));
    appendBlockWithState(
        List.of(delegationTx),
        List.of(
            new AccountSeed(sponsor, 1, false),
            new AccountSeed(skippedAuthority, 0, false),
            new AccountSeed(appliedAuthority, 1, true)));

    // the purge of the applied authority is the barrier, since all the purges of a block are done
    // together, when it is done the one of the other authority has been decided too
    await().untilAsserted(() -> assertThat(pendingNoncesOf(appliedAuthority)).containsExactly(1L));
    assertThat(pendingNoncesOf(skippedAuthority)).hasSize(ATTACK_TXS_PER_ACCOUNT);
  }

  @Test
  public void pendingTransactionsOfAnAuthorityDelegatedByAReorgAreAlsoPurged() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    final BlockHeader commonParent = blockchain.getChainHeadHeader();
    final Block canonical =
        appendBlockWithState(commonParent, Difficulty.ONE, emptyList(), emptyList());
    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authority)));
    }
    assertThat(pendingNoncesOf(authority)).hasSize(ATTACK_TXS_PER_ACCOUNT);

    // a fork that has the delegation, becomes the canonical chain, so the event is a reorg, and
    // the authority is delegated in the state of the new head
    final Transaction delegationTx =
        delegationTransaction(0, sponsor, signedDelegation(authority, 0L));
    final Block fork1 =
        appendBlockWithState(
            commonParent,
            Difficulty.ONE,
            List.of(delegationTx),
            List.of(new AccountSeed(sponsor, 1, false)));
    final Block fork2 =
        appendBlockWithState(
            fork1.getHeader(),
            Difficulty.of(2000),
            emptyList(),
            List.of(new AccountSeed(sponsor, 1, false), new AccountSeed(authority, 1, true)));
    assertThat(blockchain.getChainHeadHash()).isEqualTo(fork2.getHash());
    assertThat(canonical.getHash()).isNotEqualTo(blockchain.getChainHeadHash());

    await().untilAsserted(() -> assertThat(pendingNoncesOf(authority)).containsExactly(1L));
  }

  @Test
  public void transactionValidatedBeforeABlockDelegatedTheSenderIsCheckedAgainstTheNewHead() {
    givenAllTransactionsAreValid();
    final KeyPair sender = generateKeyPairs(1).get(0);
    final Hash headWhenValidated = blockchain.getChainHeadHash();
    final Optional<Account> senderWhenValidated =
        Optional.of(new SimpleAccount(addressOf(sender), 0L, Wei.fromEth(100)));

    // a block imported after the validation delegates the sender
    givenChainHeadWithDelegatedAccounts(List.of(sender));
    assertThat(blockchain.getChainHeadHash()).isNotEqualTo(headWhenValidated);

    final DelegationLimits limits = new DelegationLimits(pendingTransactions, protocolContext);
    final Transaction farFutureTx = attackTransaction(5, sender);
    final var result =
        limits.addTransaction(
            PendingTransaction.newPendingTransaction(farFutureTx, false, false, Byte.MAX_VALUE),
            senderWhenValidated,
            headWhenValidated);

    // the stale snapshot says that it is a plain EOA, but it is delegated at the head now
    assertThat(result).isEqualTo(TransactionAddedResult.NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER);
    assertTransactionNotPending(farFutureTx);
  }

  @Test
  public void selfDelegationWithAnAuthorizationThatCanNotBeAppliedDoesNotLimitTheSender() {
    givenAllTransactionsAreValid();
    final KeyPair sender = generateKeyPairs(1).get(0);

    // the authorization has the nonce of the transaction (0), but the sender nonce is incremented
    // before it is processed, so it is skipped, and the transaction is still valid, and it must be
    // pooled as any other client does
    final Transaction delegationTx = delegationTransaction(0, sender, signedDelegation(sender, 0L));
    transactionPool.addRemoteTransactions(List.of(delegationTx));
    assertTransactionPending(delegationTx);

    for (int nonce = 1; nonce <= ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, sender)));
    }
    assertThat(pendingNoncesOf(sender)).hasSize(ATTACK_TXS_PER_ACCOUNT + 1);
  }

  @Test
  public void selfDelegationLimitsTheSenderOnlyIfTheAuthorizationHasTheNonceAfterTheTransaction() {
    givenAllTransactionsAreValid();
    final KeyPair skipped = generateKeyPairs(1).get(0);
    final KeyPair applied = generateKeyPairs(1).get(0);

    // transaction with nonce 3, and an authorization with nonce 1, that is already in the past
    // of the account when it is processed (4), so it never applies, and it does not limit
    transactionPool.addRemoteTransactions(
        List.of(delegationTransaction(3, skipped, signedDelegation(skipped, 1L))));
    for (int nonce = 4; nonce <= 8; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, skipped)));
    }
    assertThat(pendingNoncesOf(skipped)).containsExactly(3L, 4L, 5L, 6L, 7L, 8L);

    // while with the authorization that has the nonce after the one of the transaction (4) it does
    transactionPool.addRemoteTransactions(
        List.of(delegationTransaction(3, applied, signedDelegation(applied, 4L))));
    for (int nonce = 4; nonce <= 8; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, applied)));
    }
    assertThat(pendingNoncesOf(applied)).containsExactly(3L, 5L);
  }

  @Test
  public void selfDelegationWithConsecutiveAuthorizationsConsumesAllTheirNonces() {
    givenAllTransactionsAreValid();
    final KeyPair sender = generateKeyPairs(1).get(0);

    // the authorization with the nonce 1 is applied after the nonce of the sender is incremented,
    // and that increments the nonce again, so the one with nonce 2 is applied too, and the first
    // nonce that can be executed after the delegation is 3. The one with nonce 5 is skipped
    transactionPool.addRemoteTransactions(
        List.of(
            delegationTransaction(
                0,
                sender,
                List.of(
                    signedDelegation(sender, 2L),
                    signedDelegation(sender, 1L),
                    signedDelegation(sender, 5L)))));
    for (int nonce = 1; nonce <= 5; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, sender)));
    }
    assertThat(pendingNoncesOf(sender)).containsExactly(0L, 3L);
  }

  @Test
  public void selfDelegationOfASenderWithPendingTransactionsConsumedOrBeyondTheLimitIsRejected() {
    givenAllTransactionsAreValid();
    final KeyPair beyondLimit = generateKeyPairs(1).get(0);
    final KeyPair consumed = generateKeyPairs(1).get(0);
    final KeyPair allowed = generateKeyPairs(1).get(0);

    // the delegation has the transaction nonce 0, the authorization nonce 1 is consumed, and the
    // highest nonce allowed is 2
    transactionPool.addRemoteTransactions(List.of(attackTransaction(3, beyondLimit)));
    transactionPool.addRemoteTransactions(List.of(attackTransaction(1, consumed)));
    transactionPool.addRemoteTransactions(List.of(attackTransaction(2, allowed)));

    assertThat(
            transactionPool
                .addTransactionViaApi(
                    delegationTransaction(0, beyondLimit, signedDelegation(beyondLimit, 1L)))
                .getInvalidReason())
        .isEqualTo(TransactionInvalidReason.DELEGATION_AUTHORITY_HAS_PENDING_TRANSACTIONS);
    assertThat(
            transactionPool
                .addTransactionViaApi(
                    delegationTransaction(0, consumed, signedDelegation(consumed, 1L)))
                .getInvalidReason())
        .isEqualTo(TransactionInvalidReason.DELEGATION_AUTHORITY_HAS_PENDING_TRANSACTIONS);
    assertThat(
            transactionPool
                .addTransactionViaApi(
                    delegationTransaction(0, allowed, signedDelegation(allowed, 1L)))
                .isValid())
        .isTrue();
  }

  @Test
  public void authorizationForTheChainOfTheTransactionLimitsItsAuthority() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    final Transaction delegationTx =
        createBaseTransactionBaseFeeMarket(0)
            .type(TransactionType.DELEGATE_CODE)
            .gasLimit(100_000L)
            .to(Optional.of(DELEGATION_TARGET))
            .codeDelegations(
                List.of(
                    createSignedCodeDelegation(
                        // the chain id of the transaction, not the zero that applies to any chain
                        createBaseTransactionBaseFeeMarket(0)
                            .createTransaction(sponsor)
                            .getChainId()
                            .orElseThrow(),
                        DELEGATION_TARGET,
                        0L,
                        authority)))
            .createTransaction(sponsor);
    transactionPool.addRemoteTransactions(List.of(delegationTx));
    assertTransactionPending(delegationTx);

    for (int nonce = 0; nonce < ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authority)));
    }
    assertThat(pendingNoncesOf(authority)).containsExactly(0L, 1L);
  }

  @Test
  public void staleAuthorizationDoesNotLimitAnAuthorityThatAlreadyHasAQueue() {
    givenAllTransactionsAreValid();
    final KeyPair sponsor = generateKeyPairs(1).get(0);
    final KeyPair authority = generateKeyPairs(1).get(0);

    // the authority is already at the nonce 3, so an authorization with the nonce 1 can never be
    // applied, and its transactions, that are a long queue, must not prevent the delegation
    appendBlockWithState(emptyList(), List.of(new AccountSeed(authority, 3, false)));
    for (int nonce = 3; nonce < 3 + ATTACK_TXS_PER_ACCOUNT; nonce++) {
      transactionPool.addRemoteTransactions(List.of(attackTransaction(nonce, authority)));
    }
    assertThat(pendingNoncesOf(authority)).hasSize(ATTACK_TXS_PER_ACCOUNT);

    final Transaction staleDelegationTx =
        delegationTransaction(0, sponsor, signedDelegation(authority, 1L));
    transactionPool.addRemoteTransactions(List.of(staleDelegationTx));
    assertTransactionPending(staleDelegationTx);
    assertThat(pendingNoncesOf(authority)).hasSize(ATTACK_TXS_PER_ACCOUNT);
  }

  private List<Long> pendingNoncesOf(final KeyPair keyPair) {
    final Address address = addressOf(keyPair);
    return transactionPool.getPendingTransactions().stream()
        .map(pendingTx -> pendingTx.getTransaction())
        .filter(tx -> tx.getSender().equals(address))
        .map(Transaction::getNonce)
        .sorted()
        .toList();
  }

  private Transaction delegationTransaction(
      final int nonce, final KeyPair sender, final CodeDelegation codeDelegation) {
    return delegationTransaction(nonce, sender, List.of(codeDelegation));
  }

  private Transaction delegationTransaction(
      final int nonce, final KeyPair sender, final List<CodeDelegation> codeDelegations) {
    return createBaseTransactionBaseFeeMarket(nonce)
        .type(TransactionType.DELEGATE_CODE)
        .gasLimit(100_000L)
        .to(Optional.of(DELEGATION_TARGET))
        .codeDelegations(codeDelegations)
        .createTransaction(sender);
  }

  private static CodeDelegation signedDelegation(final KeyPair authority, final long nonce) {
    return createSignedCodeDelegation(BigInteger.ZERO, DELEGATION_TARGET, nonce, authority);
  }

  private Transaction attackTransaction(final int nonce, final KeyPair keyPair) {
    return createBaseTransactionBaseFeeMarket(nonce)
        .gasLimit(21_000L)
        .maxFeePerGas(Optional.of(ATTACK_MAX_FEE))
        .maxPriorityFeePerGas(Optional.of(ATTACK_PRIORITY_FEE))
        .payload(Bytes.random(8192))
        .createTransaction(keyPair);
  }

  private static List<KeyPair> generateKeyPairs(final int count) {
    final List<KeyPair> keyPairs = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      keyPairs.add(SignatureAlgorithmFactory.getInstance().generateKeyPair());
    }
    return keyPairs;
  }

  private static Address addressOf(final KeyPair keyPair) {
    return Util.publicKeyToAddress(keyPair.getPublicKey());
  }

  private record AccountSeed(KeyPair keyPair, long nonce, boolean delegated) {}

  /**
   * Appends a new block to the chain, where the state contains the passed accounts, already
   * delegated on chain, so they are seen as such by the txpool when it reads the chain head state.
   */
  private void givenChainHeadWithDelegatedAccounts(final List<KeyPair> delegatedAccounts) {
    appendBlockWithState(
        emptyList(), delegatedAccounts.stream().map(k -> new AccountSeed(k, 0, true)).toList());
  }

  /**
   * Appends a new block with the passed transactions to the chain, where the state contains the
   * passed accounts, so they are seen as such by the txpool when it reads the chain head state.
   */
  private void appendBlockWithState(
      final List<Transaction> blockTransactions, final List<AccountSeed> accounts) {
    appendBlockWithState(
        blockchain.getChainHeadHeader(), Difficulty.ONE, blockTransactions, accounts);
  }

  private Block appendBlockWithState(
      final BlockHeader parent,
      final Difficulty difficulty,
      final List<Transaction> blockTransactions,
      final List<AccountSeed> accounts) {
    final MutableWorldState worldState = getExecutionContext().getStateArchive().getWorldState();
    final WorldUpdater updater = worldState.updater();
    for (final AccountSeed seed : accounts) {
      final MutableAccount account =
          updater.createAccount(addressOf(seed.keyPair()), seed.nonce(), Wei.fromEth(100));
      if (seed.delegated()) {
        account.setCode(
            Bytes.concatenate(DELEGATION_INDICATOR_PREFIX, DELEGATION_TARGET.getBytes()));
      }
    }
    updater.commit();
    worldState.persist(null);

    final Block block =
        new Block(
            new BlockHeaderTestFixture()
                .baseFeePerGas(Wei.of(10L))
                .gasLimit(parent.getGasLimit())
                .difficulty(difficulty)
                .parentHash(parent.getHash())
                .number(parent.getNumber() + 1)
                .stateRoot(worldState.rootHash())
                .buildHeader(),
            new BlockBody(blockTransactions, emptyList()));
    blockchain.appendBlock(
        block,
        blockTransactions.stream()
            .map(tx -> new TransactionReceipt(1, 1, emptyList(), Optional.empty()))
            .toList());
    return block;
  }
}
