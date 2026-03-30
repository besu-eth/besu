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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.ValidationResult.valid;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.EXCEEDS_BLOCK_GAS_LIMIT;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.GAS_PRICE_TOO_LOW;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.INVALID_TRANSACTION_FORMAT;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.NONCE_TOO_LOW;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.REPLAY_PROTECTED_SIGNATURE_REQUIRED;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.TRANSACTION_ALREADY_KNOWN;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.TRANSACTION_REPLACEMENT_UNDERPRICED;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.TX_FEECAP_EXCEEDED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.plugin.services.TransactionPoolValidatorService;
import org.hyperledger.besu.util.number.Percentage;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
public abstract class AbstractTransactionPoolTest extends AbstractTransactionPoolTestBase {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void localTransactionHappyPath(final boolean noLocalPriority) {
    this.transactionPool = createTransactionPool(b -> b.noLocalPriority(noLocalPriority));
    final Transaction transaction = createTransaction(0);

    givenTransactionIsValid(transaction);

    addAndAssertTransactionViaApiValid(transaction, noLocalPriority);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldReturnLocalTransactionsWhenAppropriate(final boolean noLocalPriority) {
    this.transactionPool = createTransactionPool(b -> b.noLocalPriority(noLocalPriority));
    final Transaction localTransaction2 = createTransaction(2);

    givenTransactionIsValid(localTransaction2);
    givenTransactionIsValid(transaction0);
    givenTransactionIsValid(transaction1);

    addAndAssertTransactionViaApiValid(localTransaction2, noLocalPriority);
    addAndAssertRemoteTransactionsValid(transaction0);
    addAndAssertRemoteTransactionsValid(transaction1);

    assertThat(transactionPool.count()).isEqualTo(3);
    assertThat(getLocalTransactions()).contains(localTransaction2);
    assertThat(getPriorityTransactions()).hasSize(noLocalPriority ? 0 : 1);
  }

  @Test
  public void shouldRemoveTransactionsFromPendingListWhenIncludedInBlockOnchain() {
    givenTransactionIsValid(transaction0);

    addAndAssertRemoteTransactionsValid(transaction0);

    appendBlock(transaction0);

    assertTransactionNotPending(transaction0);
  }

  @Test
  public void shouldRemoveMultipleTransactionsAddedInOneBlock() {
    givenTransactionIsValid(transaction0);
    givenTransactionIsValid(transaction1);

    addAndAssertRemoteTransactionsValid(transaction0);
    addAndAssertRemoteTransactionsValid(transaction1);

    appendBlock(transaction0, transaction1);

    assertTransactionNotPending(transaction0);
    assertTransactionNotPending(transaction1);
    assertThat(transactionPool.count()).isZero();
  }

  @Test
  public void shouldIgnoreUnknownTransactionsThatAreAddedInABlock() {
    givenTransactionIsValid(transaction0);

    addAndAssertRemoteTransactionsValid(transaction0);

    appendBlock(transaction0, transaction1);

    assertTransactionNotPending(transaction0);
    assertTransactionNotPending(transaction1);
    assertThat(transactionPool.count()).isZero();
  }

  @Test
  public void shouldNotRemovePendingTransactionsWhenABlockAddedToAFork() {
    givenTransactionIsValid(transaction0);

    addAndAssertRemoteTransactionsValid(transaction0);

    final BlockHeader commonParent = getHeaderForCurrentChainHead();
    final Block canonicalHead = appendBlock(Difficulty.of(1000), commonParent);
    appendBlock(Difficulty.ONE, commonParent, transaction0);

    verifyChainHeadIs(canonicalHead);

    assertTransactionPending(transaction0);
  }

  @Test
  public void shouldRemovePendingTransactionsFromAllBlocksOnAForkWhenItBecomesTheCanonicalChain() {
    givenTransactionIsValid(transaction0);
    givenTransactionIsValid(transaction1);

    addAndAssertRemoteTransactionsValid(transaction0);
    addAndAssertRemoteTransactionsValid(transaction1);

    final BlockHeader commonParent = getHeaderForCurrentChainHead();
    final Block originalChainHead = appendBlock(Difficulty.of(1000), commonParent);

    final Block forkBlock1 = appendBlock(Difficulty.ONE, commonParent, transaction0);
    verifyChainHeadIs(originalChainHead);

    final Block forkBlock2 = appendBlock(Difficulty.of(2000), forkBlock1.getHeader(), transaction1);
    verifyChainHeadIs(forkBlock2);

    assertTransactionNotPending(transaction0);
    assertTransactionNotPending(transaction1);
  }

  @Test
  public void shouldReAddTransactionsFromThePreviousCanonicalHeadWhenAReorgOccurs() {
    givenTransactionIsValid(transaction0);
    givenTransactionIsValid(transactionOtherSender);

    transactionPool.addTransactionViaApi(transaction0);
    transactionPool.addRemoteTransactions(List.of(transactionOtherSender));

    final BlockHeader commonParent = getHeaderForCurrentChainHead();
    final Block originalFork1 = appendBlock(Difficulty.of(1000), commonParent, transaction0);
    final Block originalFork2 =
        appendBlock(Difficulty.ONE, originalFork1.getHeader(), transactionOtherSender);
    assertTransactionNotPending(transaction0);
    assertTransactionNotPending(transactionOtherSender);
    assertThat(getLocalTransactions()).isEmpty();

    final Block reorgFork1 = appendBlock(Difficulty.ONE, commonParent);
    verifyChainHeadIs(originalFork2);

    transactionPool.subscribePendingTransactions(listener);
    final Block reorgFork2 = appendBlock(Difficulty.of(2000), reorgFork1.getHeader());
    verifyChainHeadIs(reorgFork2);

    assertTransactionPending(transaction0);
    assertTransactionPending(transactionOtherSender);
    assertThat(getLocalTransactions()).contains(transaction0);
    assertThat(getLocalTransactions()).doesNotContain(transactionOtherSender);
    verify(listener).onTransactionAdded(transaction0);
    verify(listener).onTransactionAdded(transactionOtherSender);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void shouldNotReAddTransactionsThatAreInBothForksWhenReorgHappens() {
    givenTransactionIsValid(transaction0);
    givenTransactionIsValid(transaction1);

    addAndAssertRemoteTransactionsValid(transaction0);
    addAndAssertRemoteTransactionsValid(transaction1);

    final BlockHeader commonParent = getHeaderForCurrentChainHead();
    final Block originalFork1 = appendBlock(Difficulty.of(1000), commonParent, transaction0);
    final Block originalFork2 =
        appendBlock(Difficulty.ONE, originalFork1.getHeader(), transaction1);
    assertTransactionNotPending(transaction0);
    assertTransactionNotPending(transaction1);

    final Block reorgFork1 = appendBlock(Difficulty.ONE, commonParent, transaction0);
    verifyChainHeadIs(originalFork2);

    final Block reorgFork2 = appendBlock(Difficulty.of(2000), reorgFork1.getHeader());
    verifyChainHeadIs(reorgFork2);

    assertTransactionNotPending(transaction0);
    assertTransactionPending(transaction1);
  }

  @Test
  @EnabledIf("isBaseFeeMarket")
  public void shouldReAddBlobTxsWhenReorgHappens() {
    givenTransactionIsValid(transaction0);
    givenTransactionIsValid(transaction1);
    givenTransactionIsValid(transactionWithBlobs);

    addAndAssertRemoteTransactionsValid(transaction0);
    addAndAssertRemoteTransactionsValid(transaction1);
    addAndAssertRemoteTransactionsValid(transactionWithBlobs);

    final BlockHeader commonParent = getHeaderForCurrentChainHead();
    final Block originalFork1 = appendBlock(Difficulty.of(1000), commonParent, transaction0);
    final Block originalFork2 =
        appendBlock(Difficulty.of(10), originalFork1.getHeader(), transaction1);
    final Block originalFork3 =
        appendBlock(Difficulty.of(1), originalFork2.getHeader(), transactionWithBlobs);
    assertTransactionNotPending(transaction0);
    assertTransactionNotPending(transaction1);
    assertTransactionNotPending(transactionWithBlobs);

    final Block reorgFork1 = appendBlock(Difficulty.ONE, commonParent);
    verifyChainHeadIs(originalFork3);

    final Block reorgFork2 = appendBlock(Difficulty.of(2000), reorgFork1.getHeader());
    verifyChainHeadIs(reorgFork2);

    final Block reorgFork3 = appendBlock(Difficulty.of(3000), reorgFork2.getHeader());
    verifyChainHeadIs(reorgFork3);

    assertTransactionPending(transaction0);
    assertTransactionPending(transaction1);
    assertTransactionPending(transactionWithBlobs);

    Optional<Transaction> maybeBlob =
        transactionPool.getTransactionByHash(transactionWithBlobs.getHash());
    assertThat(maybeBlob).isPresent();
    Transaction restoredBlob = maybeBlob.get();
    assertThat(restoredBlob).isEqualTo(transactionWithBlobs);
    assertThat(restoredBlob.getBlobsWithCommitments().get().getBlobProofBundles())
        .isEqualTo(transactionWithBlobs.getBlobsWithCommitments().get().getBlobProofBundles());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void addLocalTransaction_strictReplayProtectionOn_txWithChainId_chainIdIsConfigured(
      final boolean noLocalPriority) {
    protocolSupportsTxReplayProtection(1337, true);
    transactionPool =
        createTransactionPool(
            b -> b.strictTransactionReplayProtectionEnabled(true).noLocalPriority(noLocalPriority));
    final Transaction tx = createTransaction(1);
    givenTransactionIsValid(tx);

    addAndAssertTransactionViaApiValid(tx, noLocalPriority);
  }

  @Test
  public void addRemoteTransactions_strictReplayProtectionOn_txWithChainId_chainIdIsConfigured() {
    protocolSupportsTxReplayProtection(1337, true);
    transactionPool = createTransactionPool(b -> b.strictTransactionReplayProtectionEnabled(true));
    final Transaction tx = createTransaction(1);
    givenTransactionIsValid(tx);

    addAndAssertRemoteTransactionsValid(tx);
  }

  @Test
  public void shouldNotAddRemoteTransactionsWhenGasPriceBelowMinimum() {
    final Transaction transaction = createTransaction(1, Wei.ONE);
    transactionPool.addRemoteTransactions(singletonList(transaction));

    assertTransactionNotPending(transaction);
    verifyNoMoreInteractions(transactionValidatorFactory);
  }

  @Test
  public void shouldAddRemotePriorityTransactionsWhenGasPriceBelowMinimum() {
    final Transaction transaction = createTransaction(1, Wei.of(7));
    transactionPool =
        createTransactionPool(b -> b.prioritySenders(Set.of(transaction.getSender())));

    givenTransactionIsValid(transaction);

    addAndAssertRemotePriorityTransactionsValid(transaction);
  }

  @Test
  public void shouldNotAddRemoteTransactionsThatAreInvalidAccordingToStateDependentChecks() {
    givenTransactionIsValid(transaction0);
    givenTransactionIsValid(transaction1);
    when(transactionValidatorFactory
            .get()
            .validateForSender(eq(transaction1), eq(null), any(TransactionValidationParams.class)))
        .thenReturn(ValidationResult.invalid(NONCE_TOO_LOW));
    transactionPool.addRemoteTransactions(asList(transaction0, transaction1));

    assertTransactionPending(transaction0);
    assertTransactionNotPending(transaction1);
    verify(transactionBroadcaster).onTransactionsAdded(singletonList(transaction0));
    verify(transactionValidatorFactory.get())
        .validate(eq(transaction0), any(Optional.class), any(Optional.class), any());
    verify(transactionValidatorFactory.get())
        .validateForSender(eq(transaction0), eq(null), any(TransactionValidationParams.class));
    verify(transactionValidatorFactory.get())
        .validate(eq(transaction1), any(Optional.class), any(Optional.class), any());
    verify(transactionValidatorFactory.get()).validateForSender(eq(transaction1), any(), any());
    verifyNoMoreInteractions(transactionValidatorFactory.get());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldAllowSequenceOfTransactionsWithIncreasingNonceFromSameSender(
      final boolean noLocalPriority) {
    transactionPool = createTransactionPool(b -> b.noLocalPriority(noLocalPriority));
    final Transaction transaction1 = createTransaction(1);
    final Transaction transaction2 = createTransaction(2);
    final Transaction transaction3 = createTransaction(3);

    givenTransactionIsValid(transaction1);
    givenTransactionIsValid(transaction2);
    givenTransactionIsValid(transaction3);

    addAndAssertTransactionViaApiValid(transaction1, noLocalPriority);
    addAndAssertTransactionViaApiValid(transaction2, noLocalPriority);
    addAndAssertTransactionViaApiValid(transaction3, noLocalPriority);
  }

  @Test
  public void
      shouldAllowSequenceOfTransactionsWithIncreasingNonceFromSameSenderWhenSentInBatchOutOfOrder() {
    final Transaction transaction2 = createTransaction(2);

    givenTransactionIsValid(transaction0);
    givenTransactionIsValid(transaction1);
    givenTransactionIsValid(transaction2);

    addAndAssertRemoteTransactionsValid(transaction2);
    addAndAssertRemoteTransactionsValid(transaction0);
    addAndAssertRemoteTransactionsValid(transaction1);
  }

  @Test
  public void shouldDiscardRemoteTransactionThatAlreadyExistsBeforeValidation() {
    givenTransactionIsValid(transaction0);
    addAndAssertRemoteTransactionsValid(transaction0);

    verify(transactionValidatorFactory, atLeastOnce()).get();

    clearInvocations(transactionValidatorFactory);

    // trying to re-add the same tx should return transaction already known and no
    // access to the transaction validator factory should be done
    final var result = transactionPool.addRemoteTransactions(singletonList(transaction0));
    assertThat(result.get(transaction0.getHash()))
        .isEqualTo(ValidationResult.invalid(TRANSACTION_ALREADY_KNOWN));

    verifyNoInteractions(transactionValidatorFactory);
  }

  @Test
  public void shouldNotNotifyBatchListenerWhenRemoteTransactionDoesNotReplaceExisting() {
    final Transaction transaction0a = createTransaction(0, Wei.of(100));
    final Transaction transaction0b = createTransaction(0, Wei.of(50));

    givenTransactionIsValid(transaction0a);
    givenTransactionIsValid(transaction0b);

    addAndAssertRemoteTransactionsValid(transaction0a);
    addAndAssertRemoteTransactionInvalid(transaction0b);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldNotNotifyBatchListenerWhenLocalTransactionDoesNotReplaceExisting(
      final boolean noLocalPriority) {
    transactionPool =
        createTransactionPool(b -> b.minGasPrice(Wei.of(2)).noLocalPriority(noLocalPriority));
    final Transaction transaction0a = createTransaction(0, Wei.of(10));
    final Transaction transaction0b = createTransaction(0, Wei.of(9));

    givenTransactionIsValid(transaction0a);
    givenTransactionIsValid(transaction0b);

    addAndAssertTransactionViaApiValid(transaction0a, noLocalPriority);
    addAndAssertTransactionViaApiInvalid(transaction0b, TRANSACTION_REPLACEMENT_UNDERPRICED);
  }

  @Test
  public void shouldRejectLocalTransactionsWhereGasLimitExceedBlockGasLimit() {
    final Transaction transaction0 =
        createBaseTransaction(0).gasLimit(blockGasLimit + 1).createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction0);

    addAndAssertTransactionViaApiInvalid(transaction0, EXCEEDS_BLOCK_GAS_LIMIT);
  }

  @Test
  public void shouldRejectRemoteTransactionsWhereGasLimitExceedBlockGasLimit() {
    final Transaction transaction0 =
        createBaseTransaction(0).gasLimit(blockGasLimit + 1).createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction0);

    addAndAssertRemoteTransactionInvalid(transaction0);
  }

  @Test
  public void shouldAcceptLocalTransactionsEvenIfAnInvalidTransactionWithLowerNonceExists() {
    transactionPool = createTransactionPool(b -> b.noLocalPriority(false));
    final Transaction invalidTx =
        createBaseTransaction(0).gasLimit(blockGasLimit + 1).createTransaction(KEY_PAIR1);

    final Transaction nextTx = createBaseTransaction(1).gasLimit(1).createTransaction(KEY_PAIR1);

    givenTransactionIsValid(invalidTx);
    givenTransactionIsValid(nextTx);

    addAndAssertTransactionViaApiInvalid(invalidTx, EXCEEDS_BLOCK_GAS_LIMIT);
    addAndAssertTransactionViaApiValid(nextTx, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldRejectLocalTransactionsWhenNonceTooFarInFuture(final boolean noLocalPriority) {
    transactionPool = createTransactionPool(b -> b.noLocalPriority(noLocalPriority));
    final Transaction transactionFarFuture = createTransaction(Integer.MAX_VALUE);

    givenTransactionIsValid(transactionFarFuture);

    addAndAssertTransactionViaApiInvalid(transactionFarFuture, NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER);
  }

  @Test
  public void shouldNotNotifyBatchListenerIfNoTransactionsAreAdded() {
    transactionPool.addRemoteTransactions(emptyList());
    verifyNoInteractions(transactionBroadcaster);
  }

  @Test
  public void shouldSendPooledTransactionHashesIfPeerSupportsEth65() {
    EthPeer peer = mock(EthPeer.class);
    when(peer.hasSupportForMessage(EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES))
        .thenReturn(true);

    givenTransactionIsValid(transaction0);
    transactionPool.addTransactionViaApi(transaction0);
    transactionPool.handleConnect(peer);
    syncTaskCapture.getValue().run();
    verify(newPooledTransactionHashesMessageSender).sendTransactionAnnouncementsToPeer(peer);
  }

  @Test
  public void shouldSendPooledTransactionHashesToNewlyConnectedPeer() {
    givenTransactionIsValid(transaction0);
    givenTransactionIsValid(transaction1);

    transactionPool.addTransactionViaApi(transaction0);
    transactionPool.addRemoteTransactions(Collections.singletonList(transaction1));

    RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager);
    transactionPool.handleConnect(peer.getEthPeer());

    Set<Transaction> transactionsToSendToPeer = new HashSet<>();
    Transaction tx;
    while ((tx = peerTransactionTracker.claimAnnouncementToSendToPeer(peer.getEthPeer())) != null) {
      transactionsToSendToPeer.add(tx);
    }

    assertThat(transactionsToSendToPeer).contains(transaction0, transaction1);
  }

  @Test
  public void shouldCallValidatorWithExpectedValidationParameters() {
    final ArgumentCaptor<TransactionValidationParams> txValidationParamCaptor =
        ArgumentCaptor.forClass(TransactionValidationParams.class);

    when(transactionValidatorFactory
            .get()
            .validate(eq(transaction0), any(Optional.class), any(Optional.class), any()))
        .thenReturn(valid());
    when(transactionValidatorFactory
            .get()
            .validateForSender(any(), any(), txValidationParamCaptor.capture()))
        .thenReturn(valid());

    final TransactionValidationParams expectedValidationParams =
        TransactionValidationParams.transactionPool();

    transactionPool.addTransactionViaApi(transaction0);

    assertThat(txValidationParamCaptor.getValue())
        .usingRecursiveComparison()
        .isEqualTo(expectedValidationParams);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldIgnoreFeeCapIfSetZero(final boolean noLocalPriority) {
    final Wei twoEthers = Wei.fromEth(2);
    transactionPool =
        createTransactionPool(b -> b.txFeeCap(Wei.ZERO).noLocalPriority(noLocalPriority));
    final Transaction transaction = createTransaction(0, twoEthers.add(Wei.of(1)));

    givenTransactionIsValid(transaction);

    addAndAssertTransactionViaApiValid(transaction, noLocalPriority);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldRejectLocalTransactionIfFeeCapExceeded(final boolean noLocalPriority) {
    final Wei twoEthers = Wei.fromEth(2);
    transactionPool =
        createTransactionPool(b -> b.txFeeCap(twoEthers).noLocalPriority(noLocalPriority));

    final Transaction transactionLocal = createTransaction(0, twoEthers.add(1));

    givenTransactionIsValid(transactionLocal);

    addAndAssertTransactionViaApiInvalid(transactionLocal, TX_FEECAP_EXCEEDED);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldAcceptRemoteTransactionEvenIfFeeCapExceeded(final boolean hasPriority) {
    final Wei twoEthers = Wei.fromEth(2);
    final Transaction remoteTransaction = createTransaction(0, twoEthers.add(1));
    final Set<Address> prioritySenders =
        hasPriority ? Set.of(remoteTransaction.getSender()) : Set.of();
    transactionPool =
        createTransactionPool(b -> b.txFeeCap(twoEthers).prioritySenders(prioritySenders));

    givenTransactionIsValid(remoteTransaction);

    addAndAssertRemoteTransactionsValid(hasPriority, remoteTransaction);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void transactionNotRejectedByPluginShouldBeAdded(final boolean noLocalPriority) {
    final TransactionPoolValidatorService transactionPoolValidatorService =
        getTransactionPoolValidatorServiceReturning(null); // null -> not rejecting !!
    this.transactionPool =
        createTransactionPool(
            b ->
                b.noLocalPriority(noLocalPriority)
                    .transactionPoolValidatorService(transactionPoolValidatorService));

    givenTransactionIsValid(transaction0);

    addAndAssertTransactionViaApiValid(transaction0, noLocalPriority);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void transactionRejectedByPluginShouldNotBeAdded(final boolean noLocalPriority) {
    final TransactionPoolValidatorService transactionPoolValidatorService =
        getTransactionPoolValidatorServiceReturning("false");
    this.transactionPool =
        createTransactionPool(
            b ->
                b.noLocalPriority(noLocalPriority)
                    .transactionPoolValidatorService(transactionPoolValidatorService));

    givenTransactionIsValid(transaction0);

    addAndAssertTransactionViaApiInvalid(
        transaction0, TransactionInvalidReason.PLUGIN_TX_POOL_VALIDATOR);
  }

  @Test
  public void remoteTransactionRejectedByPluginShouldNotBeAdded() {
    final TransactionPoolValidatorService transactionPoolValidatorService =
        getTransactionPoolValidatorServiceReturning("false");
    this.transactionPool =
        createTransactionPool(
            b -> b.transactionPoolValidatorService(transactionPoolValidatorService));

    givenTransactionIsValid(transaction0);

    addAndAssertRemoteTransactionInvalid(transaction0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @DisabledIf("isBaseFeeMarket")
  public void
      addLocalTransaction_strictReplayProtectionOn_txWithoutChainId_chainIdIsConfigured_protectionNotSupportedAtCurrentBlock(
          final boolean noLocalPriority) {
    protocolSupportsTxReplayProtection(1337, false);
    transactionPool =
        createTransactionPool(
            b -> b.strictTransactionReplayProtectionEnabled(true).noLocalPriority(noLocalPriority));
    final Transaction tx = createTransactionWithoutChainId(1);
    givenTransactionIsValid(tx);

    addAndAssertTransactionViaApiValid(tx, noLocalPriority);
  }

  @Test
  @DisabledIf("isBaseFeeMarket")
  public void
      addRemoteTransactions_strictReplayProtectionOff_txWithoutChainId_chainIdIsConfigured() {
    protocolSupportsTxReplayProtection(1337, true);
    transactionPool = createTransactionPool(b -> b.strictTransactionReplayProtectionEnabled(false));
    final Transaction tx = createTransactionWithoutChainId(1);
    givenTransactionIsValid(tx);

    addAndAssertRemoteTransactionsValid(tx);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @DisabledIf("isBaseFeeMarket")
  public void addLocalTransaction_strictReplayProtectionOff_txWithoutChainId_chainIdIsConfigured(
      final boolean noLocalPriority) {
    protocolSupportsTxReplayProtection(1337, true);
    transactionPool =
        createTransactionPool(
            b ->
                b.strictTransactionReplayProtectionEnabled(false).noLocalPriority(noLocalPriority));
    final Transaction tx = createTransactionWithoutChainId(1);
    givenTransactionIsValid(tx);

    addAndAssertTransactionViaApiValid(tx, noLocalPriority);
  }

  @Test
  @DisabledIf("isBaseFeeMarket")
  public void addLocalTransaction_strictReplayProtectionOn_txWithoutChainId_chainIdIsConfigured() {
    protocolSupportsTxReplayProtection(1337, true);
    transactionPool = createTransactionPool(b -> b.strictTransactionReplayProtectionEnabled(true));
    final Transaction tx = createTransactionWithoutChainId(1);
    givenTransactionIsValid(tx);

    addAndAssertTransactionViaApiInvalid(tx, REPLAY_PROTECTED_SIGNATURE_REQUIRED);
  }

  @Test
  @DisabledIf("isBaseFeeMarket")
  public void
      addRemoteTransactions_strictReplayProtectionOn_txWithoutChainId_chainIdIsConfigured() {
    protocolSupportsTxReplayProtection(1337, true);
    transactionPool = createTransactionPool(b -> b.strictTransactionReplayProtectionEnabled(true));
    final Transaction tx = createTransactionWithoutChainId(1);
    givenTransactionIsValid(tx);

    addAndAssertRemoteTransactionsValid(tx);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @DisabledIf("isBaseFeeMarket")
  public void addLocalTransaction_strictReplayProtectionOn_txWithoutChainId_chainIdIsNotConfigured(
      final boolean noLocalPriority) {
    protocolDoesNotSupportTxReplayProtection();
    transactionPool =
        createTransactionPool(
            b -> b.strictTransactionReplayProtectionEnabled(true).noLocalPriority(noLocalPriority));
    final Transaction tx = createTransactionWithoutChainId(1);
    givenTransactionIsValid(tx);

    addAndAssertTransactionViaApiValid(tx, noLocalPriority);
  }

  @Test
  @DisabledIf("isBaseFeeMarket")
  public void
      addRemoteTransactions_strictReplayProtectionOn_txWithoutChainId_chainIdIsNotConfigured() {
    protocolDoesNotSupportTxReplayProtection();
    transactionPool = createTransactionPool(b -> b.strictTransactionReplayProtectionEnabled(true));
    final Transaction tx = createTransactionWithoutChainId(1);
    givenTransactionIsValid(tx);

    addAndAssertRemoteTransactionsValid(tx);
  }

  @Test
  @DisabledIf("isBaseFeeMarket")
  public void shouldIgnoreEIP1559TransactionWhenNotAllowed() {
    final Transaction transaction =
        createBaseTransaction(1)
            .type(TransactionType.EIP1559)
            .maxFeePerGas(Optional.of(Wei.of(100L)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(50L)))
            .gasLimit(10)
            .gasPrice(null)
            .createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction);

    addAndAssertTransactionViaApiInvalid(transaction, INVALID_TRANSACTION_FORMAT);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @DisabledIf("isBaseFeeMarket")
  public void shouldAcceptZeroGasPriceFrontierPriorityTransactions(final boolean isLocal) {
    final Transaction transaction = createFrontierTransaction(0, Wei.ZERO);
    transactionPool =
        createTransactionPool(b -> b.prioritySenders(List.of(transaction.getSender())));

    givenTransactionIsValid(transaction);

    if (isLocal) {
      addAndAssertTransactionViaApiValid(transaction, false);
    } else {
      addAndAssertRemoteTransactionsValid(true, transaction);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldRejectZeroGasPriceNoPriorityTransaction(final boolean isLocal) {
    final Transaction transaction = createTransaction(0, Wei.ZERO);
    transactionPool = createTransactionPool(b -> b.noLocalPriority(true));

    givenTransactionIsValid(transaction);

    if (isLocal) {
      addAndAssertTransactionViaApiInvalid(transaction, GAS_PRICE_TOO_LOW);
    } else {
      addAndAssertRemoteTransactionInvalid(transaction);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @DisabledIf("isBaseFeeMarket")
  public void shouldAcceptZeroGasPriceNoPriorityTransactionWhenMinGasPriceIsZero(
      final boolean isLocal) {
    transactionPool = createTransactionPool(b -> b.minGasPrice(Wei.ZERO).noLocalPriority(true));

    final Transaction transaction = createTransaction(0, Wei.ZERO);

    givenTransactionIsValid(transaction);

    if (isLocal) {
      addAndAssertTransactionViaApiValid(transaction, true);
    } else {
      addAndAssertRemoteTransactionsValid(false, transaction);
    }
  }

  @ParameterizedTest
  @MethodSource("provideHasPriorityAndIsLocal")
  public void shouldAcceptZeroGasPriceFrontierTxsWhenMinGasPriceIsZeroAndLondonWithZeroBaseFee(
      final boolean hasPriority, final boolean isLocal) {
    final Transaction frontierTransaction = createFrontierTransaction(0, Wei.ZERO);
    internalAcceptZeroGasPriceTxsWhenMinGasPriceIsZeroAndZeroBaseFee(
        hasPriority, isLocal, frontierTransaction);
  }

  @ParameterizedTest
  @MethodSource("provideHasPriorityAndIsLocal")
  public void shouldAcceptZeroGasPrice1559TxsWhenMinGasPriceIsZeroAndLondonWithZeroBaseFee(
      final boolean hasPriority, final boolean isLocal) {
    final Transaction transaction = createTransactionBaseFeeMarket(0, Wei.ZERO);
    internalAcceptZeroGasPriceTxsWhenMinGasPriceIsZeroAndZeroBaseFee(
        hasPriority, isLocal, transaction);
  }

  private void internalAcceptZeroGasPriceTxsWhenMinGasPriceIsZeroAndZeroBaseFee(
      final boolean hasPriority, final boolean isLocal, final Transaction transaction) {
    transactionPool =
        createTransactionPool(
            b -> {
              b.minGasPrice(Wei.ZERO);
              if (hasPriority) {
                b.prioritySenders(List.of(transaction.getSender()));
              } else {
                b.noLocalPriority(true);
              }
            });

    when(protocolSpec.getFeeMarket()).thenReturn(FeeMarket.london(0, Optional.of(Wei.ZERO)));
    whenBlockBaseFeeIs(Wei.ZERO);

    givenTransactionIsValid(transaction);
    if (isLocal) {
      addAndAssertTransactionViaApiValid(transaction, !hasPriority);
    } else {
      addAndAssertRemoteTransactionsValid(hasPriority, transaction);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void samePriceTxReplacementWhenPriceBumpIsZeroFrontier(final boolean noLocalPriority) {
    transactionPool =
        createTransactionPool(
            b ->
                b.priceBump(Percentage.ZERO)
                    .noLocalPriority(noLocalPriority)
                    .minGasPrice(Wei.ZERO));

    final Transaction transaction1a =
        createBaseTransactionGasPriceMarket(0)
            .gasPrice(Wei.ZERO)
            .to(Optional.of(Address.ALTBN128_ADD))
            .createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction1a);

    transactionPool.addRemoteTransactions(List.of(transaction1a));

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction1a);

    final Transaction transaction1b =
        createBaseTransactionGasPriceMarket(0)
            .gasPrice(Wei.ZERO)
            .to(Optional.of(Address.KZG_POINT_EVAL))
            .createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction1b);

    transactionPool.addRemoteTransactions(List.of(transaction1b));

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction1b);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @EnabledIf("isBaseFeeMarket")
  public void replaceSamePriceTxWhenPriceBumpIsZeroLondon(final boolean noLocalPriority) {
    transactionPool =
        createTransactionPool(
            b ->
                b.priceBump(Percentage.ZERO)
                    .noLocalPriority(noLocalPriority)
                    .minGasPrice(Wei.ZERO));

    final Transaction transaction1a =
        createBaseTransactionBaseFeeMarket(0)
            .maxFeePerGas(Optional.of(Wei.ZERO))
            .maxPriorityFeePerGas(Optional.of(Wei.ZERO))
            .to(Optional.of(Address.ALTBN128_ADD))
            .createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction1a);

    transactionPool.addRemoteTransactions(List.of(transaction1a));

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction1a);

    final Transaction transaction1b =
        createBaseTransactionBaseFeeMarket(0)
            .maxFeePerGas(Optional.of(Wei.ZERO))
            .maxPriorityFeePerGas(Optional.of(Wei.ZERO))
            .to(Optional.of(Address.KZG_POINT_EVAL))
            .createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction1b);

    transactionPool.addRemoteTransactions(List.of(transaction1b));

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction1b);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @EnabledIf("isBaseFeeMarket")
  public void replaceSamePriceTxWhenPriceBumpIsZeroLondonToFrontier(final boolean noLocalPriority) {
    transactionPool =
        createTransactionPool(
            b ->
                b.priceBump(Percentage.ZERO)
                    .noLocalPriority(noLocalPriority)
                    .minGasPrice(Wei.ZERO));

    final Transaction transaction1a =
        createBaseTransactionBaseFeeMarket(0)
            .maxFeePerGas(Optional.of(Wei.ZERO))
            .maxPriorityFeePerGas(Optional.of(Wei.ZERO))
            .to(Optional.of(Address.ALTBN128_ADD))
            .createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction1a);

    transactionPool.addRemoteTransactions(List.of(transaction1a));

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction1a);

    final Transaction transaction1b =
        createBaseTransactionGasPriceMarket(0)
            .gasPrice(Wei.ZERO)
            .to(Optional.of(Address.KZG_POINT_EVAL))
            .createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction1b);

    transactionPool.addRemoteTransactions(List.of(transaction1b));

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction1b);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @EnabledIf("isBaseFeeMarket")
  public void replaceSamePriceTxWhenPriceBumpIsZeroFrontierToLondon(final boolean noLocalPriority) {
    transactionPool =
        createTransactionPool(
            b ->
                b.priceBump(Percentage.ZERO)
                    .noLocalPriority(noLocalPriority)
                    .minGasPrice(Wei.ZERO));

    final Transaction transaction1a =
        createBaseTransactionGasPriceMarket(0)
            .gasPrice(Wei.ZERO)
            .to(Optional.of(Address.KZG_POINT_EVAL))
            .createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction1a);

    transactionPool.addRemoteTransactions(List.of(transaction1a));

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction1a);

    final Transaction transaction1b =
        createBaseTransactionBaseFeeMarket(0)
            .maxFeePerGas(Optional.of(Wei.ZERO))
            .maxPriorityFeePerGas(Optional.of(Wei.ZERO))
            .to(Optional.of(Address.ALTBN128_ADD))
            .createTransaction(KEY_PAIR1);

    givenTransactionIsValid(transaction1b);

    transactionPool.addRemoteTransactions(List.of(transaction1b));

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction1b);
  }

  private static Stream<Arguments> provideHasPriorityAndIsLocal() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldAcceptBaseFeeFloorGasPriceFrontierPriorityTransactions(final boolean isLocal) {
    final Transaction frontierTransaction = createFrontierTransaction(0, BASE_FEE_FLOOR);
    transactionPool =
        createTransactionPool(b -> b.prioritySenders(List.of(frontierTransaction.getSender())));

    givenTransactionIsValid(frontierTransaction);

    if (isLocal) {
      addAndAssertTransactionViaApiValid(frontierTransaction, false);
    } else {
      addAndAssertRemoteTransactionsValid(true, frontierTransaction);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldRejectNoPriorityTxsWhenMaxFeePerGasBelowMinGasPrice(final boolean isLocal) {
    final Wei genesisBaseFee = Wei.of(100L);
    final Wei minGasPrice = Wei.of(200L);
    final Wei lastBlockBaseFee = minGasPrice.add(50L);
    final Wei txMaxFeePerGas = minGasPrice.subtract(1L);

    assertThat(
            addTxAndGetPendingTxsCount(
                genesisBaseFee, minGasPrice, lastBlockBaseFee, txMaxFeePerGas, isLocal, false))
        .isZero();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldAcceptNoPriorityTxsWhenMaxFeePerGasIsAtLeastEqualToMinGasPrice(
      final boolean isLocal) {
    final Wei genesisBaseFee = Wei.of(100L);
    final Wei minGasPrice = Wei.of(200L);
    final Wei lastBlockBaseFee = minGasPrice.add(50L);
    final Wei txMaxFeePerGas = minGasPrice;

    assertThat(
            addTxAndGetPendingTxsCount(
                genesisBaseFee, minGasPrice, lastBlockBaseFee, txMaxFeePerGas, isLocal, false))
        .isEqualTo(1);
  }

  @Test
  public void addRemoteTransactionsShouldAllowDuplicates() {
    final Transaction transaction1 = createTransaction(1, Wei.of(7L));
    final Transaction transaction2a = createTransaction(2, Wei.of(7L));
    final Transaction transaction2b = createTransaction(2, Wei.of(7L));
    final Transaction transaction3 = createTransaction(3, Wei.of(7L));

    givenTransactionIsValid(transaction1);
    givenTransactionIsValid(transaction2a);
    givenTransactionIsValid(transaction2b);
    givenTransactionIsValid(transaction3);

    transactionPool.addRemoteTransactions(
        List.of(transaction1, transaction2a, transaction2b, transaction3));

    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsExactlyInAnyOrder(transaction1, transaction2a, transaction3);
  }
}
