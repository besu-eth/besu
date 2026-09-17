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
package org.hyperledger.besu.ethereum.eth.transactions.layered;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.TransactionTestFixture.createSignedCodeDelegation;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MAX_SCORE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;

public class BaseTransactionPoolTest extends TrustedSetupClassLoaderExtension {

  protected static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  protected static final KeyPair KEYS1 = SIGNATURE_ALGORITHM.generateKeyPair();
  protected static final KeyPair KEYS2 = SIGNATURE_ALGORITHM.generateKeyPair();
  protected static final Address SENDER1 = Util.publicKeyToAddress(KEYS1.getPublicKey());
  protected static final Address SENDER2 = Util.publicKeyToAddress(KEYS2.getPublicKey());
  protected static final CodeDelegation CODE_DELEGATION_SENDER_1 =
      createSignedCodeDelegation(BigInteger.ONE, Address.ZERO, 0, KEYS1);
  protected static final Wei DEFAULT_MIN_GAS_PRICE = Wei.of(50);
  protected static final Wei DEFAULT_MIN_PRIORITY_FEE = Wei.ZERO;
  private static final Random randomizeTxType = new Random();

  protected final Transaction transaction0 = createTransaction(0);
  protected final Transaction transaction1 = createTransaction(1);
  protected final Transaction transaction2 = createTransaction(2);
  protected final Transaction blobTransaction0 = createEIP4844Transaction(0, KEYS1, 1, 1);

  protected final EthScheduler ethScheduler = new DeterministicEthScheduler();
  protected final StubMetricsSystem metricsSystem = new StubMetricsSystem();
  protected final SenderBalanceChecker senderBalanceChecker = mock(SenderBalanceChecker.class);

  @BeforeEach
  public void setupSenderBalanceChecker() {
    when(senderBalanceChecker.hasEnoughBalanceFor(any())).thenReturn(true);
  }

  protected Transaction createTransaction(final long nonce) {
    return createTransaction(nonce, Wei.of(5000L), KEYS1);
  }

  protected Transaction createTransaction(final long nonce, final KeyPair keys) {
    return createTransaction(nonce, Wei.of(5000L), keys);
  }

  protected Transaction createTransaction(final long nonce, final Wei maxGasPrice) {
    return createTransaction(nonce, maxGasPrice, KEYS1);
  }

  protected Transaction createTransaction(final long nonce, final int payloadSize) {
    return createTransaction(nonce, Wei.of(5000L), payloadSize, KEYS1);
  }

  protected Transaction createTransaction(
      final long nonce, final Wei maxGasPrice, final KeyPair keys) {
    return createTransaction(nonce, maxGasPrice, 0, keys);
  }

  protected Transaction createEIP1559Transaction(
      final long nonce, final KeyPair keys, final int gasFeeMultiplier) {
    return createTransaction(
        TransactionType.EIP1559, nonce, Wei.of(5000L).multiply(gasFeeMultiplier), 0, null, keys);
  }

  protected Transaction createEIP4844Transaction(
      final long nonce, final KeyPair keys, final int gasFeeMultiplier, final int blobCount) {
    return createTransaction(
        TransactionType.BLOB,
        nonce,
        Wei.of(5000L).multiply(gasFeeMultiplier),
        Wei.of(5000L).multiply(gasFeeMultiplier).divide(10),
        0,
        blobCount,
        BlobType.KZG_PROOF,
        null,
        keys);
  }

  protected Transaction createEIP7702Transaction(
      final long nonce,
      final KeyPair keys,
      final int gasFeeMultiplier,
      final List<CodeDelegation> codeDelegations) {
    return createTransaction(
        TransactionType.DELEGATE_CODE,
        nonce,
        Wei.of(5000L).multiply(gasFeeMultiplier),
        0,
        codeDelegations,
        keys);
  }

  protected Transaction createTransactionOfSize(
      final long nonce, final Wei maxGasPrice, final int txSize, final KeyPair keys) {

    final TransactionType txType =
        TransactionType.values()[
            randomizeTxType.nextInt(txSize < blobTransaction0.getSizeForAnnouncement() ? 3 : 4)];

    final Transaction baseTx =
        createTransaction(
            txType,
            nonce,
            maxGasPrice,
            maxGasPrice.divide(10),
            0,
            1,
            BlobType.KZG_PROOF,
            null,
            keys);
    final int payloadSize = txSize - baseTx.getSizeForAnnouncement();

    return createTransaction(
        txType,
        nonce,
        maxGasPrice,
        maxGasPrice.divide(10),
        payloadSize,
        1,
        BlobType.KZG_PROOF,
        null,
        keys);
  }

  protected Transaction createTransaction(
      final long nonce, final Wei maxGasPrice, final int payloadSize, final KeyPair keys) {

    final TransactionType txType = TransactionType.values()[randomizeTxType.nextInt(4)];

    return switch (txType) {
      case FRONTIER, ACCESS_LIST, EIP1559 ->
          createTransaction(txType, nonce, maxGasPrice, payloadSize, null, keys);
      case BLOB ->
          createTransaction(
              txType,
              nonce,
              maxGasPrice,
              maxGasPrice.divide(10),
              payloadSize,
              1,
              BlobType.KZG_PROOF,
              null,
              keys);
      case DELEGATE_CODE ->
          createTransaction(
              txType, nonce, maxGasPrice, payloadSize, List.of(CODE_DELEGATION_SENDER_1), keys);
    };
  }

  protected Transaction createTransaction(
      final TransactionType type,
      final long nonce,
      final Wei maxGasPrice,
      final int payloadSize,
      final List<CodeDelegation> codeDelegations,
      final KeyPair keys) {
    return createTransaction(
        type,
        nonce,
        maxGasPrice,
        maxGasPrice.divide(10),
        payloadSize,
        0,
        BlobType.KZG_PROOF,
        codeDelegations,
        keys);
  }

  protected Transaction createTransaction(
      final TransactionType type,
      final long nonce,
      final Wei maxGasPrice,
      final Wei maxPriorityFeePerGas,
      final int payloadSize,
      final int blobCount,
      final BlobType blobType,
      final List<CodeDelegation> codeDelegations,
      final KeyPair keys) {
    return prepareTransaction(
            type,
            nonce,
            maxGasPrice,
            maxPriorityFeePerGas,
            payloadSize,
            blobCount,
            blobType,
            codeDelegations)
        .createTransaction(keys);
  }

  protected TransactionTestFixture prepareTransaction(
      final TransactionType type,
      final long nonce,
      final Wei maxGasPrice,
      final Wei maxPriorityFeePerGas,
      final int payloadSize,
      final int blobCount,
      final BlobType blobType,
      final List<CodeDelegation> codeDelegations) {

    var tx =
        new TransactionTestFixture()
            .to(Optional.of(Address.fromHexString("0x634316eA0EE79c701c6F67C53A4C54cBAfd2316d")))
            .value(Wei.of(nonce))
            .nonce(nonce)
            .type(type);
    if (payloadSize > 0) {
      var payloadBytes = Bytes.fromHexString("01".repeat(payloadSize));
      tx.payload(payloadBytes);
    }
    if (type.supports1559FeeMarket()) {
      tx.maxFeePerGas(Optional.of(maxGasPrice))
          .maxPriorityFeePerGas(Optional.of(maxPriorityFeePerGas));
      if (type.supportsBlob() && blobCount > 0) {
        tx.maxFeePerBlobGas(Optional.of(maxGasPrice));
        final var blobTestFixture = new BlobTestFixture();
        final var blobProofBundles =
            IntStream.range(0, blobCount)
                .mapToObj(i -> blobTestFixture.createBlobProofBundle(blobType))
                .toList();

        final var versionedHashes =
            blobProofBundles.stream().map(BlobProofBundle::getVersionedHash).toList();

        final var blobsWithCommitments = new BlobsWithCommitments(blobProofBundles);

        tx.versionedHashes(Optional.of(versionedHashes));
        tx.blobsWithCommitments(Optional.of(blobsWithCommitments));
      } else if (type.supportsDelegateCode()) {
        tx.codeDelegations(codeDelegations);
      }
    } else {
      tx.gasPrice(maxGasPrice);
    }
    return tx;
  }

  protected Transaction createTransactionReplacement(
      final Transaction originalTransaction, final KeyPair keys) {
    return createTransaction(
        originalTransaction.getType(),
        originalTransaction.getNonce(),
        originalTransaction.getMaxGasPrice().multiply(2),
        originalTransaction.getMaxGasPrice().multiply(2).divide(10),
        0,
        1,
        originalTransaction
            .getBlobsWithCommitments()
            .map(BlobsWithCommitments::getBlobType)
            .orElse(BlobType.KZG_PROOF),
        originalTransaction.getCodeDelegationList().orElse(null),
        keys);
  }

  protected PendingTransaction createRemotePendingTransaction(final Transaction transaction) {
    return new PendingTransaction.Remote(transaction);
  }

  protected PendingTransaction createRemotePendingTransaction(
      final Transaction transaction, final boolean hasPriority) {
    return PendingTransaction.newPendingTransaction(transaction, false, hasPriority, MAX_SCORE);
  }

  protected PendingTransaction createLocalPendingTransaction(final Transaction transaction) {
    return new PendingTransaction.Local(transaction);
  }

  protected void assertTransactionPending(
      final PendingTransactions transactions, final Transaction t) {
    assertThat(transactions.getTransactionByHash(t.getHash())).contains(t);
  }

  protected void assertTransactionNotPending(
      final PendingTransactions transactions, final Transaction t) {
    assertThat(transactions.getTransactionByHash(t.getHash())).isEmpty();
  }

  protected void assertNoNextNonceForSender(
      final PendingTransactions pendingTransactions, final Address sender) {
    assertThat(pendingTransactions.getNextNonceForSender(sender)).isEmpty();
  }

  protected void assertNextNonceForSender(
      final PendingTransactions pendingTransactions, final Address sender1, final int i) {
    assertThat(pendingTransactions.getNextNonceForSender(sender1)).isPresent().hasValue(i);
  }

  protected void addLocalTransactions(
      final PendingTransactions sorter, final Account sender, final long... nonces) {
    for (final long nonce : nonces) {
      sorter.addTransaction(
          createLocalPendingTransaction(createTransaction(nonce)), Optional.of(sender));
    }
  }

  protected long getAddedCount(
      final String source, final String priority, final AddReason addReason, final String layer) {
    return metricsSystem.getCounterValue(
        TransactionPoolMetrics.ADDED_COUNTER_NAME, source, priority, addReason.label(), layer);
  }

  protected long getRemovedCount(
      final String source, final String priority, final String operation, final String layer) {
    return metricsSystem.getCounterValue(
        TransactionPoolMetrics.REMOVED_COUNTER_NAME, source, priority, operation, layer);
  }
}
