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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPendingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPoolResult;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TxPoolContentTest {

  @Mock private TransactionPool transactionPool;

  private TxPoolContent method;

  private static final String JSON_RPC_VERSION = "2.0";
  private static final String METHOD_NAME = "txpool_content";
  private static final Address SENDER_A =
      Address.fromHexString("0x1234567890123456789012345678901234567890");
  private static final Address SENDER_B =
      Address.fromHexString("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  @BeforeEach
  public void setUp() {
    method = new TxPoolContent(transactionPool);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(METHOD_NAME);
  }

  @Test
  public void shouldReturnEmptyResultWhenPoolIsEmpty() {
    when(transactionPool.getPendingTransactionsBySender()).thenReturn(Map.of());

    final TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>
        result = invokeMethod();

    assertThat(result.getPending()).isEmpty();
    assertThat(result.getQueued()).isEmpty();
  }

  @Test
  public void shouldReturnAllTransactionsAsPendingWhenAllAreConsecutive() {
    // Nonce = 0, txs at nonces 0, 1, 2 → all pending, none queued
    final PendingTransaction tx0 = pendingTx(SENDER_A, 0);
    final PendingTransaction tx1 = pendingTx(SENDER_A, 1);
    final PendingTransaction tx2 = pendingTx(SENDER_A, 2);

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(
                SENDER_A, new SenderPendingTransactionsData(SENDER_A, 0L, List.of(tx0, tx1, tx2))));

    final TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>
        result = invokeMethod();

    assertThat(result.getPending()).containsOnlyKeys(SENDER_A.toString());
    assertThat(result.getPending().get(SENDER_A.toString())).containsOnlyKeys("0", "1", "2");
    assertThat(result.getQueued()).isEmpty();
  }

  @Test
  public void shouldReturnAllTransactionsAsQueuedWhenGapExistsAtStart() {
    // Nonce = 0, but first tx has nonce 2 → all queued, none pending
    final PendingTransaction tx2 = pendingTx(SENDER_A, 2);
    final PendingTransaction tx3 = pendingTx(SENDER_A, 3);

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(SENDER_A, new SenderPendingTransactionsData(SENDER_A, 0L, List.of(tx2, tx3))));

    final TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>
        result = invokeMethod();

    assertThat(result.getPending()).isEmpty();
    assertThat(result.getQueued()).containsOnlyKeys(SENDER_A.toString());
    assertThat(result.getQueued().get(SENDER_A.toString())).containsOnlyKeys("2", "3");
  }

  @Test
  public void shouldSplitTransactionsIntoPendingAndQueuedForSingleSender() {
    // Nonce = 0, txs at nonces 0, 1, 3, 4 → pending: [0,1], queued: [3,4]
    final PendingTransaction tx0 = pendingTx(SENDER_A, 0);
    final PendingTransaction tx1 = pendingTx(SENDER_A, 1);
    final PendingTransaction tx3 = pendingTx(SENDER_A, 3);
    final PendingTransaction tx4 = pendingTx(SENDER_A, 4);

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(
                SENDER_A,
                new SenderPendingTransactionsData(SENDER_A, 0L, List.of(tx0, tx1, tx3, tx4))));

    final TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>
        result = invokeMethod();

    assertThat(result.getPending()).containsOnlyKeys(SENDER_A.toString());
    assertThat(result.getPending().get(SENDER_A.toString())).containsOnlyKeys("0", "1");
    assertThat(result.getQueued()).containsOnlyKeys(SENDER_A.toString());
    assertThat(result.getQueued().get(SENDER_A.toString())).containsOnlyKeys("3", "4");
  }

  @Test
  public void shouldHandleMultipleSendersWithIndependentPendingAndQueuedSets() {
    // SENDER_A: nonce=0, txs [0,1,3] → pending: [0,1], queued: [3]
    // SENDER_B: nonce=5, txs [5,6]   → pending: [5,6], queued: []
    final PendingTransaction txA0 = pendingTx(SENDER_A, 0);
    final PendingTransaction txA1 = pendingTx(SENDER_A, 1);
    final PendingTransaction txA3 = pendingTx(SENDER_A, 3);
    final PendingTransaction txB5 = pendingTx(SENDER_B, 5);
    final PendingTransaction txB6 = pendingTx(SENDER_B, 6);

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(
                SENDER_A,
                    new SenderPendingTransactionsData(SENDER_A, 0L, List.of(txA0, txA1, txA3)),
                SENDER_B, new SenderPendingTransactionsData(SENDER_B, 5L, List.of(txB5, txB6))));

    final TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>
        result = invokeMethod();

    assertThat(result.getPending()).containsOnlyKeys(SENDER_A.toString(), SENDER_B.toString());
    assertThat(result.getPending().get(SENDER_A.toString())).containsOnlyKeys("0", "1");
    assertThat(result.getPending().get(SENDER_B.toString())).containsOnlyKeys("5", "6");
    assertThat(result.getQueued()).containsOnlyKeys(SENDER_A.toString());
    assertThat(result.getQueued().get(SENDER_A.toString())).containsOnlyKeys("3");
  }

  @Test
  public void shouldDefaultToNonceZeroWhenAccountNonceIsAbsent() {
    // nonce=0 (default when absent), tx at nonce 0 → pending
    final PendingTransaction tx0 = pendingTx(SENDER_A, 0);

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(SENDER_A, new SenderPendingTransactionsData(SENDER_A, 0L, List.of(tx0))));

    final TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>
        result = invokeMethod();

    assertThat(result.getPending().get(SENDER_A.toString())).containsOnlyKeys("0");
    assertThat(result.getQueued()).isEmpty();
  }

  @Test
  public void shouldHandleSenderWithOnlyQueuedTransactionsNotAppearsInPending() {
    // Sender with only queued txs should appear in queued map but NOT in pending map
    final PendingTransaction tx5 = pendingTx(SENDER_A, 5);

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(SENDER_A, new SenderPendingTransactionsData(SENDER_A, 0L, List.of(tx5))));

    final TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>
        result = invokeMethod();

    assertThat(result.getPending()).doesNotContainKey(SENDER_A.toString());
    assertThat(result.getQueued()).containsOnlyKeys(SENDER_A.toString());
    assertThat(result.getQueued().get(SENDER_A.toString())).containsOnlyKeys("5");
  }

  @Test
  public void shouldPreserveNonceOrderingInPendingResult() {
    // Nonces must appear in ascending order in the result map
    final PendingTransaction tx0 = pendingTx(SENDER_A, 0);
    final PendingTransaction tx1 = pendingTx(SENDER_A, 1);
    final PendingTransaction tx2 = pendingTx(SENDER_A, 2);

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(
                SENDER_A, new SenderPendingTransactionsData(SENDER_A, 0L, List.of(tx0, tx1, tx2))));

    final TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>
        result = invokeMethod();

    assertThat(result.getPending().get(SENDER_A.toString()).sequencedKeySet())
        .containsExactly("0", "1", "2");
  }

  @SuppressWarnings("unchecked")
  private TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>
      invokeMethod() {
    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse)
            method.response(
                new JsonRpcRequestContext(
                    new JsonRpcRequest(JSON_RPC_VERSION, METHOD_NAME, new Object[] {})));
    return (TransactionPoolResult<Map<String, SequencedMap<String, TransactionPendingResult>>>)
        response.getResult();
  }

  private PendingTransaction pendingTx(final Address sender, final long nonce) {
    final Transaction tx =
        new TransactionTestFixture().sender(sender).nonce(nonce).createTransaction(KEY_PAIR);
    final PendingTransaction pendingTransaction = mock(PendingTransaction.class);
    lenient().when(pendingTransaction.getNonce()).thenReturn(nonce);
    when(pendingTransaction.getTransaction()).thenReturn(tx);
    return pendingTransaction;
  }
}
