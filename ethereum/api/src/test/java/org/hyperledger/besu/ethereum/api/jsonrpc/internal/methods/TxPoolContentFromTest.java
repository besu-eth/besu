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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPoolContentFromResult;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TxPoolContentFromTest {

  @Mock private TransactionPool transactionPool;

  private TxPoolContentFrom method;

  private static final String JSON_RPC_VERSION = "2.0";
  private static final String METHOD_NAME = "txpool_contentFrom";
  private static final Address SENDER =
      Address.fromHexString("0x1234567890123456789012345678901234567890");
  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  @BeforeEach
  public void setUp() {
    method = new TxPoolContentFrom(transactionPool);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(METHOD_NAME);
  }

  @Test
  public void shouldReturnEmptyResultForSenderWithNoTransactions() {
    when(transactionPool.getPendingTransactionsFor(SENDER))
        .thenReturn(SenderPendingTransactionsData.empty(SENDER));

    final TransactionPoolContentFromResult result = invokeMethod();

    assertThat(result.getPending()).isEmpty();
    assertThat(result.getQueued()).isEmpty();
  }

  @Test
  public void shouldReturnAllTransactionsAsPendingWhenAllAreConsecutive() {
    // Nonce = 0, txs at nonces 0, 1, 2 → all pending, none queued
    final PendingTransaction tx0 = pendingTx(0);
    final PendingTransaction tx1 = pendingTx(1);
    final PendingTransaction tx2 = pendingTx(2);

    when(transactionPool.getPendingTransactionsFor(SENDER))
        .thenReturn(new SenderPendingTransactionsData(SENDER, 0L, List.of(tx0, tx1, tx2)));

    final TransactionPoolContentFromResult result = invokeMethod();

    assertThat(result.getPending()).containsOnlyKeys("0", "1", "2");
    assertThat(result.getQueued()).isEmpty();
  }

  @Test
  public void shouldReturnAllTransactionsAsQueuedWhenGapExistsAtStart() {
    // Nonce = 0, but first tx has nonce 2 → all queued, none pending
    final PendingTransaction tx2 = pendingTx(2);
    final PendingTransaction tx3 = pendingTx(3);

    when(transactionPool.getPendingTransactionsFor(SENDER))
        .thenReturn(new SenderPendingTransactionsData(SENDER, 0L, List.of(tx2, tx3)));

    final TransactionPoolContentFromResult result = invokeMethod();

    assertThat(result.getPending()).isEmpty();
    assertThat(result.getQueued()).containsOnlyKeys("2", "3");
  }

  @Test
  public void shouldSplitTransactionsIntoPendingAndQueued() {
    // Nonce = 0, txs at nonces 0, 1, 3, 4 → pending: [0,1], queued: [3,4]
    final PendingTransaction tx0 = pendingTx(0);
    final PendingTransaction tx1 = pendingTx(1);
    final PendingTransaction tx3 = pendingTx(3);
    final PendingTransaction tx4 = pendingTx(4);

    when(transactionPool.getPendingTransactionsFor(SENDER))
        .thenReturn(new SenderPendingTransactionsData(SENDER, 0L, List.of(tx0, tx1, tx3, tx4)));

    final TransactionPoolContentFromResult result = invokeMethod();

    assertThat(result.getPending()).containsOnlyKeys("0", "1");
    assertThat(result.getQueued()).containsOnlyKeys("3", "4");
  }

  @Test
  public void shouldHandleMidNonceAccountState() {
    // Account has mined nonces 0-4; pool has nonces 5, 6, 8 → pending: [5,6], queued: [8]
    final PendingTransaction tx5 = pendingTx(5);
    final PendingTransaction tx6 = pendingTx(6);
    final PendingTransaction tx8 = pendingTx(8);

    when(transactionPool.getPendingTransactionsFor(SENDER))
        .thenReturn(new SenderPendingTransactionsData(SENDER, 5L, List.of(tx5, tx6, tx8)));

    final TransactionPoolContentFromResult result = invokeMethod();

    assertThat(result.getPending()).containsOnlyKeys("5", "6");
    assertThat(result.getQueued()).containsOnlyKeys("8");
  }

  @Test
  public void shouldThrowInvalidJsonRpcParametersWhenAddressParamIsMissing() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(JSON_RPC_VERSION, METHOD_NAME, new Object[] {}));

    assertThatThrownBy(() -> method.response(request)).isInstanceOf(InvalidJsonRpcParameters.class);
  }

  private TransactionPoolContentFromResult invokeMethod() {
    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) method.response(buildRequest(SENDER));
    return (TransactionPoolContentFromResult) response.getResult();
  }

  private JsonRpcRequestContext buildRequest(final Address sender) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest(JSON_RPC_VERSION, METHOD_NAME, new Object[] {sender.toString()}));
  }

  private PendingTransaction pendingTx(final long nonce) {
    final Transaction tx =
        new TransactionTestFixture().sender(SENDER).nonce(nonce).createTransaction(KEY_PAIR);
    final PendingTransaction pendingTransaction = mock(PendingTransaction.class);
    lenient().when(pendingTransaction.getNonce()).thenReturn(nonce);
    when(pendingTransaction.getTransaction()).thenReturn(tx);
    return pendingTransaction;
  }
}
