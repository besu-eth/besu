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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPoolResult;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TxPoolInspectTest {

  @Mock private TransactionPool transactionPool;

  private TxPoolInspect method;

  private static final String JSON_RPC_VERSION = "2.0";
  private static final String METHOD_NAME = "txpool_inspect";
  private static final Address SENDER =
      Address.fromHexString("0x1234567890123456789012345678901234567890");
  private static final Address RECIPIENT =
      Address.fromHexString("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  @BeforeEach
  public void setUp() {
    method = new TxPoolInspect(transactionPool);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(METHOD_NAME);
  }

  @Test
  public void shouldReturnEmptyResultWhenPoolIsEmpty() {
    when(transactionPool.getPendingTransactionsBySender()).thenReturn(Map.of());

    final TransactionPoolResult<Map<String, SequencedMap<String, String>>> result = invokeMethod();

    assertThat(result.getPending()).isEmpty();
    assertThat(result.getQueued()).isEmpty();
  }

  @Test
  public void humanReadableViewFormatsLegacyTransactionCorrectly() {
    final Wei gasPrice = Wei.of(50_000_000_000L);
    final Wei value = Wei.of(115_000_000_000_000_000L);
    final long gasLimit = 21_000L;

    final Transaction tx =
        new TransactionTestFixture()
            .sender(SENDER)
            .to(Optional.of(RECIPIENT))
            .nonce(0)
            .gasPrice(gasPrice)
            .gasLimit(gasLimit)
            .value(value)
            .createTransaction(KEY_PAIR);

    final PendingTransaction pendingTx = pendingTxOf(tx);

    final String summary = TxPoolInspect.humanReadableView(pendingTx);

    assertThat(summary)
        .isEqualTo(
            RECIPIENT
                + ": "
                + value.toBigInteger()
                + " wei + "
                + gasLimit
                + " gas × "
                + gasPrice.toBigInteger()
                + " wei");
  }

  @Test
  public void humanReadableViewUsesMaxFeePerGasForEip1559Transaction() {
    final Wei maxFeePerGas = Wei.of(100_000_000_000L);
    final Wei maxPriorityFeePerGas = Wei.of(1_000_000_000L);
    final Wei value = Wei.of(1_000_000_000_000_000L);
    final long gasLimit = 50_000L;

    final Transaction tx =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .sender(SENDER)
            .to(Optional.of(RECIPIENT))
            .nonce(1)
            .maxFeePerGas(Optional.of(maxFeePerGas))
            .maxPriorityFeePerGas(Optional.of(maxPriorityFeePerGas))
            .gasLimit(gasLimit)
            .value(value)
            .createTransaction(KEY_PAIR);

    final String summary = TxPoolInspect.humanReadableView(pendingTxOf(tx));

    assertThat(summary)
        .isEqualTo(
            RECIPIENT
                + ": "
                + value.toBigInteger()
                + " wei + "
                + gasLimit
                + " gas × "
                + maxFeePerGas.toBigInteger()
                + " wei");
  }

  @Test
  public void humanReadableViewUsesContractCreationWhenToIsAbsent() {
    final Wei gasPrice = Wei.of(20_000_000_000L);
    final Wei value = Wei.ZERO;
    final long gasLimit = 100_000L;

    final Transaction tx =
        new TransactionTestFixture()
            .sender(SENDER)
            .to(Optional.empty())
            .nonce(0)
            .gasPrice(gasPrice)
            .gasLimit(gasLimit)
            .value(value)
            .createTransaction(KEY_PAIR);

    final String summary = TxPoolInspect.humanReadableView(pendingTxOf(tx));

    assertThat(summary)
        .isEqualTo(
            "contract creation: 0 wei + " + gasLimit + " gas × " + gasPrice.toBigInteger() + " wei");
  }

  @Test
  public void shouldReturnAllTransactionsAsPendingWhenAllAreConsecutive() {
    final PendingTransaction tx0 = pendingTxWith(0, Wei.of(1_000_000_000L));
    final PendingTransaction tx1 = pendingTxWith(1, Wei.of(1_000_000_000L));

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(SENDER, new SenderPendingTransactionsData(SENDER, 0L, List.of(tx0, tx1))));

    final TransactionPoolResult<Map<String, SequencedMap<String, String>>> result = invokeMethod();

    assertThat(result.getPending()).containsOnlyKeys(SENDER.toString());
    assertThat(result.getPending().get(SENDER.toString())).containsOnlyKeys("0", "1");
    assertThat(result.getQueued()).isEmpty();
  }

  @Test
  public void shouldSplitTransactionsIntoPendingAndQueued() {
    final PendingTransaction tx0 = pendingTxWith(0, Wei.of(1_000_000_000L));
    final PendingTransaction tx2 = pendingTxWith(2, Wei.of(1_000_000_000L));

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(SENDER, new SenderPendingTransactionsData(SENDER, 0L, List.of(tx0, tx2))));

    final TransactionPoolResult<Map<String, SequencedMap<String, String>>> result = invokeMethod();

    assertThat(result.getPending().get(SENDER.toString())).containsOnlyKeys("0");
    assertThat(result.getQueued().get(SENDER.toString())).containsOnlyKeys("2");
  }

  @Test
  public void summaryValuesAreStringsNotTransactionObjects() {
    final PendingTransaction tx0 = pendingTxWith(0, Wei.of(1_000_000_000L));

    when(transactionPool.getPendingTransactionsBySender())
        .thenReturn(
            Map.of(SENDER, new SenderPendingTransactionsData(SENDER, 0L, List.of(tx0))));

    final TransactionPoolResult<Map<String, SequencedMap<String, String>>> result = invokeMethod();

    final String summary = result.getPending().get(SENDER.toString()).get("0");
    assertThat(summary).isInstanceOf(String.class);
    assertThat(summary).contains("wei").contains("gas").contains("×");
  }

  @SuppressWarnings("unchecked")
  private TransactionPoolResult<Map<String, SequencedMap<String, String>>> invokeMethod() {
    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse)
            method.response(
                new JsonRpcRequestContext(
                    new JsonRpcRequest(JSON_RPC_VERSION, METHOD_NAME, new Object[] {})));
    return (TransactionPoolResult<Map<String, SequencedMap<String, String>>>) response.getResult();
  }

  private PendingTransaction pendingTxOf(final Transaction tx) {
    final PendingTransaction pendingTransaction = mock(PendingTransaction.class);
    when(pendingTransaction.getTransaction()).thenReturn(tx);
    return pendingTransaction;
  }

  private PendingTransaction pendingTxWith(final long nonce, final Wei gasPrice) {
    final Transaction tx =
        new TransactionTestFixture()
            .sender(SENDER)
            .to(Optional.of(RECIPIENT))
            .nonce(nonce)
            .gasPrice(gasPrice)
            .createTransaction(KEY_PAIR);
    final PendingTransaction pendingTransaction = pendingTxOf(tx);
    when(pendingTransaction.getNonce()).thenReturn(nonce);
    return pendingTransaction;
  }
}
