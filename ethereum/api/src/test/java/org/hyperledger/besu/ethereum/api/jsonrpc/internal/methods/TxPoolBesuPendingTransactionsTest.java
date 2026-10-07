/*
 * Copyright ConsenSys AG.
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
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PendingTransactionsParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionPendingResult;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TxPoolBesuPendingTransactionsTest {

  @Mock private TransactionPool transactionPool;
  private TxPoolBesuPendingTransactions method;
  private final String JSON_RPC_VERSION = "2.0";
  private final String TXPOOL_PENDING_TRANSACTIONS_METHOD = "txpool_besuPendingTransactions";
  private Set<PendingTransaction> listTrx;

  @BeforeEach
  public void setUp() {
    listTrx = getTransactionPool();
    method = new TxPoolBesuPendingTransactions(transactionPool);
    when(transactionPool.getPendingTransactions()).thenReturn(listTrx);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(TXPOOL_PENDING_TRANSACTIONS_METHOD);
  }

  @Test
  public void shouldReturnPendingTransactions() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION, TXPOOL_PENDING_TRANSACTIONS_METHOD, new Object[] {}));

    final JsonRpcSuccessResponse actualResponse = (JsonRpcSuccessResponse) method.response(request);
    final Set<TransactionPendingResult> result =
        (Set<TransactionPendingResult>) actualResponse.getResult();
    assertThat(result.size()).isEqualTo(getTransactionPool().size());
  }

  @Test
  public void pendingTransactionsGasPricesDoNotHaveLeadingZeroes() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION, TXPOOL_PENDING_TRANSACTIONS_METHOD, new Object[] {100}));

    final JsonRpcSuccessResponse actualResponse = (JsonRpcSuccessResponse) method.response(request);
    final Set<TransactionPendingResult> result =
        (Set<TransactionPendingResult>) actualResponse.getResult();

    assertThat(result)
        .extracting(TransactionPendingResult::getGasPrice)
        .filteredOn(Objects::nonNull)
        .allSatisfy(p -> assertThat(p).doesNotContain("0x0"));
    assertThat(result)
        .extracting(TransactionPendingResult::getMaxFeePerGas)
        .filteredOn(Objects::nonNull)
        .allSatisfy(p -> assertThat(p).doesNotContain("0x0"));
    assertThat(result)
        .extracting(TransactionPendingResult::getMaxPriorityFeePerGas)
        .filteredOn(Objects::nonNull)
        .allSatisfy(p -> assertThat(p).doesNotContain("0x0"));
  }

  @Test
  public void shouldReturnPendingTransactionsWithLimit() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION, TXPOOL_PENDING_TRANSACTIONS_METHOD, new Object[] {1}));

    final JsonRpcSuccessResponse actualResponse = (JsonRpcSuccessResponse) method.response(request);

    final Set<TransactionPendingResult> result =
        (Set<TransactionPendingResult>) actualResponse.getResult();
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  public void shouldReturnPendingTransactionsWithFilter() {

    final Map<String, String> fromFilter = new HashMap<>();
    fromFilter.put(
        "eq",
        listTrx.stream().findAny().get().getTransaction().getSender().getBytes().toHexString());

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION,
                TXPOOL_PENDING_TRANSACTIONS_METHOD,
                new Object[] {
                  null,
                  new PendingTransactionsParams(
                      fromFilter,
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>())
                }));

    final JsonRpcSuccessResponse actualResponse = (JsonRpcSuccessResponse) method.response(request);

    final Set<TransactionPendingResult> result =
        (Set<TransactionPendingResult>) actualResponse.getResult();
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  public void shouldReturnPendingTransactionsWithLimitAndFilter() {

    final Map<String, String> fromFilter = new HashMap<>();
    fromFilter.put(
        "eq",
        listTrx.stream().findAny().get().getTransaction().getSender().getBytes().toHexString());

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION,
                TXPOOL_PENDING_TRANSACTIONS_METHOD,
                new Object[] {
                  100,
                  new PendingTransactionsParams(
                      fromFilter,
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>())
                }));

    final JsonRpcSuccessResponse actualResponse = (JsonRpcSuccessResponse) method.response(request);

    final Set<TransactionPendingResult> result =
        (Set<TransactionPendingResult>) actualResponse.getResult();
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  public void shouldReturnsErrorIfInvalidPredicate() {

    final Map<String, String> fromFilter = new HashMap<>();
    fromFilter.put("invalid", "0x0000000000000000000000000000000000000001");

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION,
                TXPOOL_PENDING_TRANSACTIONS_METHOD,
                new Object[] {
                  100,
                  new PendingTransactionsParams(
                      fromFilter,
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>())
                }));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Unknown field expected one of `eq`, `gt`, `lt`, `action`");
  }

  @Test
  public void shouldReturnsErrorIfInvalidNumberOfPredicate() {

    final Map<String, String> fromFilter = new HashMap<>();
    fromFilter.put("eq", "0x0000000000000000000000000000000000000001");
    fromFilter.put("lt", "0x0000000000000000000000000000000000000001");

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION,
                TXPOOL_PENDING_TRANSACTIONS_METHOD,
                new Object[] {
                  100,
                  new PendingTransactionsParams(
                      fromFilter,
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>())
                }));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Only one operator per filter type allowed");
  }

  @Test
  public void shouldReturnsErrorIfInvalidPredicateUsedForFromField() {

    final Map<String, String> fromFilter = new HashMap<>();
    fromFilter.put("lt", "0x0000000000000000000000000000000000000001");

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION,
                TXPOOL_PENDING_TRANSACTIONS_METHOD,
                new Object[] {
                  100,
                  new PendingTransactionsParams(
                      fromFilter,
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>())
                }));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("The `from` filter only supports the `eq` operator");
  }

  @Test
  public void shouldReturnsErrorIfInvalidPredicateUsedForToField() {

    final Map<String, String> toFilter = new HashMap<>();
    toFilter.put("lt", "0x0000000000000000000000000000000000000001");

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION,
                TXPOOL_PENDING_TRANSACTIONS_METHOD,
                new Object[] {
                  100,
                  new PendingTransactionsParams(
                      new HashMap<>(),
                      toFilter,
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>())
                }));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("The `to` filter only supports the `eq` or `action` operator");
  }

  /**
   * EIP-1559 transactions have no gasPrice field at all -- only maxFeePerGas -- so
   * Transaction#getGasPrice is empty for them. The pool here is
   * BlockDataGenerator#transactionsWithAllTypes, which guarantees one, and the filter used to
   * unwrap that empty Optional and fail the whole request.
   *
   * <p>The bound is exact rather than arbitrary: the generator sets both gasPrice and maxFeePerGas
   * from a 4-byte value, so every transaction in this pool is below 2^32 whichever field carries
   * its price. That makes the expected count deterministic.
   */
  @Test
  public void shouldFilterByGasPriceWhenPoolContainsEip1559Transactions() {
    assertThat(
            listTrx.stream().map(pt -> pt.getTransaction().getType()).collect(Collectors.toSet()))
        .as("fixture must actually cover the EIP-1559 case")
        .contains(TransactionType.EIP1559);

    final Map<String, String> gasPriceFilter = new HashMap<>();
    gasPriceFilter.put("lt", "0x100000000");

    final Set<TransactionPendingResult> result = requestWithGasPriceFilter(gasPriceFilter);

    assertThat(result).hasSize(listTrx.size());
  }

  /**
   * Pins what an EIP-1559 transaction's gasPrice resolves to: its maxFeePerGas, not zero and not an
   * error. Filtering for that exact value must match the transaction it was read from.
   */
  @Test
  public void gasPriceOfAnEip1559TransactionResolvesToItsMaxFeePerGas() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Transaction eip1559Transaction = gen.transaction(TransactionType.EIP1559);
    when(transactionPool.getPendingTransactions())
        .thenReturn(Set.of(new PendingTransaction.Local(eip1559Transaction)));

    final Map<String, String> gasPriceFilter = new HashMap<>();
    gasPriceFilter.put("eq", eip1559Transaction.getMaxFeePerGas().orElseThrow().toHexString());

    assertThat(requestWithGasPriceFilter(gasPriceFilter)).hasSize(1);
  }

  /**
   * The limit reaches Stream#limit unchecked, so a negative value used to surface as an
   * IllegalArgumentException and an internal error rather than an invalid-parameter response.
   */
  @Test
  public void shouldRejectNegativeLimit() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION, TXPOOL_PENDING_TRANSACTIONS_METHOD, new Object[] {-1}));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid transaction limit parameter (index 0)");
  }

  private Set<TransactionPendingResult> requestWithGasPriceFilter(
      final Map<String, String> gasPriceFilter) {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                JSON_RPC_VERSION,
                TXPOOL_PENDING_TRANSACTIONS_METHOD,
                new Object[] {
                  null,
                  new PendingTransactionsParams(
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>(),
                      gasPriceFilter,
                      new HashMap<>(),
                      new HashMap<>())
                }));

    final JsonRpcSuccessResponse actualResponse = (JsonRpcSuccessResponse) method.response(request);
    return (Set<TransactionPendingResult>) actualResponse.getResult();
  }

  private Set<PendingTransaction> getTransactionPool() {

    final BlockDataGenerator gen = new BlockDataGenerator();
    return gen.transactionsWithAllTypes(4).stream()
        .map(transaction -> new PendingTransaction.Local(transaction))
        .collect(Collectors.toUnmodifiableSet());
  }
}
