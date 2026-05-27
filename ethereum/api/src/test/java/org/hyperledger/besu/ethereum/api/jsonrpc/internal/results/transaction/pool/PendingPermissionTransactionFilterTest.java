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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.transaction.pool;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.transaction.pool.Predicate.ACTION;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.transaction.pool.Predicate.EQ;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.transaction.pool.Predicate.GT;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.transaction.pool.Predicate.LT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.transaction.pool.PendingTransactionFilter.Filter;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PendingPermissionTransactionFilterTest {

  public static Collection<Object[]> data() {
    return asList(
        new Object[][] {
          {
            singletonList(new Filter("from", "0x0000000000000000000000000000000000000001", EQ)),
            100,
            singletonList("1")
          },
          {
            singletonList(new Filter("to", "0x0000000000000000000000000000000000000002", EQ)),
            100,
            singletonList("1")
          },
          {singletonList(new Filter("gas", "0x01", EQ)), 100, singletonList("1")},
          {singletonList(new Filter("gas", "0x01", LT)), 100, EMPTY_LIST},
          {singletonList(new Filter("gas", "0x01", GT)), 100, asList("2", "3", "4")},
          {singletonList(new Filter("gas", "0x01", GT)), 1, singletonList("2")},
          {singletonList(new Filter("gasPrice", "0x01", EQ)), 100, singletonList("1")},
          {singletonList(new Filter("gasPrice", "0x01", LT)), 100, EMPTY_LIST},
          {singletonList(new Filter("gasPrice", "0x01", GT)), 100, asList("2", "3", "4")},
          {singletonList(new Filter("gasPrice", "0x01", GT)), 1, singletonList("2")},
          {singletonList(new Filter("value", "0x01", EQ)), 100, singletonList("1")},
          {singletonList(new Filter("value", "0x01", LT)), 100, EMPTY_LIST},
          {singletonList(new Filter("value", "0x01", GT)), 100, asList("2", "3", "4")},
          {singletonList(new Filter("value", "0x01", GT)), 1, singletonList("2")},
          {singletonList(new Filter("nonce", "0x01", EQ)), 100, singletonList("1")},
          {singletonList(new Filter("nonce", "0x01", LT)), 100, EMPTY_LIST},
          {singletonList(new Filter("nonce", "0x01", GT)), 100, asList("2", "3", "4")},
          {singletonList(new Filter("nonce", "0x01", GT)), 1, singletonList("2")},
          {
            asList(new Filter("gas", "0x03", GT), new Filter("gasPrice", "0x02", GT)),
            100,
            singletonList("4")
          },
          {
            asList(new Filter("from", "0x01", EQ), new Filter("gasPrice", "0x02", GT)),
            100,
            EMPTY_LIST
          },
          {singletonList(new Filter("to", "contract_creation", ACTION)), 1, singletonList("4")},
        });
  }

  private final PendingTransactionFilter pendingTransactionFilter = new PendingTransactionFilter();

  @ParameterizedTest
  @MethodSource("data")
  public void PendingPermissionTransactionFilterTest(
      final List<Filter> filters,
      final int limit,
      final List<String> expectedListOfTransactionHash) {

    final Collection<Transaction> filteredList =
        pendingTransactionFilter.reduce(getPendingTransactions(), filters, limit);

    assertThat(filteredList.size()).isEqualTo(expectedListOfTransactionHash.size());
    for (Transaction trx : filteredList) {
      assertThat(expectedListOfTransactionHash)
          .contains(String.valueOf(trx.getHash().getBytes().toBigInteger()));
    }
  }

  private Set<PendingTransaction> getPendingTransactions() {
    final List<PendingTransaction> pendingTransactionList = new ArrayList<>();
    final int numberTrx = 5;
    for (int i = 1; i < numberTrx; i++) {
      Transaction transaction = mock(Transaction.class);
      when(transaction.getGasPrice()).thenReturn(Optional.of(Wei.of(i)));
      when(transaction.getValue()).thenReturn(Wei.of(i));
      when(transaction.getGasLimit()).thenReturn((long) i);
      when(transaction.getNonce()).thenReturn((long) i);
      when(transaction.getSender()).thenReturn(Address.fromHexString(String.valueOf(i)));
      when(transaction.getTo())
          .thenReturn(Optional.of(Address.fromHexString(String.valueOf(i + 1))));
      when(transaction.getHash()).thenReturn(Hash.fromHexStringLenient(String.valueOf(i)));
      if (i == numberTrx - 1) {
        when(transaction.isContractCreation()).thenReturn(true);
      }
      pendingTransactionList.add(new PendingTransaction.Local(transaction));
    }
    return new LinkedHashSet<>(pendingTransactionList);
  }
}
