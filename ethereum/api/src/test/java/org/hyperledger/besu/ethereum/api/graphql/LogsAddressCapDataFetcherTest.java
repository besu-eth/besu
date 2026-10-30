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
package org.hyperledger.besu.ethereum.api.graphql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.LogAdapter;
import org.hyperledger.besu.ethereum.chain.Blockchain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import graphql.schema.DataFetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Regression coverage for the address-count cap on GraphQL {@code logs(filter)}: the limit that
 * GHSA-p6f8-q9mp-7mj9 added to {@code eth_newFilter} and {@code eth_subscribe} was still bypassable
 * here with an unbounded address list, and each address costs a bloom lookup per block.
 */
@ExtendWith(MockitoExtension.class)
class LogsAddressCapDataFetcherTest extends AbstractDataFetcherTest {

  private static final long CHAIN_HEAD = 100L;

  private final Blockchain blockchain = mock(Blockchain.class);

  @BeforeEach
  @Override
  public void before() {
    super.before();
    when(graphQLContext.get(GraphQLContextType.BLOCKCHAIN_QUERIES)).thenReturn(query);
    when(query.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(CHAIN_HEAD);
  }

  private DataFetcher<Optional<List<LogAdapter>>> fetcherWithMaxAddresses(final int maxAddresses) {
    return new GraphQLDataFetchers(supportedCapabilities, 5000L, maxAddresses).getLogsDataFetcher();
  }

  private void withFilter(final int addressCount) {
    when(environment.getArgument("filter"))
        .thenReturn(
            Map.of(
                "fromBlock",
                0L,
                "toBlock",
                0L,
                "addresses",
                addresses(addressCount),
                "topics",
                List.of()));
  }

  private static List<Address> addresses(final int count) {
    final List<Address> addresses = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      addresses.add(Address.fromHexString(String.format("0x%040x", i)));
    }
    return addresses;
  }

  @Test
  void rejectsMoreAddressesThanTheCap() {
    withFilter(3);

    assertThatThrownBy(() -> fetcherWithMaxAddresses(2).get(environment))
        .isInstanceOf(GraphQLException.class);
    verify(query, never()).matchingLogs(anyLong(), anyLong(), any(), any());
  }

  @Test
  void allowsAsManyAddressesAsTheCap() throws Exception {
    withFilter(2);
    when(query.matchingLogs(anyLong(), anyLong(), any(), any())).thenReturn(List.of());

    final Optional<List<LogAdapter>> result = fetcherWithMaxAddresses(2).get(environment);

    assertThat(result).isPresent();
    assertThat(result.get()).isEmpty();
  }

  @Test
  void zeroMaxAddressesMeansNoAddressCap() throws Exception {
    withFilter(3);
    when(query.matchingLogs(anyLong(), anyLong(), any(), any())).thenReturn(List.of());

    final Optional<List<LogAdapter>> result = fetcherWithMaxAddresses(0).get(environment);

    assertThat(result).isPresent();
  }
}
