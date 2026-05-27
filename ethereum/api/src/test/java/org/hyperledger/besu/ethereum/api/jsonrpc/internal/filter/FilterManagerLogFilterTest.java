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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FilterManagerLogFilterTest {

  private FilterManager filterManager;

  @Mock private Blockchain blockchain;
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private TransactionPool transactionPool;
  @Spy private final FilterRepository filterRepository = new FilterRepository();

  @BeforeEach
  public void setupTest() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    this.filterManager =
        new FilterManagerBuilder()
            .blockchainQueries(blockchainQueries)
            .transactionPool(transactionPool)
            .filterRepository(filterRepository)
            .build();
  }

  @Test
  public void installUninstallNewLogFilter() {
    final String filterId = filterManager.installLogFilter(latest(), latest(), logsQuery());
    assertThat(filterRepository.exists(filterId)).isTrue();

    assertThat(filterManager.uninstallFilter(filterId)).isTrue();
    assertThat(filterRepository.exists(filterId)).isFalse();

    assertThat(filterManager.blockChanges(filterId)).isNull();
  }

  @Test
  public void shouldCheckMatchingLogsWhenRecordedNewBlockEventForPrivateFiltersOnly() {
    filterManager.installLogFilter(latest(), latest(), logsQuery());

    verify(blockchainQueries, never()).matchingLogs(eq(100L), eq(100L), eq(logsQuery()), any());
  }

  @Test
  public void shouldUseBlockHashWhenCheckingLogsForChangesForPrivateFiltersOnly() {
    filterManager.installLogFilter(blockNum(1L), blockNum(10L), logsQuery());

    verify(blockchainQueries, never()).matchingLogs(any(Hash.class), any(LogsQuery.class), any());
  }

  @Test
  public void shouldReturnLogWhenLogFilterMatches() {

    final String filterId = filterManager.installLogFilter(latest(), latest(), logsQuery());
    final List<LogWithMetadata> expectedLogs = recordBlockEvents(1).get(0).getLogsWithMetadata();

    final List<LogWithMetadata> retrievedLogs = filterManager.logsChanges(filterId);

    assertThat(retrievedLogs).usingRecursiveComparison().isEqualTo(expectedLogs);
  }

  @Test
  public void shouldNotReturnLogOnNewBlockIfToBlockIsInPast() {
    final String filterId =
        filterManager.installLogFilter(
            new BlockParameter("0x0"), new BlockParameter("0x1"), logsQuery());
    recordBlockEvents(1).get(0);

    final List<LogWithMetadata> retrievedLogs = filterManager.logsChanges(filterId);

    assertThat(retrievedLogs).isEqualTo(emptyList());
  }

  @Test
  public void shouldNotQueryOnNewBlock() {
    filterManager.installLogFilter(latest(), latest(), logsQuery());
    filterManager.installLogFilter(latest(), latest(), logsQuery());
    filterManager.installLogFilter(latest(), latest(), logsQuery());
    recordBlockEvents(1);

    verify(blockchainQueries, never()).matchingLogs(anyLong(), anyLong(), any(), any());
  }

  @Test
  public void shouldReturnNullWhenForAbsentLogFilter() {
    final List<LogWithMetadata> logs = filterManager.logsChanges("NOT THERE");

    assertThat(logs).isNull();
    verify(blockchainQueries, never()).matchingLogs(anyLong(), anyLong(), any(), any());
  }

  @Test
  public void shouldClearLogsAfterGettingLogChanges() {
    final String filterId = filterManager.installLogFilter(latest(), latest(), logsQuery());
    recordBlockEvents(2);

    assertThat(filterManager.logsChanges(filterId).size()).isEqualTo(8);
    assertThat(filterManager.logsChanges(filterId).size()).isEqualTo(0);
  }

  private List<BlockAddedEvent> recordBlockEvents(final int numEvents) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final List<BlockAddedEvent> blockAddedEvents =
        Stream.generate(
                () -> {
                  final Block block =
                      gen.block(new BlockDataGenerator.BlockOptions().setBlockNumber(3));
                  return BlockAddedEvent.createForHeadAdvancement(
                      block,
                      LogWithMetadata.generate(block, gen.receipts(block), false),
                      emptyList());
                })
            .limit(numEvents)
            .collect(toUnmodifiableList());
    blockAddedEvents.forEach(event -> filterManager.recordBlockEvent(event));
    return blockAddedEvents;
  }

  @Test
  public void getLogsForAbsentFilterReturnsNull() {
    assertThat(filterManager.logs("NOTTHERE")).isNull();
  }

  @Test
  public void getLogsForExistingFilterReturnsResults() {
    final LogWithMetadata log = logWithMetadata();
    when(blockchainQueries.headBlockNumber()).thenReturn(100L);
    when(blockchainQueries.matchingLogs(eq(100L), eq(100L), eq(logsQuery()), any()))
        .thenReturn(singletonList(log));

    final String filterId = filterManager.installLogFilter(latest(), latest(), logsQuery());
    final List<LogWithMetadata> retrievedLogs = filterManager.logs(filterId);

    assertThat(retrievedLogs).usingRecursiveComparison().isEqualTo(singletonList(log));
  }

  @Test
  public void getLogsChangesShouldResetFilterExpireDate() {
    final LogFilter filter = spy(new LogFilter("foo", latest(), latest(), logsQuery()));
    doReturn(Optional.of(filter)).when(filterRepository).getFilter(eq("foo"), eq(LogFilter.class));

    filterManager.logsChanges("foo");

    verify(filter).resetExpireTime();
  }

  @Test
  public void getLogsShouldResetFilterExpireDate() {
    final LogFilter filter = spy(new LogFilter("foo", latest(), latest(), logsQuery()));
    doReturn(Optional.of(filter)).when(filterRepository).getFilter(eq("foo"), eq(LogFilter.class));

    filterManager.logs("foo");

    verify(filter).resetExpireTime();
  }

  private LogWithMetadata logWithMetadata() {
    return new LogWithMetadata(
        0,
        100L,
        Hash.ZERO,
        0L,
        Hash.ZERO,
        0,
        Address.fromHexString("0x0"),
        Bytes.EMPTY,
        Lists.newArrayList(),
        false);
  }

  private LogsQuery logsQuery() {
    return new LogsQuery(emptyList(), emptyList()); // matches everything
  }

  private BlockParameter latest() {
    return new BlockParameter("latest");
  }

  private BlockParameter blockNum(final long blockNum) {
    return new BlockParameter(Quantity.create(blockNum));
  }
}
