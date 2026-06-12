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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FilterManagerTest {

  private BlockDataGenerator blockGenerator;
  private Block currentBlock;
  private FilterManager filterManager;

  @Mock private Blockchain blockchain;
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private TransactionPool transactionPool;
  @Spy final FilterRepository filterRepository = new FilterRepository();

  @BeforeEach
  public void setupTest() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    this.blockGenerator = new BlockDataGenerator();
    this.currentBlock = blockGenerator.genesisBlock();
    this.filterManager =
        new FilterManagerBuilder()
            .blockchainQueries(blockchainQueries)
            .transactionPool(transactionPool)
            .filterRepository(filterRepository)
            .build();
  }

  @Test
  public void uninstallNonexistentFilter() {
    assertThat(filterManager.uninstallFilter("1")).isFalse();
  }

  @Test
  public void installUninstallNewBlockFilter() {
    final String filterId = filterManager.installBlockFilter();
    assertThat(filterRepository.exists(filterId)).isTrue();

    assertThat(filterManager.uninstallFilter(filterId)).isTrue();
    assertThat(filterRepository.exists(filterId)).isFalse();

    assertThat(filterManager.blockChanges(filterId)).isNull();
  }

  @Test
  public void newBlockChanges_SingleFilter() {
    final String filterId = filterManager.installBlockFilter();
    assertThat(filterManager.blockChanges(filterId).size()).isEqualTo(0);

    final Hash blockHash1 = appendBlockToBlockchain();
    final List<Hash> expectedHashes = Lists.newArrayList(blockHash1);
    assertThat(filterManager.blockChanges(filterId)).isEqualTo(expectedHashes);

    // Check that changes have been flushed.
    expectedHashes.clear();
    assertThat(filterManager.blockChanges(filterId)).isEqualTo(expectedHashes);

    final Hash blockHash2 = appendBlockToBlockchain();
    expectedHashes.add(blockHash2);
    final Hash blockHash3 = appendBlockToBlockchain();
    expectedHashes.add(blockHash3);
    assertThat(filterManager.blockChanges(filterId)).isEqualTo(expectedHashes);
  }

  @Test
  public void newBlockChanges_MultipleFilters() {
    final String filterId1 = filterManager.installBlockFilter();
    assertThat(filterManager.blockChanges(filterId1).size()).isEqualTo(0);

    final Hash blockHash1 = appendBlockToBlockchain();
    final List<Hash> expectedHashes1 = Lists.newArrayList(blockHash1);

    final String filterId2 = filterManager.installBlockFilter();
    final Hash blockHash2 = appendBlockToBlockchain();
    expectedHashes1.add(blockHash2);
    final List<Hash> expectedHashes2 = Lists.newArrayList(blockHash2);
    assertThat(filterManager.blockChanges(filterId1)).isEqualTo(expectedHashes1);
    assertThat(filterManager.blockChanges(filterId2)).isEqualTo(expectedHashes2);
    expectedHashes1.clear();
    expectedHashes2.clear();

    // Both filters have been flushed.
    assertThat(filterManager.blockChanges(filterId1)).isEqualTo(expectedHashes1);
    assertThat(filterManager.blockChanges(filterId2)).isEqualTo(expectedHashes2);

    final Hash blockHash3 = appendBlockToBlockchain();
    expectedHashes1.add(blockHash3);
    expectedHashes2.add(blockHash3);

    // Flush the first filter.
    assertThat(filterManager.blockChanges(filterId1)).isEqualTo(expectedHashes1);
    expectedHashes1.clear();

    final Hash blockHash4 = appendBlockToBlockchain();
    expectedHashes1.add(blockHash4);
    expectedHashes2.add(blockHash4);
    assertThat(filterManager.blockChanges(filterId1)).isEqualTo(expectedHashes1);
    assertThat(filterManager.blockChanges(filterId2)).isEqualTo(expectedHashes2);
  }

  @Test
  public void installUninstallPendingTransactionFilter() {
    final String filterId = filterManager.installPendingTransactionFilter();
    verify(filterRepository).save(any(Filter.class));

    assertThat(filterManager.uninstallFilter(filterId)).isTrue();
    verify(filterRepository).delete(eq(filterId));
  }

  @Test
  public void getTransactionChanges_SingleFilter() {
    final String filterId = filterManager.installPendingTransactionFilter();
    assertThat(filterManager.pendingTransactionChanges(filterId).size()).isEqualTo(0);

    final Hash transactionHash1 = receivePendingTransaction();
    final List<Hash> expectedHashes = Lists.newArrayList(transactionHash1);
    assertThat(filterManager.pendingTransactionChanges(filterId)).isEqualTo(expectedHashes);

    // Check that changes have been flushed.
    expectedHashes.clear();
    assertThat(filterManager.pendingTransactionChanges(filterId)).isEqualTo(expectedHashes);

    final Hash transactionHash2 = receivePendingTransaction();
    expectedHashes.add(transactionHash2);
    final Hash transactionHash3 = receivePendingTransaction();
    expectedHashes.add(transactionHash3);
    assertThat(filterManager.pendingTransactionChanges(filterId)).isEqualTo(expectedHashes);
  }

  @Test
  public void getPendingTransactionChanges_MultipleFilters() {
    final String filterId1 = filterManager.installPendingTransactionFilter();
    assertThat(filterManager.pendingTransactionChanges(filterId1).size()).isEqualTo(0);

    final Hash transactionHash1 = receivePendingTransaction();
    final List<Hash> expectedHashes1 = Lists.newArrayList(transactionHash1);

    final String filterId2 = filterManager.installPendingTransactionFilter();
    final Hash transactionHash2 = receivePendingTransaction();
    expectedHashes1.add(transactionHash2);
    final List<Hash> expectedHashes2 = Lists.newArrayList(transactionHash2);
    assertThat(filterManager.pendingTransactionChanges(filterId1)).isEqualTo(expectedHashes1);
    assertThat(filterManager.pendingTransactionChanges(filterId2)).isEqualTo(expectedHashes2);
    expectedHashes1.clear();
    expectedHashes2.clear();

    // Both filters have been flushed.
    assertThat(filterManager.pendingTransactionChanges(filterId1)).isEqualTo(expectedHashes1);
    assertThat(filterManager.pendingTransactionChanges(filterId2)).isEqualTo(expectedHashes2);

    final Hash transactionHash3 = receivePendingTransaction();
    expectedHashes1.add(transactionHash3);
    expectedHashes2.add(transactionHash3);

    // Flush the first filter.
    assertThat(filterManager.pendingTransactionChanges(filterId1)).isEqualTo(expectedHashes1);
    expectedHashes1.clear();

    final Hash transactionHash4 = receivePendingTransaction();
    expectedHashes1.add(transactionHash4);
    expectedHashes2.add(transactionHash4);
    assertThat(filterManager.pendingTransactionChanges(filterId1)).isEqualTo(expectedHashes1);
    assertThat(filterManager.pendingTransactionChanges(filterId2)).isEqualTo(expectedHashes2);
  }

  @Test
  public void getBlockChangesShouldResetFilterExpireDate() {
    final BlockFilter filter = spy(new BlockFilter("foo"));
    doReturn(Optional.of(filter))
        .when(filterRepository)
        .getFilter(eq("foo"), eq(BlockFilter.class));

    filterManager.blockChanges("foo");

    verify(filter).resetExpireTime();
  }

  @Test
  public void getPendingTransactionsChangesShouldResetFilterExpireDate() {
    final PendingTransactionFilter filter = spy(new PendingTransactionFilter("foo"));
    doReturn(Optional.of(filter))
        .when(filterRepository)
        .getFilter(eq("foo"), eq(PendingTransactionFilter.class));

    filterManager.pendingTransactionChanges("foo");

    verify(filter).resetExpireTime();
  }

  @Test
  public void logsForLatestLatestFilterResolvesHeadOnce() {
    // A filter installed with no fromBlock/toBlock defaults to latest..latest.
    // headBlockNumber() must be read once and reused for both bounds; otherwise
    // a block landing between the two reads makes the range straddle into
    // [N, N+1] and returns logs the caller did not ask for.
    final LogsQuery logsQuery = new LogsQuery.Builder().build();
    final String filterId = "latest-latest";
    filterRepository.save(
        new LogFilter(filterId, BlockParameter.LATEST, BlockParameter.LATEST, logsQuery));

    when(blockchainQueries.headBlockNumber()).thenReturn(100L, 101L);
    when(blockchainQueries.matchingLogs(anyLong(), anyLong(), any(LogsQuery.class), any()))
        .thenReturn(Collections.emptyList());

    filterManager.logs(filterId);

    verify(blockchainQueries, times(1)).headBlockNumber();
    verify(blockchainQueries).matchingLogs(eq(100L), eq(100L), eq(logsQuery), any());
  }

  private Hash appendBlockToBlockchain() {
    final long blockNumber = currentBlock.getHeader().getNumber() + 1;
    final Hash parentHash = currentBlock.getHash();
    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions().setBlockNumber(blockNumber).setParentHash(parentHash);
    currentBlock = blockGenerator.block(options);
    filterManager.recordBlockEvent(
        BlockAddedEvent.createForHeadAdvancement(
            currentBlock, Collections.emptyList(), Collections.emptyList()));
    return currentBlock.getHash();
  }

  private Hash receivePendingTransaction() {
    final Transaction transaction = blockGenerator.transaction();
    filterManager.recordPendingTransactionEvent(transaction);
    return transaction.getHash();
  }
}
