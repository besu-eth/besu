/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BlockHeaderSourceTest {

  private Blockchain blockchain;

  @BeforeEach
  public void setUp() {
    blockchain = mock(Blockchain.class);
  }

  @Test
  public void shouldIterateForwardFromStartToPivot() {
    when(blockchain.getBlockHeaders(anyLong(), anyInt()))
        .thenAnswer(
            invocation -> {
              long start = invocation.getArgument(0);
              int count = invocation.getArgument(1);
              return createMockHeaders(start, count);
            });

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 1000, 100);

    List<List<BlockHeader>> batches = new ArrayList<>();
    while (source.hasNext()) {
      List<BlockHeader> batch = source.next();
      if (batch != null && !batch.isEmpty()) {
        batches.add(batch);
      }
    }

    // Should have 10 batches: 100-199, 200-299, ..., 900-999, 1000
    assertThat(batches).hasSize(10);
    assertThat(batches.get(0).get(0).getNumber()).isEqualTo(100);
    assertThat(batches.get(9).get(0).getNumber()).isEqualTo(1000);
  }

  @Test
  public void shouldReturnCorrectBatchSizes() {
    when(blockchain.getBlockHeaders(100, 50)).thenReturn(createMockHeaders(100, 50));
    when(blockchain.getBlockHeaders(150, 50)).thenReturn(createMockHeaders(150, 50));
    when(blockchain.getBlockHeaders(200, 50)).thenReturn(createMockHeaders(200, 50));
    when(blockchain.getBlockHeaders(250, 1)).thenReturn(createMockHeaders(250, 1));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 250, 50);

    assertThat(source.next()).hasSize(50); // 100-149
    assertThat(source.next()).hasSize(50); // 150-199
    assertThat(source.next()).hasSize(50); // 200-249
    assertThat(source.next()).hasSize(1); // 250
    assertThatThrownBy(() -> source.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleStartBlockEqualToPivot() {
    when(blockchain.getBlockHeaders(500, 1)).thenReturn(createMockHeaders(500, 1));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 499, 500, 100);

    assertThat(source.hasNext()).isTrue();
    List<BlockHeader> batch = source.next();
    assertThat(batch).hasSize(1);
    assertThat(batch.get(0).getNumber()).isEqualTo(500);
    assertThat(source.hasNext()).isFalse();
    assertThatThrownBy(() -> source.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleInvalidRangeStartAfterPivot() {
    BlockHeaderSource source = new BlockHeaderSource(blockchain, 500, 400, 100);

    // currentBlock starts at 501, which is > pivotBlockNumber (400)
    assertThat(source.hasNext()).isFalse();
    assertThatThrownBy(() -> source.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleBatchSizeLargerThanRange() {
    when(blockchain.getBlockHeaders(100, 50)).thenReturn(createMockHeaders(100, 50));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 149, 1000);

    assertThat(source.hasNext()).isTrue();
    List<BlockHeader> batch = source.next();
    assertThat(batch).hasSize(50); // Only 50 headers available
    assertThat(source.hasNext()).isFalse();
    assertThatThrownBy(() -> source.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void hasNextShouldReturnTrueUntilExhausted() {
    when(blockchain.getBlockHeaders(anyLong(), anyInt()))
        .thenAnswer(
            invocation -> {
              long start = invocation.getArgument(0);
              int count = invocation.getArgument(1);
              return createMockHeaders(start, count);
            });

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 299, 100);

    assertThat(source.hasNext()).isTrue();
    source.next(); // 100-199
    assertThat(source.hasNext()).isTrue();
    source.next(); // 200-299
    assertThat(source.hasNext()).isFalse();
  }

  @Test
  public void hasNextShouldNotConsumeElements() {
    when(blockchain.getBlockHeaders(100, 50)).thenReturn(createMockHeaders(100, 50));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 149, 100);

    assertThat(source.hasNext()).isTrue();
    assertThat(source.hasNext()).isTrue();
    assertThat(source.hasNext()).isTrue();

    // Calling hasNext multiple times shouldn't affect next()
    List<BlockHeader> batch = source.next();
    assertThat(batch).hasSize(50);
    assertThat(batch.get(0).getNumber()).isEqualTo(100);
  }

  @Test
  public void shouldHandleSmallBatchSize() {
    when(blockchain.getBlockHeaders(100, 1)).thenReturn(createMockHeaders(100, 1));
    when(blockchain.getBlockHeaders(101, 1)).thenReturn(createMockHeaders(101, 1));
    when(blockchain.getBlockHeaders(102, 1)).thenReturn(createMockHeaders(102, 1));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 102, 1);

    assertThat(source.next()).hasSize(1);
    assertThat(source.next()).hasSize(1);
    assertThat(source.next()).hasSize(1);
    assertThatThrownBy(() -> source.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleVeryLargeBlockNumbers() {
    final long startBlock = Long.MAX_VALUE - 1000;
    final long pivotBlock = Long.MAX_VALUE - 500;

    when(blockchain.getBlockHeaders(startBlock + 1, 200))
        .thenReturn(createMockHeaders(startBlock + 1, 200));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, startBlock, pivotBlock, 200);

    assertThat(source.hasNext()).isTrue();
    List<BlockHeader> batch = source.next();
    assertThat(batch).hasSize(200);
    assertThat(batch.get(0).getNumber()).isEqualTo(startBlock + 1);
  }

  @Test
  public void shouldHandleEmptyBlockchainResponse() {
    when(blockchain.getBlockHeaders(100, 10)).thenReturn(Collections.emptyList());

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 109, 100);

    List<BlockHeader> batch = source.next();
    assertThat(batch).isEmpty();
  }

  @Test
  public void shouldHandleNullBlockchainResponse() {
    when(blockchain.getBlockHeaders(100, 10)).thenThrow(new RuntimeException("Blockchain error"));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 109, 100);

    assertThatThrownBy(() -> source.next()).isInstanceOf(RuntimeException.class);
  }

  @Test
  public void shouldCallBlockchainWithCorrectParameters() {
    when(blockchain.getBlockHeaders(100, 50)).thenReturn(createMockHeaders(100, 50));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 149, 100);

    source.next();

    verify(blockchain).getBlockHeaders(eq(100L), eq(50));
  }

  @Test
  public void shouldHandleExactMultipleOfBatchSize() {
    when(blockchain.getBlockHeaders(100, 50)).thenReturn(createMockHeaders(100, 50));
    when(blockchain.getBlockHeaders(150, 50)).thenReturn(createMockHeaders(150, 50));
    when(blockchain.getBlockHeaders(200, 50)).thenReturn(createMockHeaders(200, 50));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 199, 50);

    assertThat(source.next()).hasSize(50);
    assertThat(source.next()).hasSize(50);
    assertThatThrownBy(() -> source.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleConcurrentNextCalls() throws InterruptedException {
    when(blockchain.getBlockHeaders(anyLong(), anyInt()))
        .thenAnswer(
            invocation -> {
              long start = invocation.getArgument(0);
              int count = invocation.getArgument(1);
              return createMockHeaders(start, count);
            });

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 199, 10);

    final int numThreads = 5;
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch completionLatch = new CountDownLatch(numThreads);
    final CopyOnWriteArrayList<List<BlockHeader>> collectedBatches = new CopyOnWriteArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            try {
              startLatch.await();
              // Each thread tries to get batches
              for (int j = 0; j < 5; j++) {
                List<BlockHeader> batch = source.next();
                if (batch != null && !batch.isEmpty()) {
                  collectedBatches.add(batch);
                }
              }
            } catch (final Exception e) {
              // Ignore
            } finally {
              completionLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    completionLatch.await(5, TimeUnit.SECONDS);
    executorService.shutdown();

    // Should have collected exactly 10 batches (100-109, 110-119, ..., 190-199)
    assertThat(collectedBatches).hasSize(10);

    // Verify no duplicate batches by checking first header of each batch is unique
    List<Long> firstBlockNumbers =
        collectedBatches.stream().map(batch -> batch.get(0).getNumber()).sorted().toList();
    assertThat(firstBlockNumbers)
        .containsExactly(100L, 110L, 120L, 130L, 140L, 150L, 160L, 170L, 180L, 190L);
  }

  @Test
  public void shouldHandleConcurrentHasNextAndNextCalls() throws InterruptedException {
    when(blockchain.getBlockHeaders(anyLong(), anyInt()))
        .thenAnswer(
            invocation -> {
              long start = invocation.getArgument(0);
              int count = invocation.getArgument(1);
              return createMockHeaders(start, count);
            });

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 149, 10);

    final int numThreads = 3;
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch completionLatch = new CountDownLatch(numThreads);
    final CopyOnWriteArrayList<List<BlockHeader>> collectedBatches = new CopyOnWriteArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            try {
              startLatch.await();
              while (source.hasNext()) {
                List<BlockHeader> batch = source.next();
                if (batch != null && !batch.isEmpty()) {
                  collectedBatches.add(batch);
                }
              }
            } catch (final Exception e) {
              // Ignore
            } finally {
              completionLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    completionLatch.await(5, TimeUnit.SECONDS);
    executorService.shutdown();

    // Should have collected 5 batches: 100-109, 110-119, 120-129, 130-139, 140-149
    assertThat(collectedBatches).hasSize(5);
  }

  @Test
  public void shouldHandleOddBatchSize() {
    when(blockchain.getBlockHeaders(100, 37)).thenReturn(createMockHeaders(100, 37));
    when(blockchain.getBlockHeaders(137, 37)).thenReturn(createMockHeaders(137, 37));
    when(blockchain.getBlockHeaders(174, 26)).thenReturn(createMockHeaders(174, 26));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 99, 199, 37);

    assertThat(source.next()).hasSize(37); // 100-136
    assertThat(source.next()).hasSize(37); // 137-173
    assertThat(source.next()).hasSize(26); // 174-199
    assertThatThrownBy(() -> source.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleZeroStartBlock() {
    when(blockchain.getBlockHeaders(1, 10)).thenReturn(createMockHeaders(1, 10));

    BlockHeaderSource source = new BlockHeaderSource(blockchain, 0, 10, 100);

    List<BlockHeader> batch = source.next();
    assertThat(batch).hasSize(10);
    assertThat(batch.get(0).getNumber()).isEqualTo(1);
  }

  private List<BlockHeader> createMockHeaders(final long startBlock, final int count) {
    List<BlockHeader> headers = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      headers.add(new BlockHeaderTestFixture().number(startBlock + i).buildHeader());
    }
    return headers;
  }
}
