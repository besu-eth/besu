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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class BackwardHeaderSourceTest {

  @Test
  public void shouldIterateBackwardFromStartToStop() {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(100, 0, 1000);

    final List<Long> blocks = new ArrayList<>();
    while (source.hasNext()) {
      final Long block = source.next();
      if (block != null) {
        blocks.add(block);
      }
    }

    assertThat(blocks)
        .containsExactly(1000L, 900L, 800L, 700L, 600L, 500L, 400L, 300L, 200L, 100L, 0L);
  }

  @Test
  public void shouldThrowWhenExhausted() {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(100, 50, 100);

    assertThat(source.next()).isEqualTo(100L);
    // After first call, currentBlock = 0, which is < stopBlock (50), so throws
    assertThatThrownBy(source::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleStartBlockEqualToStopBlock() {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(100, 500, 500);

    assertThat(source.hasNext()).isTrue();
    assertThat(source.next()).isEqualTo(500L);
    assertThat(source.hasNext()).isFalse();
    assertThatThrownBy(source::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleInvalidRangeStartBlockLessThanStopBlock() {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(100, 1000, 500);

    // currentBlock starts at 500, which is < stopBlock (1000)
    assertThat(source.hasNext()).isFalse();
    assertThatThrownBy(source::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleBatchSizeLargerThanRange() {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(1000, 0, 100);

    assertThat(source.hasNext()).isTrue();
    assertThat(source.next()).isEqualTo(100L);
    assertThat(source.hasNext()).isFalse();
    assertThatThrownBy(source::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldHandleBatchSizeOfOne() {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(1, 5, 10);

    final List<Long> blocks = new ArrayList<>();
    while (source.hasNext()) {
      final Long block = source.next();
      if (block != null) {
        blocks.add(block);
      }
    }

    assertThat(blocks).containsExactly(10L, 9L, 8L, 7L, 6L, 5L);
  }

  @Test
  public void hasNextShouldReturnTrueUntilExhausted() {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(100, 0, 200);

    assertThat(source.hasNext()).isTrue();
    source.next(); // 200
    assertThat(source.hasNext()).isTrue();
    source.next(); // 100
    assertThat(source.hasNext()).isTrue();
    source.next(); // 0
    assertThat(source.hasNext()).isFalse();
  }

  @Test
  public void hasNextShouldNotConsumeElements() {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(50, 0, 100);

    assertThat(source.hasNext()).isTrue();
    assertThat(source.hasNext()).isTrue();
    assertThat(source.hasNext()).isTrue();

    // Calling hasNext multiple times shouldn't affect next()
    assertThat(source.next()).isEqualTo(100L);
  }

  @Test
  public void shouldHandleConcurrentNextCalls() throws InterruptedException {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(1, 0, 100);

    final int numThreads = 10;
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch completionLatch = new CountDownLatch(numThreads);
    final CopyOnWriteArrayList<Long> collectedBlocks = new CopyOnWriteArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            try {
              startLatch.await();
              // Each thread tries to get blocks
              for (int j = 0; j < 20; j++) {
                final Long block = source.next();
                if (block != null) {
                  collectedBlocks.add(block);
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

    // Should have collected exactly 101 blocks (0 through 100)
    assertThat(collectedBlocks).hasSize(101);
    // Each block should appear exactly once (thread-safe)
    assertThat(collectedBlocks)
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L,
                19L, 20L, 21L, 22L, 23L, 24L, 25L, 26L, 27L, 28L, 29L, 30L, 31L, 32L, 33L, 34L, 35L,
                36L, 37L, 38L, 39L, 40L, 41L, 42L, 43L, 44L, 45L, 46L, 47L, 48L, 49L, 50L, 51L, 52L,
                53L, 54L, 55L, 56L, 57L, 58L, 59L, 60L, 61L, 62L, 63L, 64L, 65L, 66L, 67L, 68L, 69L,
                70L, 71L, 72L, 73L, 74L, 75L, 76L, 77L, 78L, 79L, 80L, 81L, 82L, 83L, 84L, 85L, 86L,
                87L, 88L, 89L, 90L, 91L, 92L, 93L, 94L, 95L, 96L, 97L, 98L, 99L, 100L));
  }

  @Test
  public void shouldHandleConcurrentHasNextAndNextCalls() throws InterruptedException {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(10, 0, 50);

    final int numThreads = 5;
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch completionLatch = new CountDownLatch(numThreads);
    final CopyOnWriteArrayList<Long> collectedBlocks = new CopyOnWriteArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            try {
              startLatch.await();
              while (source.hasNext()) {
                final Long block = source.next();
                if (block != null) {
                  collectedBlocks.add(block);
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

    // Should have collected 6 blocks: 50, 40, 30, 20, 10, 0
    assertThat(collectedBlocks).hasSize(6);
    assertThat(collectedBlocks).containsExactlyInAnyOrder(50L, 40L, 30L, 20L, 10L, 0L);
  }

  @Test
  public void shouldHandlePartialBatchAtEnd() {
    // Start at 105, stop at 0, batch size 100
    // Should give: 105, 5 (105-100=5, which is >= 0)
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(100, 0, 105);

    assertThat(source.next()).isEqualTo(105L);
    assertThat(source.next()).isEqualTo(5L);
    assertThatThrownBy(source::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldIterateCorrectlyWithOddBatchSize() {
    final BackwardBlockNumberSource source = new BackwardBlockNumberSource(37, 0, 100);

    final List<Long> blocks = new ArrayList<>();
    while (source.hasNext()) {
      final Long block = source.next();
      if (block != null) {
        blocks.add(block);
      }
    }

    assertThat(blocks).containsExactly(100L, 63L, 26L);
  }
}
