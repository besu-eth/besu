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
package org.hyperledger.besu.consensus.qbft.adaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class ValidatedBlockCacheTest {
  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final Hash blockHash = generator.hash();
  private final ValidatedBlockCache cache = new ValidatedBlockCache();

  @Test
  void keepsOutputsUntilTheirHeightIsImported() {
    final Hash lower = generator.hash();
    final Hash higher = generator.hash();
    cache.put(lower, validatedBlock(9));
    cache.put(blockHash, validatedBlock(10));
    cache.put(higher, validatedBlock(11));

    // read by the round and the network imports
    assertThat(cache.get(blockHash)).isPresent();
    assertThat(cache.get(blockHash)).isPresent();

    cache.pruneUpTo(10);

    assertThat(cache.get(lower)).isEmpty();
    assertThat(cache.get(blockHash)).isEmpty();
    assertThat(cache.get(higher)).isPresent();
  }

  @Test
  void onlyOneThreadExecutesABlockAtATime() {
    assertThat(cache.tryStartExecution(blockHash)).isTrue();
    assertThat(cache.tryStartExecution(blockHash)).isFalse();
    assertThat(cache.tryStartExecution(generator.hash())).isTrue();

    cache.endExecution(blockHash);

    assertThat(cache.tryStartExecution(blockHash)).isTrue();
  }

  @Test
  void doesNotWaitWhenTheBlockIsNotBeingExecuted() {
    final Thread waiting = new Thread(() -> cache.awaitExecution(blockHash));

    waiting.start();

    await().atMost(Duration.ofSeconds(10)).until(() -> !waiting.isAlive());
  }

  @Test
  void waitsUntilTheExecutionEnds() {
    cache.tryStartExecution(blockHash);
    final Thread waiting = new Thread(() -> cache.awaitExecution(blockHash));

    waiting.start();
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> waiting.getState() == Thread.State.TIMED_WAITING);
    cache.endExecution(blockHash);

    await().atMost(Duration.ofSeconds(10)).until(() -> !waiting.isAlive());
  }

  @Test
  void stopsWaitingWhenInterrupted() {
    cache.tryStartExecution(blockHash);
    final Thread waiting = new Thread(() -> cache.awaitExecution(blockHash));

    waiting.start();
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> waiting.getState() == Thread.State.TIMED_WAITING);
    waiting.interrupt();

    await().atMost(Duration.ofSeconds(10)).until(() -> !waiting.isAlive());
  }

  @Test
  void comparesBodiesByTransactionHashes() {
    final BlockBody body = new BlockBody(List.of(generator.transaction()), List.of());
    // decoded again from a peer: new objects, same content
    final BlockBody copy =
        new BlockBody(body.getTransactions(), body.getOmmers(), body.getWithdrawals());
    final BlockBody other = new BlockBody(List.of(generator.transaction()), List.of());

    assertThat(ValidatedBlockCache.isSameBody(body, copy)).isTrue();
    assertThat(ValidatedBlockCache.isSameBody(body, other)).isFalse();
  }

  private ValidatedBlockCache.ValidatedBlock validatedBlock(final long blockNumber) {
    return new ValidatedBlockCache.ValidatedBlock(
        blockNumber, BlockBody.empty(), List.of(), Optional.empty());
  }
}
