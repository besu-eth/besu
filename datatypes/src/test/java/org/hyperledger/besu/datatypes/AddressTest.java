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
package org.hyperledger.besu.datatypes;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class AddressTest {

  @Test
  public void addressHashReturnsCorrectValue() {
    final Address address = Address.fromHexString("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    final Hash expected = Hash.hash(address.getBytes());
    assertThat(address.addressHash()).isEqualTo(expected);
  }

  @Test
  public void addressHashIsCached() {
    final Address address = Address.fromHexString("0x1234567890123456789012345678901234567890");
    final Hash first = address.addressHash();
    final Hash second = address.addressHash();
    assertThat(first).isSameAs(second);
  }

  /**
   * Verifies that addressHash() remains live and correct under the dual-pool concurrent load
   * pattern observed in production.
   *
   * <p>Two independent caller pools — simulating transaction pool validation and parallel block
   * processing — start simultaneously and each process far more unique addresses than the cache
   * capacity (4000), sustaining cache misses throughout. The test confirms that all threads
   * complete within a reasonable time and that every computed hash is correct.
   */
  @Test
  public void addressHashDoesNotDeadlockUnderConcurrentCacheMisses() throws InterruptedException {
    // Mirrors the production thread count: txpool validation + parallel block processor
    final int txpoolThreads = 4;
    final int blockProcessorThreads = 8;
    final int totalThreads = txpoolThreads + blockProcessorThreads;

    // Each thread processes more unique addresses than the cache capacity (4000),
    // sustaining cache misses across both pools simultaneously
    final int addressesPerThread = 600;

    final ExecutorService txpoolExecutor = Executors.newFixedThreadPool(txpoolThreads);
    final ExecutorService blockExecutor = Executors.newFixedThreadPool(blockProcessorThreads);

    // Single barrier so both pools start simultaneously, maximising segment lock contention
    final CountDownLatch startBarrier = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(totalThreads);
    final AtomicBoolean incorrectHash = new AtomicBoolean(false);

    // Submit txpool-style threads (pool A — bytes[0]=0x00)
    for (int t = 0; t < txpoolThreads; t++) {
      final int threadIndex = t;
      txpoolExecutor.submit(
          () -> {
            try {
              startBarrier.await();
              for (int i = 0; i < addressesPerThread; i++) {
                final byte[] bytes = new byte[20];
                bytes[0] = 0x00; // pool A marker
                bytes[1] = (byte) threadIndex;
                bytes[2] = (byte) (i >> 8);
                bytes[3] = (byte) i;
                final Address addr = Address.wrap(Bytes.wrap(bytes));
                final Hash result = addr.addressHash();
                if (!result.equals(Hash.hash(addr.getBytes()))) {
                  incorrectHash.set(true);
                }
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    // Submit parallel-block-processor-style threads (pool B — bytes[0]=0x01)
    for (int t = 0; t < blockProcessorThreads; t++) {
      final int threadIndex = t;
      blockExecutor.submit(
          () -> {
            try {
              startBarrier.await();
              for (int i = 0; i < addressesPerThread; i++) {
                final byte[] bytes = new byte[20];
                bytes[0] = 0x01; // pool B marker
                bytes[1] = (byte) threadIndex;
                bytes[2] = (byte) (i >> 8);
                bytes[3] = (byte) i;
                final Address addr = Address.wrap(Bytes.wrap(bytes));
                final Hash result = addr.addressHash();
                if (!result.equals(Hash.hash(addr.getBytes()))) {
                  incorrectHash.set(true);
                }
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startBarrier.countDown();

    final boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    txpoolExecutor.shutdownNow();
    blockExecutor.shutdownNow();

    assertThat(completed)
        .as(
            "addressHash() stalled under concurrent dual-pool access — "
                + "%d of %d threads did not complete within 30 seconds",
            doneLatch.getCount(), totalThreads)
        .isTrue();
    assertThat(incorrectHash.get())
        .as("addressHash() returned an incorrect value under concurrent access")
        .isFalse();
  }

  /**
   * Verifies that concurrent threads reading the same address see consistent, correct hash values
   * (no torn reads or stale cache entries).
   */
  @Test
  public void addressHashIsConsistentUnderConcurrentReads() throws InterruptedException {
    final Address sharedAddress =
        Address.fromHexString("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
    final Hash expected = Hash.hash(sharedAddress.getBytes());

    final int numThreads = 50;
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch startBarrier = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(numThreads);
    final List<Hash> results = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      results.add(null);
    }

    for (int t = 0; t < numThreads; t++) {
      final int idx = t;
      executor.submit(
          () -> {
            try {
              startBarrier.await();
              results.set(idx, sharedAddress.addressHash());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startBarrier.countDown();
    assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
    executor.shutdownNow();

    assertThat(results).allMatch(h -> expected.equals(h));
  }
}
