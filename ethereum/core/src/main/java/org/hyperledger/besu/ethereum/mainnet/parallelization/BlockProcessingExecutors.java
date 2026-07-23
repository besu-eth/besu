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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Typed thread pools for block processing: one for CPU work (tx execution), one for IO (RocksDB
 * prefetch), one for parallel storage-trie updates, one for BAL state-root. Sizes can be set with
 * system properties {@code besu.block.cpuThreads}, {@code besu.block.ioThreads}, {@code
 * besu.block.storageTrieThreads}, {@code besu.block.accountTrieThreads}, {@code
 * besu.block.stateRootThreads}. All threads are daemon.
 */
public final class BlockProcessingExecutors {

  private static final Logger LOG = LoggerFactory.getLogger(BlockProcessingExecutors.class);

  private static final int NCPU = Runtime.getRuntime().availableProcessors();

  private static final int CPU_THREADS = intProperty("besu.block.cpuThreads", NCPU);
  private static final int IO_THREADS = intProperty("besu.block.ioThreads", NCPU * 2);
  private static final int STORAGE_TRIE_THREADS =
      intProperty("besu.block.storageTrieThreads", NCPU * 2);
  private static final int ACCOUNT_TRIE_THREADS =
      intProperty("besu.block.accountTrieThreads", NCPU);
  private static final int STATE_ROOT_THREADS = intProperty("besu.block.stateRootThreads", 1);

  // CPU work: parallel tx execution (EVM, keccak, RLP). Bounded to cores.
  private static final ExecutorService CPU_EXECUTOR =
      Executors.newFixedThreadPool(CPU_THREADS, namedDaemonThreadFactory("besu-block-cpu"));

  // IO work: best-effort RocksDB prefetch/reads. Sized to device, not cores.
  private static final ExecutorService IO_EXECUTOR =
      Executors.newFixedThreadPool(IO_THREADS, namedDaemonThreadFactory("besu-block-io"));

  // Parallel per-account storage-trie updates during state-root commit. Work-stealing
  // suits many short independent tasks (same model as parallelStream / common pool).
  private static final ForkJoinPool STORAGE_TRIE_FORK_JOIN_POOL =
      newWorkStealingPool(STORAGE_TRIE_THREADS, "besu-block-storage-trie");

  // Parallel account-trie updates during state-root commit. Kept separate from the storage-trie
  // pool so account workers can block on storage futures without starving storage root hashing.
  private static final ForkJoinPool ACCOUNT_TRIE_FORK_JOIN_POOL =
      newWorkStealingPool(ACCOUNT_TRIE_THREADS, "besu-block-account-trie");

  private static final ExecutorService STORAGE_TRIE_EXECUTOR = STORAGE_TRIE_FORK_JOIN_POOL;

  // BAL state-root: small dedicated pool, not the common pool.
  private static final ExecutorService STATE_ROOT_EXECUTOR =
      Executors.newFixedThreadPool(
          STATE_ROOT_THREADS, namedDaemonThreadFactory("besu-block-stateroot"));

  private BlockProcessingExecutors() {}

  /**
   * CPU pool for tx execution.
   *
   * @return the CPU executor
   */
  public static ExecutorService cpuExecutor() {
    return CPU_EXECUTOR;
  }

  /**
   * IO pool for storage prefetch/reads.
   *
   * @return the IO executor
   */
  public static ExecutorService ioExecutor() {
    return IO_EXECUTOR;
  }

  /**
   * Work-stealing pool for parallel storage-trie updates during state-root commit.
   *
   * @return the storage-trie executor
   */
  public static ExecutorService storageTrieExecutor() {
    return STORAGE_TRIE_EXECUTOR;
  }

  /**
   * ForkJoinPool for parallel account-trie hashing during state-root commit.
   *
   * @return the account-trie fork-join pool
   */
  public static ForkJoinPool accountTrieForkJoinPool() {
    return ACCOUNT_TRIE_FORK_JOIN_POOL;
  }

  /**
   * ForkJoinPool for parallel storage-trie hashing during state-root commit.
   *
   * @return the storage-trie fork-join pool
   */
  public static ForkJoinPool storageTrieForkJoinPool() {
    return STORAGE_TRIE_FORK_JOIN_POOL;
  }

  /**
   * Pool for BAL state-root computation.
   *
   * @return the state-root executor
   */
  public static ExecutorService stateRootExecutor() {
    return STATE_ROOT_EXECUTOR;
  }

  private static int intProperty(final String key, final int defaultValue) {
    final String raw = System.getProperty(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      final int parsed = Integer.parseInt(raw.trim());
      return parsed > 0 ? parsed : defaultValue;
    } catch (final NumberFormatException e) {
      LOG.warn(
          "Invalid value '{}' for system property '{}', using default {}", raw, key, defaultValue);
      return defaultValue;
    }
  }

  private static ForkJoinPool newWorkStealingPool(final int parallelism, final String prefix) {
    final AtomicInteger counter = new AtomicInteger();
    return new ForkJoinPool(
        parallelism,
        pool -> {
          final ForkJoinWorkerThread worker =
              ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
          worker.setName(prefix + "-" + counter.getAndIncrement());
          worker.setDaemon(true);
          return worker;
        },
        null,
        false);
  }

  private static ThreadFactory namedDaemonThreadFactory(final String prefix) {
    final AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      final Thread thread = new Thread(runnable, prefix + "-" + counter.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    };
  }
}
