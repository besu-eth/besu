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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Typed thread pools for block processing: one for CPU work (tx execution), one for IO (RocksDB
 * prefetch), one for BAL state-root.
 * Sizes can be set with system properties {@code besu.block.cpuThreads}, {@code besu.block.ioThreads},
 * {@code besu.block.stateRootThreads}. All threads are daemon.
 */
public final class BlockProcessingExecutors {

  private static final int NCPU = Runtime.getRuntime().availableProcessors();

  private static final int CPU_THREADS = intProperty("besu.block.cpuThreads", NCPU);
  private static final int IO_THREADS = intProperty("besu.block.ioThreads", NCPU * 2);
  private static final int STATE_ROOT_THREADS = intProperty("besu.block.stateRootThreads", 2);

  // CPU work: parallel tx execution (EVM, keccak, RLP). Bounded to cores.
  private static final ExecutorService CPU_EXECUTOR =
      Executors.newFixedThreadPool(
          CPU_THREADS, namedDaemonThreadFactory("besu-block-cpu", Thread.NORM_PRIORITY + 1));

  // IO work: best-effort RocksDB prefetch/reads. Sized to device, not cores.
  private static final ExecutorService IO_EXECUTOR =
      Executors.newFixedThreadPool(
          IO_THREADS, namedDaemonThreadFactory("besu-block-io", Thread.NORM_PRIORITY));

  // BAL state-root: small dedicated pool, not the common pool.
  private static final ExecutorService STATE_ROOT_EXECUTOR =
      Executors.newFixedThreadPool(
          STATE_ROOT_THREADS,
          namedDaemonThreadFactory("besu-block-stateroot", Thread.NORM_PRIORITY - 1));

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
      return defaultValue;
    }
  }

  // Error Prone don't like setPriority
  @SuppressWarnings("ThreadPriorityCheck")
  private static ThreadFactory namedDaemonThreadFactory(final String prefix, final int priority) {
    final int clampedPriority =
        Math.max(Thread.MIN_PRIORITY, Math.min(Thread.MAX_PRIORITY, priority));
    final AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      final Thread thread = new Thread(runnable, prefix + "-" + counter.getAndIncrement());
      thread.setDaemon(true);
      thread.setPriority(clampedPriority);
      return thread;
    };
  }
}
