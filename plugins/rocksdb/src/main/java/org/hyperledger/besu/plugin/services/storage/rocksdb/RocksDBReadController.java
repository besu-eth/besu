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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.StorageReadPriority;
import org.hyperledger.besu.plugin.services.storage.StorageReadPriorityContext;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.rocksdb.RocksDBException;

/**
 * Global admission controller for RocksDB read calls.
 *
 * <p>The controller limits background RocksDB reads and gives queued medium-priority reads
 * precedence over low-priority reads. Execution reads use the default high priority and are never
 * gated; they only mark short-lived read pressure so best-effort reads can back off while execution
 * is actively hitting RocksDB.
 */
public class RocksDBReadController {

  private static final int NCPU = Runtime.getRuntime().availableProcessors();
  private static final int DEFAULT_MAX_CONCURRENT_BACKGROUND_READS = Math.max(4, NCPU * 4);
  private static final int DEFAULT_MAX_CONCURRENT_LOW_READS_UNDER_HIGH_PRESSURE = Math.max(1, NCPU);
  private static final long DEFAULT_LOW_READ_LIMIT_RAMP_UP_STEP_NANOS =
      TimeUnit.MILLISECONDS.toNanos(2);
  private static final long QUEUED_READ_RETRY_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
  private static final RocksDBReadController GLOBAL =
      new RocksDBReadController(
          DEFAULT_MAX_CONCURRENT_BACKGROUND_READS,
          DEFAULT_MAX_CONCURRENT_BACKGROUND_READS,
          DEFAULT_MAX_CONCURRENT_LOW_READS_UNDER_HIGH_PRESSURE,
          DEFAULT_LOW_READ_LIMIT_RAMP_UP_STEP_NANOS);

  private final int maxConcurrentReads;
  private final int maxConcurrentMediumReadsUnderHighPressure;
  private final int maxConcurrentLowReadsUnderHighPressure;
  private final long lowReadLimitRampUpStepNanos;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition readAvailable = lock.newCondition();
  private final int[] waitingReadsByPriority = new int[StorageReadPriority.values().length];
  private final int[] inFlightReadsByPriority = new int[StorageReadPriority.values().length];
  private final Map<ReadMetricsKey, ReadMetrics> readMetricsByContext = new ConcurrentHashMap<>();
  private final Map<ReadMetricsKey, AtomicLong> waitingReadsByContext = new ConcurrentHashMap<>();
  private final AtomicLong maxInFlightBackgroundReads = new AtomicLong();
  private final AtomicLong inFlightHighReads = new AtomicLong();
  private final AtomicLong maxInFlightHighReads = new AtomicLong();
  private final AtomicLong maxActiveHighPriorityScopes = new AtomicLong();
  private final AtomicLong lastHighReadNanos = new AtomicLong();
  private int inFlightReads;

  RocksDBReadController(final int maxConcurrentReads) {
    this(
        maxConcurrentReads,
        maxConcurrentReads,
        maxConcurrentReads,
        DEFAULT_LOW_READ_LIMIT_RAMP_UP_STEP_NANOS);
  }

  RocksDBReadController(
      final int maxConcurrentReads,
      final int maxConcurrentMediumReadsUnderHighPressure,
      final int maxConcurrentLowReadsUnderHighPressure,
      final long lowReadLimitRampUpStepNanos) {
    this.maxConcurrentReads = Math.max(1, maxConcurrentReads);
    this.maxConcurrentMediumReadsUnderHighPressure =
        Math.max(1, Math.min(this.maxConcurrentReads, maxConcurrentMediumReadsUnderHighPressure));
    this.maxConcurrentLowReadsUnderHighPressure =
        Math.max(1, Math.min(this.maxConcurrentReads, maxConcurrentLowReadsUnderHighPressure));
    this.lowReadLimitRampUpStepNanos = Math.max(1, lowReadLimitRampUpStepNanos);
  }

  public static RocksDBReadController global() {
    return GLOBAL;
  }

  public <T> T execute(final RocksDBReadOperation<T> readOperation) throws RocksDBException {
    final StorageReadPriority priority = StorageReadPriorityContext.currentPriority();
    if (priority == StorageReadPriority.HIGH) {
      return executeHighPriority(readOperation);
    }
    final ReadMetricsKey metricsKey = currentMetricsKey(priority);
    final long waitStartedAtNanos = System.nanoTime();
    final boolean queued = acquire(priority, metricsKey);
    final long waitNanos = queued ? System.nanoTime() - waitStartedAtNanos : 0L;
    final long readStartedAtNanos = System.nanoTime();
    try {
      return readOperation.execute();
    } finally {
      recordRead(metricsKey, queued, waitNanos, System.nanoTime() - readStartedAtNanos);
      release(priority);
    }
  }

  public <T> T executeUnchecked(final RocksDBUncheckedReadOperation<T> readOperation) {
    final StorageReadPriority priority = StorageReadPriorityContext.currentPriority();
    if (priority == StorageReadPriority.HIGH) {
      return executeHighPriority(readOperation);
    }
    final ReadMetricsKey metricsKey = currentMetricsKey(priority);
    final long waitStartedAtNanos = System.nanoTime();
    final boolean queued = acquire(priority, metricsKey);
    final long waitNanos = queued ? System.nanoTime() - waitStartedAtNanos : 0L;
    final long readStartedAtNanos = System.nanoTime();
    try {
      return readOperation.execute();
    } finally {
      recordRead(metricsKey, queued, waitNanos, System.nanoTime() - readStartedAtNanos);
      release(priority);
    }
  }

  private <T> T executeHighPriority(final RocksDBReadOperation<T> readOperation)
      throws RocksDBException {
    final ReadMetricsKey metricsKey = currentMetricsKey(StorageReadPriority.HIGH);
    final long readStartedAtNanos = highReadStarted();
    try {
      return readOperation.execute();
    } finally {
      recordRead(metricsKey, false, 0L, System.nanoTime() - readStartedAtNanos);
      highReadFinished();
    }
  }

  private <T> T executeHighPriority(final RocksDBUncheckedReadOperation<T> readOperation) {
    final ReadMetricsKey metricsKey = currentMetricsKey(StorageReadPriority.HIGH);
    final long readStartedAtNanos = highReadStarted();
    try {
      return readOperation.execute();
    } finally {
      recordRead(metricsKey, false, 0L, System.nanoTime() - readStartedAtNanos);
      highReadFinished();
    }
  }

  private long highReadStarted() {
    final long startedAtNanos = System.nanoTime();
    lastHighReadNanos.set(startedAtNanos);
    final long currentHighReads = inFlightHighReads.incrementAndGet();
    updateMax(maxInFlightHighReads, currentHighReads);
    updateActiveHighPriorityScopeMax();
    return startedAtNanos;
  }

  private void highReadFinished() {
    lastHighReadNanos.set(System.nanoTime());
    inFlightHighReads.decrementAndGet();
  }

  private boolean acquire(final StorageReadPriority priority, final ReadMetricsKey metricsKey) {
    lock.lock();
    try {
      if (canAcquire(priority)) {
        acquireRead(priority);
        return false;
      }

      waitingReadsByPriority[priority.ordinal()]++;
      final AtomicLong contextWaitingReads = waitingReadsFor(metricsKey);
      metricsFor(metricsKey).recordQueuedDepth(contextWaitingReads.incrementAndGet());
      try {
        while (!canAcquire(priority)) {
          readAvailable.awaitNanos(QUEUED_READ_RETRY_NANOS);
        }
        acquireRead(priority);
        return true;
      } finally {
        contextWaitingReads.decrementAndGet();
        waitingReadsByPriority[priority.ordinal()]--;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageException("Interrupted while waiting for RocksDB read capacity", e);
    } finally {
      lock.unlock();
    }
  }

  private void acquireRead(final StorageReadPriority priority) {
    inFlightReads++;
    inFlightReadsByPriority[priority.ordinal()]++;
    updateMax(maxInFlightBackgroundReads, inFlightReads);
  }

  private boolean canAcquire(final StorageReadPriority priority) {
    return inFlightReads < currentConcurrentReadLimit(priority)
        && !hasWaitingHigherPriorityRead(priority);
  }

  private int currentConcurrentReadLimit(final StorageReadPriority priority) {
    return switch (priority) {
      case HIGH -> maxConcurrentReads;
      case MEDIUM ->
          isHighReadPressureActive()
              ? maxConcurrentMediumReadsUnderHighPressure
              : maxConcurrentReads;
      case LOW -> currentLowConcurrentReadLimit();
    };
  }

  private int currentLowConcurrentReadLimit() {
    if (inFlightHighReads.get() > 0 || isHighPriorityScopeActive()) {
      return maxConcurrentLowReadsUnderHighPressure;
    }
    final long lastHighRead = lastHighReadNanos.get();
    if (lastHighRead == 0) {
      return maxConcurrentReads;
    }
    final long elapsedNanos = Math.max(0, System.nanoTime() - lastHighRead);
    final long rampUpSteps = elapsedNanos / lowReadLimitRampUpStepNanos;
    final long rampedLimit =
        maxConcurrentLowReadsUnderHighPressure
            + rampUpSteps * (long) maxConcurrentLowReadsUnderHighPressure;
    return (int) Math.min(maxConcurrentReads, rampedLimit);
  }

  private boolean isHighReadPressureActive() {
    return inFlightHighReads.get() > 0
        || isHighPriorityScopeActive()
        || currentLowConcurrentReadLimit() < maxConcurrentReads;
  }

  private boolean isHighPriorityScopeActive() {
    return updateActiveHighPriorityScopeMax() > 0;
  }

  private int updateActiveHighPriorityScopeMax() {
    final int activeHighPriorityScopes = StorageReadPriorityContext.activeHighPriorityScopes();
    updateMax(maxActiveHighPriorityScopes, activeHighPriorityScopes);
    return activeHighPriorityScopes;
  }

  private boolean hasWaitingHigherPriorityRead(final StorageReadPriority priority) {
    for (final StorageReadPriority waitingPriority : StorageReadPriority.values()) {
      if (!waitingPriority.isHigherThan(priority)) {
        return false;
      }
      if (waitingReadsByPriority[waitingPriority.ordinal()] > 0) {
        return true;
      }
    }
    return false;
  }

  private void release(final StorageReadPriority priority) {
    lock.lock();
    try {
      inFlightReads--;
      inFlightReadsByPriority[priority.ordinal()]--;
      readAvailable.signalAll();
    } finally {
      lock.unlock();
    }
  }

  int waitingReads(final StorageReadPriority priority) {
    lock.lock();
    try {
      return waitingReadsByPriority[priority.ordinal()];
    } finally {
      lock.unlock();
    }
  }

  int inFlightReads(final StorageReadPriority priority) {
    lock.lock();
    try {
      return inFlightReadsByPriority[priority.ordinal()];
    } finally {
      lock.unlock();
    }
  }

  public Optional<String> metricsSummaryAndReset() {
    final StringJoiner prioritySummaries = new StringJoiner(", ");
    readMetricsByContext.entrySet().stream()
        .sorted(
            Comparator.comparing(
                    (Map.Entry<ReadMetricsKey, ReadMetrics> entry) -> entry.getKey().priority())
                .thenComparing(entry -> entry.getKey().source()))
        .forEach(
            entry -> {
              final ReadMetricsSnapshot snapshot = entry.getValue().snapshotAndReset();
              if (snapshot.readCount() > 0) {
                prioritySummaries.add(entry.getKey().toLogString() + snapshot.toLogString());
              }
            });
    final long maxInFlightBackground = maxInFlightBackgroundReads.getAndSet(0);
    final long maxInFlightHigh = maxInFlightHighReads.getAndSet(0);
    final int activeHighPriorityScopes = updateActiveHighPriorityScopeMax();
    final long maxActiveHighPriorityScopesInInterval =
        Math.max(
            maxActiveHighPriorityScopes.getAndSet(activeHighPriorityScopes),
            activeHighPriorityScopes);
    if (prioritySummaries.length() == 0 && maxInFlightBackground == 0 && maxInFlightHigh == 0) {
      return Optional.empty();
    }
    final StringBuilder summary =
        new StringBuilder()
            .append("maxConcurrentBackgroundReads=")
            .append(maxConcurrentReads)
            .append(", lowLimitNow=")
            .append(currentConcurrentReadLimit(StorageReadPriority.LOW))
            .append(", mediumLimitNow=")
            .append(currentConcurrentReadLimit(StorageReadPriority.MEDIUM))
            .append(", highPressure=")
            .append(isHighReadPressureActive())
            .append(", lowRampStepMs=")
            .append(TimeUnit.NANOSECONDS.toMillis(lowReadLimitRampUpStepNanos))
            .append(", maxInFlightHighReads=")
            .append(maxInFlightHigh)
            .append(", inFlightHighNow=")
            .append(inFlightHighReads.get())
            .append(", maxActiveHighPriorityScopes=")
            .append(maxActiveHighPriorityScopesInInterval)
            .append(", activeHighPriorityScopesNow=")
            .append(activeHighPriorityScopes)
            .append(", maxInFlightBackgroundReads=")
            .append(maxInFlightBackground)
            .append(", inFlightBackgroundNow=")
            .append(currentInFlightReads())
            .append(", queuedNow=")
            .append(currentQueuedReadsSummary())
            .append(", queuedNowBySource=")
            .append(currentQueuedReadsByContextSummary())
            .append(", HIGH{gated=false}");
    if (prioritySummaries.length() > 0) {
      summary.append(", ").append(prioritySummaries);
    }
    return Optional.of(summary.toString());
  }

  private int currentInFlightReads() {
    lock.lock();
    try {
      return inFlightReads;
    } finally {
      lock.unlock();
    }
  }

  private String currentQueuedReadsSummary() {
    lock.lock();
    try {
      final StringJoiner queuedReads = new StringJoiner(",", "{", "}");
      for (final StorageReadPriority priority : StorageReadPriority.values()) {
        final int waitingReads = waitingReadsByPriority[priority.ordinal()];
        if (waitingReads > 0) {
          queuedReads.add(priority + "=" + waitingReads);
        }
      }
      return queuedReads.toString();
    } finally {
      lock.unlock();
    }
  }

  private String currentQueuedReadsByContextSummary() {
    final StringJoiner queuedReads = new StringJoiner(",", "{", "}");
    waitingReadsByContext.entrySet().stream()
        .sorted(
            Comparator.comparing(
                    (Map.Entry<ReadMetricsKey, AtomicLong> entry) -> entry.getKey().priority())
                .thenComparing(entry -> entry.getKey().source()))
        .forEach(
            entry -> {
              final long waitingReads = entry.getValue().get();
              if (waitingReads > 0) {
                queuedReads.add(entry.getKey().toLogString() + "=" + waitingReads);
              }
            });
    return queuedReads.toString();
  }

  private void recordRead(
      final ReadMetricsKey metricsKey,
      final boolean queued,
      final long waitNanos,
      final long readNanos) {
    metricsFor(metricsKey).recordRead(queued, waitNanos, readNanos);
  }

  private ReadMetrics metricsFor(final ReadMetricsKey metricsKey) {
    return readMetricsByContext.computeIfAbsent(metricsKey, __ -> new ReadMetrics());
  }

  private AtomicLong waitingReadsFor(final ReadMetricsKey metricsKey) {
    return waitingReadsByContext.computeIfAbsent(metricsKey, __ -> new AtomicLong());
  }

  private ReadMetricsKey currentMetricsKey(final StorageReadPriority priority) {
    return new ReadMetricsKey(priority, StorageReadPriorityContext.currentSource());
  }

  private static void updateMax(final AtomicLong max, final long value) {
    long currentMax;
    while (value > (currentMax = max.get()) && !max.compareAndSet(currentMax, value)) {
      // retry until the maximum is updated or a larger value wins the race
    }
  }

  private record ReadMetricsKey(StorageReadPriority priority, String source) {
    private String toLogString() {
      return priority + "/" + source;
    }
  }

  private static class ReadMetrics {
    private final LongAdder readCount = new LongAdder();
    private final LongAdder queuedReadCount = new LongAdder();
    private final LongAdder waitNanos = new LongAdder();
    private final LongAdder readNanos = new LongAdder();
    private final AtomicLong maxWaitNanos = new AtomicLong();
    private final AtomicLong maxReadNanos = new AtomicLong();
    private final AtomicLong maxQueuedDepth = new AtomicLong();

    private void recordRead(
        final boolean queued, final long readWaitNanos, final long readDurationNanos) {
      readCount.increment();
      if (queued) {
        queuedReadCount.increment();
        waitNanos.add(readWaitNanos);
        updateMax(maxWaitNanos, readWaitNanos);
      }
      readNanos.add(readDurationNanos);
      updateMax(maxReadNanos, readDurationNanos);
    }

    private void recordQueuedDepth(final long queuedDepth) {
      updateMax(maxQueuedDepth, queuedDepth);
    }

    private ReadMetricsSnapshot snapshotAndReset() {
      return new ReadMetricsSnapshot(
          readCount.sumThenReset(),
          queuedReadCount.sumThenReset(),
          waitNanos.sumThenReset(),
          maxWaitNanos.getAndSet(0),
          readNanos.sumThenReset(),
          maxReadNanos.getAndSet(0),
          maxQueuedDepth.getAndSet(0));
    }
  }

  private record ReadMetricsSnapshot(
      long readCount,
      long queuedReadCount,
      long waitNanos,
      long maxWaitNanos,
      long readNanos,
      long maxReadNanos,
      long maxQueuedDepth) {

    private String toLogString() {
      return "{reads="
          + readCount
          + ", queued="
          + queuedReadCount
          + ", waitAvgUs="
          + averageMicros(waitNanos, queuedReadCount)
          + ", waitMaxMs="
          + nanosToMillis(maxWaitNanos)
          + ", readAvgUs="
          + averageMicros(readNanos, readCount)
          + ", readMaxMs="
          + nanosToMillis(maxReadNanos)
          + ", maxQueued="
          + maxQueuedDepth
          + "}";
    }

    private static long averageMicros(final long nanos, final long count) {
      return count == 0 ? 0 : nanos / count / 1_000L;
    }

    private static long nanosToMillis(final long nanos) {
      return nanos / 1_000_000L;
    }
  }

  @FunctionalInterface
  public interface RocksDBReadOperation<T> {
    T execute() throws RocksDBException;
  }

  @FunctionalInterface
  public interface RocksDBUncheckedReadOperation<T> {
    T execute();
  }
}
