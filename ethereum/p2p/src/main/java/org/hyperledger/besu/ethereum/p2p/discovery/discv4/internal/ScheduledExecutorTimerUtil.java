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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JDK-based {@link TimerUtil} backed by a {@link ScheduledExecutorService}. */
public final class ScheduledExecutorTimerUtil implements TimerUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledExecutorTimerUtil.class);

  private final ScheduledExecutorService scheduler;
  private final ConcurrentHashMap<Long, ScheduledFuture<?>> timers = new ConcurrentHashMap<>();
  private final AtomicLong nextId = new AtomicLong(0);

  public ScheduledExecutorTimerUtil(final ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public long setTimer(final long delayInMs, final TimerHandler handler) {
    final long id = nextId.incrementAndGet();
    final ScheduledFuture<?> future =
        scheduler.schedule(
            () -> {
              try {
                handler.handle();
              } finally {
                timers.remove(id);
              }
            },
            delayInMs,
            TimeUnit.MILLISECONDS);
    timers.put(id, future);
    // Race close-out: for ≈0-delay schedules, the task may have already run before put()
    // (its finally found nothing to remove). Clean up any stale completed entry now.
    if (future.isDone()) {
      timers.remove(id);
    }
    return id;
  }

  /**
   * Schedules a periodic timer. Periodic timers do not auto-remove on fire — callers must invoke
   * {@link #cancelTimer(long)} when the timer is no longer needed; otherwise the entry persists
   * until the scheduler is shut down.
   */
  @Override
  public long setPeriodic(final long delayInMs, final String name, final TimerHandler handler) {
    final long id = nextId.incrementAndGet();
    final ScheduledFuture<?> future =
        scheduler.scheduleAtFixedRate(
            () -> {
              try {
                handler.handle();
              } catch (final RuntimeException e) {
                LOG.error("Uncaught exception in periodic timer '{}' (id={})", name, id, e);
              }
            },
            delayInMs,
            delayInMs,
            TimeUnit.MILLISECONDS);
    timers.put(id, future);
    return id;
  }

  @Override
  public void cancelTimer(final long timerId) {
    final ScheduledFuture<?> future = timers.remove(timerId);
    if (future != null) {
      future.cancel(false);
    }
  }
}
