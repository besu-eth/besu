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
package org.hyperledger.besu.ethereum.worldstate;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.function.LongSupplier;

/** A thread-safe, cumulative resource allowance for one world-state repair attempt. */
public final class WorldStateRepairBudget {
  private static final long TIME_LIMIT_NANOS = Duration.ofSeconds(10).toNanos();
  private static final long RECEIVED_BYTES_LIMIT = 8L * 1024 * 1024;
  private static final long STAGED_BYTES_LIMIT = 16L * 1024 * 1024;
  private static final int OBJECT_LIMIT = 32;
  private static final int TRANSITION_LIMIT = 64;

  private final LongSupplier nanoTime;
  private final long startedAt;
  private boolean cancelled;
  private boolean exhausted;
  private long receivedBytes;
  private long stagedBytes;
  private int repairObjects;
  private int transitions;

  /** Creates the standard ten-second repair allowance using the system monotonic clock. */
  public static WorldStateRepairBudget defaults() {
    return new WorldStateRepairBudget(System::nanoTime);
  }

  /**
   * Creates the standard allowance with an injectable monotonic clock.
   *
   * @param nanoTime monotonic elapsed-time source, in nanoseconds
   */
  public WorldStateRepairBudget(final LongSupplier nanoTime) {
    this.nanoTime = requireNonNull(nanoTime);
    this.startedAt = nanoTime.getAsLong();
  }

  /** Requests cooperative cancellation; the worker must still drain before releasing ownership. */
  public synchronized void cancel() {
    cancelled = true;
  }

  public synchronized boolean isCancelled() {
    return cancelled;
  }

  /**
   * Whether time expired or an attempted charge exceeded a resource limit. Reaching a resource
   * limit exactly is permitted; a subsequent charge beyond it latches exhaustion.
   *
   * @return whether this allowance has been exhausted
   */
  public synchronized boolean isExhausted() {
    exhausted |= elapsedNanos() >= TIME_LIMIT_NANOS;
    return exhausted;
  }

  /** Returns the remaining time, or zero after cancellation or exhaustion. */
  public synchronized Duration remainingTime() {
    if (cancelled || isExhausted()) {
      return Duration.ZERO;
    }
    return Duration.ofNanos(Math.max(0L, TIME_LIMIT_NANOS - elapsedNanos()));
  }

  /** Charges received bytes before accepting them; failed charges cannot wrap or reset counters. */
  public synchronized boolean consumeReceivedBytes(final long bytes) {
    requireNonNegative(bytes);
    if (!canConsume()) {
      return false;
    }
    if (bytes > RECEIVED_BYTES_LIMIT - receivedBytes) {
      exhausted = true;
      return false;
    }
    receivedBytes += bytes;
    return true;
  }

  /** Charges staged bytes before writing them to the isolated repair view. */
  public synchronized boolean consumeStagedBytes(final long bytes) {
    requireNonNegative(bytes);
    if (!canConsume()) {
      return false;
    }
    if (bytes > STAGED_BYTES_LIMIT - stagedBytes) {
      exhausted = true;
      return false;
    }
    stagedBytes += bytes;
    return true;
  }

  /** Charges one repair object before fetching or staging it. */
  public synchronized boolean consumeRepairObject() {
    if (!canConsume()) {
      return false;
    }
    if (repairObjects == OBJECT_LIMIT) {
      exhausted = true;
      return false;
    }
    repairObjects++;
    return true;
  }

  /** Charges cumulative local state transitions, including retries and retargeting work. */
  public synchronized boolean consumeTransitions(final int count) {
    requireNonNegative(count);
    if (!canConsume()) {
      return false;
    }
    if (count > TRANSITION_LIMIT - transitions) {
      exhausted = true;
      return false;
    }
    transitions += count;
    return true;
  }

  private boolean canConsume() {
    return !cancelled && !isExhausted();
  }

  private long elapsedNanos() {
    // Subtraction also handles System.nanoTime() wrapping during this short allowance.
    return Math.max(0L, nanoTime.getAsLong() - startedAt);
  }

  private static void requireNonNegative(final long value) {
    if (value < 0) {
      throw new IllegalArgumentException("Repair budget charges must not be negative");
    }
  }
}
