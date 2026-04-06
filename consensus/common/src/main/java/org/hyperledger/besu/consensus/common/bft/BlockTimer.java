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
package org.hyperledger.besu.consensus.common.bft;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.events.BlockTimerExpiry;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for starting and keeping organised block timers */
public class BlockTimer {

  private static final Logger LOG = LoggerFactory.getLogger(BlockTimer.class);

  private final ForksSchedule<? extends BftConfigOptions> forksSchedule;
  private final BftExecutors bftExecutors;
  private Optional<ScheduledFuture<?>> currentTimerTask;
  private final BftEventQueue queue;
  private final Clock clock;
  private long blockPeriodSeconds;
  private long emptyBlockPeriodSeconds;

  /**
   * Construct a BlockTimer with primed executor service ready to start timers
   *
   * @param queue The queue in which to put block expiry events
   * @param forksSchedule Bft fork schedule that contains block period seconds
   * @param bftExecutors Executor services that timers can be scheduled with
   * @param clock System clock
   */
  public BlockTimer(
      final BftEventQueue queue,
      final ForksSchedule<? extends BftConfigOptions> forksSchedule,
      final BftExecutors bftExecutors,
      final Clock clock) {
    this.queue = queue;
    this.forksSchedule = forksSchedule;
    this.bftExecutors = bftExecutors;
    this.currentTimerTask = Optional.empty();
    this.clock = clock;
    this.blockPeriodSeconds = 0;
    this.emptyBlockPeriodSeconds = 0;
  }

  /** Cancels the current running round timer if there is one */
  public synchronized void cancelTimer() {
    currentTimerTask.ifPresent(t -> t.cancel(false));
    currentTimerTask = Optional.empty();
  }

  /**
   * Whether there is a timer currently running or not
   *
   * @return boolean of whether a timer is ticking or not
   */
  public synchronized boolean isRunning() {
    return currentTimerTask.map(t -> !t.isDone()).orElse(false);
  }

  /**
   * Starts a timer for the supplied round cancelling any previously active block timer
   *
   * @param round The round identifier which this timer is tracking
   * @param headerTimestamp The timestamp from the of the chain head header
   */
  public synchronized void startTimer(
      final ConsensusRoundIdentifier round, final Supplier<Long> headerTimestamp) {
    cancelTimer();

    final BftConfigOptions currentForkOptions =
        forksSchedule.getFork(round.getSequenceNumber(), headerTimestamp.get()).getValue();

    final long expiryTime;
    int currentBlockPeriodSeconds = currentForkOptions.getBlockPeriodSeconds();
    final int nextBlockPeriodSeconds =
        forksSchedule
            .getFork(round.getSequenceNumber(), headerTimestamp.get() + currentBlockPeriodSeconds)
            .getValue()
            .getBlockPeriodSeconds();

    // If the block period seconds change between the current block and the next one we need to
    // produce this block on the longer of the two values,
    // otherwise block validation will fail (blocks produced too close together)
    if (nextBlockPeriodSeconds > currentBlockPeriodSeconds) {
      currentBlockPeriodSeconds = nextBlockPeriodSeconds;
    }

    // Experimental option for test scenarios only. Not for production use.
    final long blockPeriodMilliseconds = currentForkOptions.getBlockPeriodMilliseconds();
    if (blockPeriodMilliseconds > 0) {
      // Experimental mode for setting < 1 second block periods e.g. for CI/CD pipelines
      // running tests against Besu
      expiryTime = clock.millis() + blockPeriodMilliseconds;
      LOG.warn(
          "Test-mode only xblockperiodmilliseconds has been set to {} millisecond blocks. Do not use in a production system.",
          blockPeriodMilliseconds);
    } else {
      // absolute time when the timer is supposed to expire
      final long minimumTimeBetweenBlocksMillis = currentBlockPeriodSeconds * 1000L;
      expiryTime = headerTimestamp.get() * 1_000 + minimumTimeBetweenBlocksMillis;
    }

    final int emptyBlockPeriodSeconds = currentForkOptions.getEmptyBlockPeriodSeconds();
    setBlockTimes(currentBlockPeriodSeconds, emptyBlockPeriodSeconds);

    startTimer(round, expiryTime);
  }

  /**
   * Checks if the empty block timer is expired
   *
   * @param headerTimestamp Function to get the chain head timestamp
   * @param currentTimeInMillis The current time
   * @return a boolean value
   */
  public synchronized boolean checkEmptyBlockExpired(
      final Supplier<Long> headerTimestamp, final long currentTimeInMillis) {
    final long emptyBlockPeriodExpiryTime =
        (headerTimestamp.get() + emptyBlockPeriodSeconds) * 1000;

    if (currentTimeInMillis > emptyBlockPeriodExpiryTime) {
      LOG.debug("Empty Block expired");
      return true;
    }
    LOG.debug("Empty Block NOT expired");
    return false;
  }

  /**
   * Resets the empty block timer
   *
   * @param roundIdentifier The current round identifier
   * @param headerTimestamp Function to get timestamp from the header of the chain head
   * @param currentTimeInMillis The current time
   */
  public void resetTimerForEmptyBlock(
      final ConsensusRoundIdentifier roundIdentifier,
      final Supplier<Long> headerTimestamp,
      final long currentTimeInMillis) {
    final long emptyBlockPeriodExpiryTime =
        (headerTimestamp.get() + emptyBlockPeriodSeconds) * 1000;
    final long nextBlockPeriodExpiryTime = currentTimeInMillis + blockPeriodSeconds * 1000;

    startTimer(roundIdentifier, Math.min(emptyBlockPeriodExpiryTime, nextBlockPeriodExpiryTime));
  }

  private synchronized void startTimer(
      final ConsensusRoundIdentifier round, final long expiryTime) {
    cancelTimer();
    final long now = clock.millis();

    if (expiryTime > now) {
      final long delay = expiryTime - now;

      final Runnable newTimerRunnable = () -> queue.add(new BlockTimerExpiry(round));

      final ScheduledFuture<?> newTimerTask =
          bftExecutors.scheduleTask(newTimerRunnable, delay, TimeUnit.MILLISECONDS);
      currentTimerTask = Optional.of(newTimerTask);
    } else {
      queue.add(new BlockTimerExpiry(round));
    }
  }

  private synchronized void setBlockTimes(
      final int blockPeriodSeconds, final int emptyBlockPeriodSeconds) {
    this.blockPeriodSeconds = blockPeriodSeconds;
    this.emptyBlockPeriodSeconds = emptyBlockPeriodSeconds;
  }

  /**
   * Retrieves the Block Period Seconds
   *
   * @return the Block Period Seconds
   */
  public synchronized long getBlockPeriodSeconds() {
    return blockPeriodSeconds;
  }

  /**
   * Retrieves the Empty Block Period Seconds
   *
   * @return the Empty Block Period Seconds
   */
  public synchronized long getEmptyBlockPeriodSeconds() {
    return emptyBlockPeriodSeconds;
  }
}
