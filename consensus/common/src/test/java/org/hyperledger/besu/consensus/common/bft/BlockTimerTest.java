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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.JsonBftConfigOptions;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.events.BftEvent;
import org.hyperledger.besu.consensus.common.bft.events.BlockTimerExpiry;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.time.Clock;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BlockTimerTest {

  private BftExecutors bftExecutors;
  private BftEventQueue mockQueue;
  private Clock mockClock;
  private ForksSchedule<BftConfigOptions> mockForksSchedule;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void initialise() {
    bftExecutors = mock(BftExecutors.class);
    mockQueue = mock(BftEventQueue.class);
    mockClock = mock(Clock.class);
    mockForksSchedule = mock(ForksSchedule.class);
  }

  @Test
  public void cancelTimerCancelsWhenNoTimer() {
    final BlockTimer timer = new BlockTimer(mockQueue, mockForksSchedule, bftExecutors, mockClock);
    // Starts with nothing running
    assertThat(timer.isRunning()).isFalse();
    // cancel shouldn't die if there's nothing running
    timer.cancelTimer();
    // there is still nothing running
    assertThat(timer.isRunning()).isFalse();
  }

  @Test
  public void startTimerSchedulesCorrectlyWhenExpiryIsInTheFuture() {
    final int MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS = 15;
    final int MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS = 60;
    final long NOW_MILLIS = 505_000L;
    final long BLOCK_TIME_STAMP = 500L;
    final long EXPECTED_DELAY = 10_000L;

    when(mockForksSchedule.getFork(anyLong(), anyLong()))
        .thenReturn(
            new ForkSpec<>(
                0,
                createBftFork(
                    MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS,
                    MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS)));

    final BlockTimer timer = new BlockTimer(mockQueue, mockForksSchedule, bftExecutors, mockClock);

    when(mockClock.millis()).thenReturn(NOW_MILLIS);

    final BlockHeader header =
        new BlockHeaderTestFixture().timestamp(BLOCK_TIME_STAMP).buildHeader();
    final ConsensusRoundIdentifier round =
        new ConsensusRoundIdentifier(0xFEDBCA9876543210L, 0x12345678);

    final ScheduledFuture<?> mockedFuture = mock(ScheduledFuture.class);
    Mockito.<ScheduledFuture<?>>when(
            bftExecutors.scheduleTask(any(Runnable.class), anyLong(), any()))
        .thenReturn(mockedFuture);

    timer.startTimer(round, header::getTimestamp);
    verify(bftExecutors)
        .scheduleTask(any(Runnable.class), eq(EXPECTED_DELAY), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void startTimerSchedulesCorrectlyOnBlockPeriodTransition() {
    final int INITIAL_TIME_BETWEEN_BLOCKS_SECONDS = 15;
    final int NEW_TIME_BETWEEN_BLOCKS_SECONDS = 30;
    final long NOW_MILLIS = 505_000L;
    final long BLOCK_TIME_STAMP = 500L;
    final long EXPECTED_DELAY = 25_000L;

    // Simulate a transition from 15s block period to 30s block period. The BFT timer should
    // reflect the larger of the two block periods to ensure that the produced block passes
    // validation which will be based on the post-transition time.
    when(mockForksSchedule.getFork(anyLong(), anyLong()))
        .thenReturn(new ForkSpec<>(0, createBftFork(INITIAL_TIME_BETWEEN_BLOCKS_SECONDS)));

    when(mockForksSchedule.getFork(
            anyLong(), eq(BLOCK_TIME_STAMP + INITIAL_TIME_BETWEEN_BLOCKS_SECONDS)))
        .thenReturn(new ForkSpec<>(0, createBftFork(NEW_TIME_BETWEEN_BLOCKS_SECONDS)));

    final BlockTimer timer = new BlockTimer(mockQueue, mockForksSchedule, bftExecutors, mockClock);

    when(mockClock.millis()).thenReturn(NOW_MILLIS);

    final BlockHeader header =
        new BlockHeaderTestFixture().timestamp(BLOCK_TIME_STAMP).buildHeader();
    final ConsensusRoundIdentifier round =
        new ConsensusRoundIdentifier(0xFEDBCA9876543210L, 0x12345678);

    final ScheduledFuture<?> mockedFuture = mock(ScheduledFuture.class);
    Mockito.<ScheduledFuture<?>>when(
            bftExecutors.scheduleTask(any(Runnable.class), anyLong(), any()))
        .thenReturn(mockedFuture);

    timer.startTimer(round, header::getTimestamp);
    verify(bftExecutors)
        .scheduleTask(any(Runnable.class), eq(EXPECTED_DELAY), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void aBlockTimerExpiryEventIsAddedToTheQueueOnExpiry() throws InterruptedException {
    final int MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS = 1;
    final int MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS = 10;
    final long NOW_MILLIS = 300_500L;
    final long BLOCK_TIME_STAMP = 300;
    final long EXPECTED_DELAY = 500;

    when(mockForksSchedule.getFork(anyLong(), anyLong()))
        .thenReturn(
            new ForkSpec<>(
                0,
                createBftFork(
                    MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS,
                    MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS)));
    when(mockClock.millis()).thenReturn(NOW_MILLIS);

    final BlockHeader header =
        new BlockHeaderTestFixture().timestamp(BLOCK_TIME_STAMP).buildHeader();
    final ConsensusRoundIdentifier round =
        new ConsensusRoundIdentifier(0xFEDBCA9876543210L, 0x12345678);

    final ScheduledFuture<?> mockedFuture = mock(ScheduledFuture.class);
    Mockito.<ScheduledFuture<?>>when(
            bftExecutors.scheduleTask(any(Runnable.class), anyLong(), any()))
        .thenReturn(mockedFuture);

    final BftEventQueue eventQueue = new BftEventQueue(1000);
    final BlockTimer timer = new BlockTimer(eventQueue, mockForksSchedule, bftExecutors, mockClock);
    timer.startTimer(round, header::getTimestamp);

    // Verify that the event will not be added to the queue immediately
    assertThat(eventQueue.isEmpty()).isTrue();

    // Verify that a task is sceheduled for EXPECTED_DELAY milliseconds in the future
    ArgumentCaptor<Runnable> expiryTask = ArgumentCaptor.forClass(Runnable.class);
    verify(bftExecutors, times(1))
        .scheduleTask(expiryTask.capture(), eq(EXPECTED_DELAY), eq(TimeUnit.MILLISECONDS));

    // assert that the task puts a BlockExpired event into the queue
    final Runnable scheduledTask = expiryTask.getValue();
    assertThat(eventQueue.isEmpty()).isTrue();
    scheduledTask.run();
    assertThat(eventQueue.size()).isEqualTo(1);
    final BftEvent queuedEvent = eventQueue.poll(0, TimeUnit.SECONDS);
    assertThat(queuedEvent).isInstanceOf(BlockTimerExpiry.class);
    assertThat(((BlockTimerExpiry) queuedEvent).getRoundIdentifier())
        .usingRecursiveComparison()
        .isEqualTo(round);
  }

  @Test
  public void eventIsImmediatelyAddedToTheQueueIfAbsoluteExpiryIsEqualToNow() {
    final int MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS = 15;
    final int MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS = 60;
    final long NOW_MILLIS = 515_000L;
    final long BLOCK_TIME_STAMP = 500;

    when(mockForksSchedule.getFork(anyLong(), anyLong()))
        .thenReturn(
            new ForkSpec<>(
                0,
                createBftFork(
                    MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS,
                    MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS)));

    final BlockTimer timer = new BlockTimer(mockQueue, mockForksSchedule, bftExecutors, mockClock);

    when(mockClock.millis()).thenReturn(NOW_MILLIS);

    final BlockHeader header =
        new BlockHeaderTestFixture().timestamp(BLOCK_TIME_STAMP).buildHeader();
    final ConsensusRoundIdentifier round =
        new ConsensusRoundIdentifier(0xFEDBCA9876543210L, 0x12345678);

    timer.startTimer(round, header::getTimestamp);
    verify(bftExecutors, never()).scheduleTask(any(Runnable.class), anyLong(), any());

    final ArgumentCaptor<BftEvent> bftEventCaptor = ArgumentCaptor.forClass(BftEvent.class);
    verify(mockQueue).add(bftEventCaptor.capture());

    assertThat(bftEventCaptor.getValue() instanceof BlockTimerExpiry).isTrue();
    assertThat(((BlockTimerExpiry) bftEventCaptor.getValue()).getRoundIdentifier())
        .usingRecursiveComparison()
        .isEqualTo(round);
  }

  @Test
  public void eventIsImmediatelyAddedToTheQueueIfAbsoluteExpiryIsInThePast() {
    final int MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS = 15;
    final int MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS = 60;
    final long NOW_MILLIS = 520_000L;
    final long BLOCK_TIME_STAMP = 500L;

    when(mockForksSchedule.getFork(anyLong(), anyLong()))
        .thenReturn(
            new ForkSpec<>(
                0,
                createBftFork(
                    MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS,
                    MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS)));

    final BlockTimer timer = new BlockTimer(mockQueue, mockForksSchedule, bftExecutors, mockClock);

    when(mockClock.millis()).thenReturn(NOW_MILLIS);

    final BlockHeader header =
        new BlockHeaderTestFixture().timestamp(BLOCK_TIME_STAMP).buildHeader();
    final ConsensusRoundIdentifier round =
        new ConsensusRoundIdentifier(0xFEDBCA9876543210L, 0x12345678);

    timer.startTimer(round, header::getTimestamp);
    verify(bftExecutors, never()).scheduleTask(any(Runnable.class), anyLong(), any());

    final ArgumentCaptor<BftEvent> bftEventCaptor = ArgumentCaptor.forClass(BftEvent.class);
    verify(mockQueue).add(bftEventCaptor.capture());

    assertThat(bftEventCaptor.getValue() instanceof BlockTimerExpiry).isTrue();
    assertThat(((BlockTimerExpiry) bftEventCaptor.getValue()).getRoundIdentifier())
        .usingRecursiveComparison()
        .isEqualTo(round);
  }

  @Test
  public void startTimerCancelsExistingTimer() {
    final int MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS = 15;
    final int MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS = 60;
    final long NOW_MILLIS = 500_000L;
    final long BLOCK_TIME_STAMP = 500L;

    when(mockForksSchedule.getFork(anyLong(), anyLong()))
        .thenReturn(
            new ForkSpec<>(
                0,
                createBftFork(
                    MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS,
                    MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS)));

    final BlockTimer timer = new BlockTimer(mockQueue, mockForksSchedule, bftExecutors, mockClock);

    when(mockClock.millis()).thenReturn(NOW_MILLIS);

    final BlockHeader header =
        new BlockHeaderTestFixture().timestamp(BLOCK_TIME_STAMP).buildHeader();
    final ConsensusRoundIdentifier round =
        new ConsensusRoundIdentifier(0xFEDBCA9876543210L, 0x12345678);
    final ScheduledFuture<?> mockedFuture = mock(ScheduledFuture.class);

    Mockito.<ScheduledFuture<?>>when(
            bftExecutors.scheduleTask(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockedFuture);
    timer.startTimer(round, header::getTimestamp);
    verify(mockedFuture, times(0)).cancel(false);
    timer.startTimer(round, header::getTimestamp);
    verify(mockedFuture, times(1)).cancel(false);
  }

  @Test
  public void runningFollowsTheStateOfTheTimer() {
    final int MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS = 15;
    final int MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS = 60;
    final long NOW_MILLIS = 500_000L;
    final long BLOCK_TIME_STAMP = 500L;

    when(mockForksSchedule.getFork(anyLong(), anyLong()))
        .thenReturn(
            new ForkSpec<>(
                0,
                createBftFork(
                    MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS,
                    MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS)));

    final BlockTimer timer = new BlockTimer(mockQueue, mockForksSchedule, bftExecutors, mockClock);

    when(mockClock.millis()).thenReturn(NOW_MILLIS);

    final BlockHeader header =
        new BlockHeaderTestFixture().timestamp(BLOCK_TIME_STAMP).buildHeader();
    final ConsensusRoundIdentifier round =
        new ConsensusRoundIdentifier(0xFEDBCA9876543210L, 0x12345678);

    final ScheduledFuture<?> mockedFuture = mock(ScheduledFuture.class);
    Mockito.<ScheduledFuture<?>>when(
            bftExecutors.scheduleTask(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockedFuture);
    timer.startTimer(round, header::getTimestamp);
    when(mockedFuture.isDone()).thenReturn(false);
    assertThat(timer.isRunning()).isTrue();
    when(mockedFuture.isDone()).thenReturn(true);
    assertThat(timer.isRunning()).isFalse();
  }

  @Test
  public void checkBlockTimerEmptyAndNonEmptyPeriodSecods() {
    final int MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS = 15;
    final int MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS = 60;
    final long BLOCK_TIME_STAMP = 500L;
    final ConsensusRoundIdentifier round =
        new ConsensusRoundIdentifier(0xFEDBCA9876543210L, 0x12345678);
    final BlockHeader header =
        new BlockHeaderTestFixture().timestamp(BLOCK_TIME_STAMP).buildHeader();
    final ScheduledFuture<?> mockedFuture = mock(ScheduledFuture.class);
    Mockito.<ScheduledFuture<?>>when(
            bftExecutors.scheduleTask(any(Runnable.class), anyLong(), any()))
        .thenReturn(mockedFuture);

    when(mockForksSchedule.getFork(anyLong(), anyLong()))
        .thenReturn(
            new ForkSpec<>(
                0,
                createBftFork(
                    MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS,
                    MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS)));

    final BlockTimer timer = new BlockTimer(mockQueue, mockForksSchedule, bftExecutors, mockClock);
    timer.startTimer(round, header::getTimestamp);

    assertThat(timer.getBlockPeriodSeconds()).isEqualTo(MINIMAL_TIME_BETWEEN_BLOCKS_SECONDS);
    assertThat(timer.getEmptyBlockPeriodSeconds())
        .isEqualTo(MINIMAL_TIME_BETWEEN_EMPTY_BLOCKS_SECONDS);
  }

  private BftConfigOptions createBftFork(final int blockPeriodSeconds) {
    final MutableBftConfigOptions bftConfigOptions =
        new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT);
    bftConfigOptions.setBlockPeriodSeconds(blockPeriodSeconds);
    return bftConfigOptions;
  }

  private BftConfigOptions createBftFork(
      final int blockPeriodSeconds, final int emptyBlockPeriodSeconds) {
    final MutableBftConfigOptions bftConfigOptions =
        new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT);
    bftConfigOptions.setBlockPeriodSeconds(blockPeriodSeconds);
    bftConfigOptions.setEmptyBlockPeriodSeconds(emptyBlockPeriodSeconds);
    return bftConfigOptions;
  }
}
