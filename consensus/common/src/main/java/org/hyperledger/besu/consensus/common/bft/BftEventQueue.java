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

import org.hyperledger.besu.consensus.common.bft.events.BftEvent;
import org.hyperledger.besu.consensus.common.bft.events.BftEvents;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Threadsafe queue that lets parts of the system inform the Bft infrastructure about events */
public class BftEventQueue {
  private final BlockingQueue<BftEvent> queue = new LinkedBlockingQueue<>();

  private static final Logger LOG = LoggerFactory.getLogger(BftEventQueue.class);
  private final int messageQueueLimit;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Instantiates a new Bft event queue.
   *
   * @param messageQueueLimit the message queue limit
   */
  public BftEventQueue(final int messageQueueLimit) {
    this.messageQueueLimit = messageQueueLimit;
  }

  /** Start the event queue. Until it has been started no events will be queued for processing. */
  public void start() {
    started.set(true);
  }

  /** Stop the event queue. No events will be queued for processing until it is started. */
  public void stop() {
    started.set(false);
  }

  private boolean isStarted() {
    return started.get();
  }

  /**
   * Put an Bft event onto the queue. Note: the event queue must be started before an event will be
   * queued for processing. Events received before the queue is started will be discarded.
   *
   * @param event Provided bft event
   */
  public void add(final BftEvent event) {

    // Don't queue events other than block timer expiry, until we know we can process them
    if (isStarted() || event.getType() == BftEvents.Type.BLOCK_TIMER_EXPIRY) {
      if (queue.size() > messageQueueLimit) {
        LOG.warn("Queue size exceeded trying to add new bft event {}", event);
      } else {
        queue.add(event);
      }
    }
  }

  /**
   * Size of queue.
   *
   * @return the int
   */
  public int size() {
    return queue.size();
  }

  /**
   * Is empty.
   *
   * @return the boolean
   */
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  /**
   * Blocking request for the next item available on the queue that will timeout after a specified
   * period
   *
   * @param timeout number of time units after which this operation should timeout
   * @param unit the time units in which to count
   * @return The next BftEvent to become available on the queue or null if the expiry passes
   * @throws InterruptedException If the underlying queue implementation is interrupted
   */
  @Nullable
  public BftEvent poll(final long timeout, final TimeUnit unit) throws InterruptedException {
    return queue.poll(timeout, unit);
  }
}
