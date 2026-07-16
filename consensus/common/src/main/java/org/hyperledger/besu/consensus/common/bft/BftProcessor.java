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

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Execution context for draining queued bft events and applying them to a maintained state */
public class BftProcessor implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(BftProcessor.class);

  private final BftEventQueue incomingQueue;
  private volatile boolean shutdown = false;
  private final EventMultiplexer eventMultiplexer;
  private volatile CountDownLatch shutdownLatch = new CountDownLatch(1);

  /**
   * Construct a new BftProcessor
   *
   * @param incomingQueue The event queue from which to drain new events
   * @param eventMultiplexer an object capable of handling any/all BFT events
   */
  public BftProcessor(final BftEventQueue incomingQueue, final EventMultiplexer eventMultiplexer) {
    this.incomingQueue = incomingQueue;
    this.eventMultiplexer = eventMultiplexer;
  }

  /** Indicate to the processor that it can be started */
  public synchronized void start() {
    shutdownLatch = new CountDownLatch(1);
    shutdown = false;
  }

  /** Indicate to the processor that it should gracefully stop at its next opportunity */
  public synchronized void stop() {
    shutdown = true;
  }

  /**
   * Await stop.
   *
   * @throws InterruptedException the interrupted exception
   */
  public void awaitStop() throws InterruptedException {
    shutdownLatch.await();
  }

  @Override
  public void run() {
    final CountDownLatch currentShutdownLatch = shutdownLatch;
    try {
      // Start the event queue. Until it is started it won't accept new events from peers
      incomingQueue.start();

      while (!shutdown) {
        nextEvent().ifPresent(eventMultiplexer::handleBftEvent);
      }

      incomingQueue.stop();
    } catch (final Throwable t) {
      LOG.error("BFT Mining thread has suffered a fatal error, mining has been halted", t);
    }
    // Clean up the executor service the round timer has been utilising
    LOG.info("Shutting down BFT event processor");
    currentShutdownLatch.countDown();
  }

  private Optional<BftEvent> nextEvent() {
    try {
      return Optional.ofNullable(incomingQueue.poll(500, TimeUnit.MILLISECONDS));
    } catch (final InterruptedException interrupt) {
      // If the queue was interrupted propagate it and spin to check our shutdown status
      Thread.currentThread().interrupt();
      return Optional.empty();
    }
  }
}
