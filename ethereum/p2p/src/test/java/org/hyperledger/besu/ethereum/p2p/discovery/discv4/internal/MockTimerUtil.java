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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MockTimerUtil implements TimerUtil {
  private final AtomicLong nextId = new AtomicLong(0);
  private final Map<Long, TimerHandler> timerHandlers = new HashMap<>();
  private final Map<Long, TimerHandler> periodicHandlers = new HashMap<>();

  @Override
  public long setPeriodic(final long delayInMs, final String name, final TimerHandler handler) {
    long id = nextId.incrementAndGet();
    periodicHandlers.put(id, handler);
    return id;
  }

  @Override
  public long setTimer(final long delayInMs, final TimerHandler handler) {
    long id = nextId.incrementAndGet();
    timerHandlers.put(id, handler);
    return id;
  }

  @Override
  public void cancelTimer(final long timerId) {
    timerHandlers.remove(timerId);
    periodicHandlers.remove(timerId);
  }

  public void runHandlers() {
    runTimerHandlers();
    runPeriodicHandlers();
  }

  public void runTimerHandlers() {
    // Create a copy of the handlers to avoid concurrent modification as handlers run
    final List<TimerHandler> handlers = new ArrayList<>(timerHandlers.values());
    timerHandlers.clear();

    handlers.forEach(TimerHandler::handle);
  }

  public void runPeriodicHandlers() {
    // Create a copy of the handlers to avoid concurrent modification as handlers run
    List<TimerHandler> handlers = new ArrayList<>();
    periodicHandlers.forEach((id, handler) -> handlers.add(handler));

    handlers.forEach(TimerHandler::handle);
  }
}
