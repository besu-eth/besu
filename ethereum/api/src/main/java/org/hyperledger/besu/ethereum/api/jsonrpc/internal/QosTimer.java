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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;

public class QosTimer {

  private final Vertx timerVertx;
  private final AtomicLong timerId = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong lastReset = new AtomicLong(System.currentTimeMillis());
  private volatile boolean stopped = false;

  private final long periodMillis;
  private final Consumer<Long> consumerTask;

  public QosTimer(
      final Vertx timerVertx, final long periodMillis, final Consumer<Long> consumerTask) {
    this.timerVertx = timerVertx;
    this.periodMillis = periodMillis;
    this.consumerTask = consumerTask;
    resetTimer();
  }

  public void resetTimer() {
    if (stopped) {
      return;
    }
    lastReset.set(System.currentTimeMillis());
    resetTimerHandler(timerHandler());
  }

  public void stop() {
    stopped = true;
    timerVertx.cancelTimer(timerId.get());
  }

  void resetTimerHandler(final Handler<Long> timerHandler) {
    timerVertx.cancelTimer(timerId.get());
    if (!stopped) {
      timerId.set(timerVertx.setTimer(periodMillis, timerHandler));
    }
  }

  Handler<Long> timerHandler() {
    return z -> {
      if (stopped) {
        return;
      }
      var lastCall = getLastCallMillis();
      var now = System.currentTimeMillis();
      if (lastCall + periodMillis < now) {
        consumerTask.accept(lastCall);
      }
      resetTimerHandler(timerHandler());
    };
  }

  long getLastCallMillis() {
    return lastReset.get();
  }
}
