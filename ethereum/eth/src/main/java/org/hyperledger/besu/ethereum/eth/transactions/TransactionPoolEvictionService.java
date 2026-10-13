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
package org.hyperledger.besu.ethereum.eth.transactions;

import java.time.Duration;
import java.util.Optional;

import io.vertx.core.Vertx;

public class TransactionPoolEvictionService {
  private final Vertx vertx;
  private final TransactionPool transactionPool;
  private final Duration evictionInterval;
  private Optional<Long> timerId = Optional.empty();

  public TransactionPoolEvictionService(final Vertx vertx, final TransactionPool transactionPool) {
    this(vertx, transactionPool, TransactionPoolConfiguration.DEFAULT_TX_EVICTION_INTERVAL);
  }

  public TransactionPoolEvictionService(
      final Vertx vertx, final TransactionPool transactionPool, final Duration evictionInterval) {
    this.vertx = vertx;
    this.transactionPool = transactionPool;
    this.evictionInterval = evictionInterval;
  }

  public Duration getEvictionInterval() {
    return evictionInterval;
  }

  public void start() {
    assert (!timerId.isPresent());
    timerId =
        Optional.of(
            vertx.setPeriodic(
                Math.max(1, evictionInterval.toMillis()),
                id -> {
                  if (transactionPool.isEnabled()) {
                    transactionPool.evictOldTransactions();
                  }
                }));
  }

  public void stop() {
    if (timerId.isPresent()) {
      vertx.cancelTimer(timerId.get());
      timerId = Optional.empty();
    }
  }
}
