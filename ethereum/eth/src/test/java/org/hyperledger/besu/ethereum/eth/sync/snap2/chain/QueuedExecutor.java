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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/** Keeps what is submitted, to run it in turn on the test thread when the test says. */
public final class QueuedExecutor extends AbstractExecutorService {
  private final List<Runnable> tasks = new ArrayList<>();
  private boolean shutdown;

  public void runAll() {
    while (!tasks.isEmpty()) {
      tasks.removeFirst().run();
      // a pipeline stage, or an interrupted back-fill, leaves the test thread interrupted
      Thread.interrupted();
    }
  }

  @Override
  public void execute(final Runnable command) {
    tasks.add(command);
  }

  @Override
  public void shutdown() {
    shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    return List.of();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return shutdown && tasks.isEmpty();
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit) {
    return isTerminated();
  }
}
