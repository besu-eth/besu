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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/** JDK-based {@link PeerDiscoveryController.AsyncExecutor} backed by an {@link ExecutorService}. */
public final class ScheduledExecutorAsyncExecutor implements PeerDiscoveryController.AsyncExecutor {

  private final ExecutorService executor;

  public ScheduledExecutorAsyncExecutor(final ExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public <T> CompletableFuture<T> execute(final Supplier<T> action) {
    final CompletableFuture<T> result = new CompletableFuture<>();
    executor.submit(
        () -> {
          try {
            result.complete(action.get());
          } catch (final Throwable t) {
            result.completeExceptionally(t);
          }
        });
    return result;
  }
}
