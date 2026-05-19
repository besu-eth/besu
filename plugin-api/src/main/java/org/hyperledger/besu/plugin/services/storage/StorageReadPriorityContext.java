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
package org.hyperledger.besu.plugin.services.storage;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Thread-local context used to classify storage reads without changing every storage call site. */
public final class StorageReadPriorityContext {

  private static final ThreadLocal<StorageReadPriority> CURRENT_PRIORITY =
      ThreadLocal.withInitial(() -> StorageReadPriority.LOW);
  private static final ThreadLocal<String> CURRENT_SOURCE =
      ThreadLocal.withInitial(() -> "default");
  private static final AtomicInteger ACTIVE_HIGH_PRIORITY_SCOPES = new AtomicInteger();

  private StorageReadPriorityContext() {}

  /** Returns the storage read priority for the current thread. */
  public static StorageReadPriority currentPriority() {
    return CURRENT_PRIORITY.get();
  }

  /** Returns the storage read source for the current thread. */
  public static String currentSource() {
    return CURRENT_SOURCE.get();
  }

  /** Returns whether any thread is currently running a high-priority storage read scope. */
  public static boolean isHighPriorityScopeActive() {
    return ACTIVE_HIGH_PRIORITY_SCOPES.get() > 0;
  }

  /** Returns the number of currently active high-priority storage read scopes. */
  public static int activeHighPriorityScopes() {
    return ACTIVE_HIGH_PRIORITY_SCOPES.get();
  }

  /** Runs the supplier under the supplied storage read priority. */
  public static <T> T withPriority(final StorageReadPriority priority, final Supplier<T> supplier) {
    return withPriority(priority, priority.name().toLowerCase(Locale.ROOT), supplier);
  }

  /** Runs the supplier under the supplied storage read priority and source. */
  public static <T> T withPriority(
      final StorageReadPriority priority, final String source, final Supplier<T> supplier) {
    final StorageReadPriority previousPriority = CURRENT_PRIORITY.get();
    final String previousSource = CURRENT_SOURCE.get();
    final boolean highPriorityScope = priority == StorageReadPriority.HIGH;
    CURRENT_PRIORITY.set(priority);
    CURRENT_SOURCE.set(source);
    if (highPriorityScope) {
      ACTIVE_HIGH_PRIORITY_SCOPES.incrementAndGet();
    }
    try {
      return supplier.get();
    } finally {
      if (highPriorityScope) {
        ACTIVE_HIGH_PRIORITY_SCOPES.decrementAndGet();
      }
      CURRENT_PRIORITY.set(previousPriority);
      CURRENT_SOURCE.set(previousSource);
    }
  }

  /** Runs the runnable under the supplied storage read priority. */
  public static void withPriority(final StorageReadPriority priority, final Runnable runnable) {
    withPriority(priority, priority.name().toLowerCase(Locale.ROOT), runnable);
  }

  /** Runs the runnable under the supplied storage read priority and source. */
  public static void withPriority(
      final StorageReadPriority priority, final String source, final Runnable runnable) {
    withPriority(
        priority,
        source,
        () -> {
          runnable.run();
          return null;
        });
  }
}
