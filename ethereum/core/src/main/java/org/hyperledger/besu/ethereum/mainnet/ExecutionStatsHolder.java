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
package org.hyperledger.besu.ethereum.mainnet;

import java.util.Optional;

/**
 * Thread-local holder for ExecutionStats to enable metrics collection during EVM execution.
 *
 * <p>This class provides a mechanism to pass ExecutionStats through the execution stack without
 * modifying method signatures. EVM operations can access the current block's stats to record
 * metrics like SLOAD/SSTORE counts, state accesses, and cache statistics.
 */
public final class ExecutionStatsHolder {

  private static final ThreadLocal<ExecutionStats> CURRENT = new ThreadLocal<>();

  private ExecutionStatsHolder() {
    // Utility class
  }

  /**
   * Sets the current ExecutionStats for this thread.
   *
   * @param stats the ExecutionStats instance for the current block processing
   */
  public static void set(final ExecutionStats stats) {
    CURRENT.set(stats);
  }

  /**
   * Gets the current ExecutionStats for this thread.
   *
   * @return the current ExecutionStats, or null if not set
   */
  public static ExecutionStats get() {
    return CURRENT.get();
  }

  /**
   * Gets the current ExecutionStats as an Optional.
   *
   * @return Optional containing the current ExecutionStats, or empty if not set
   */
  public static Optional<ExecutionStats> getOptional() {
    return Optional.ofNullable(CURRENT.get());
  }

  /** Clears the current ExecutionStats for this thread. Should be called after block processing. */
  public static void clear() {
    CURRENT.remove();
  }
}
