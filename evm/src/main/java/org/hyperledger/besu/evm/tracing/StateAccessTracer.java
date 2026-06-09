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
package org.hyperledger.besu.evm.tracing;

/**
 * Receives per-read state-access events from the path-based world state layer. Implemented by
 * {@link SlowBlockTracer}; the interface lives in {@code evm} so the {@code ethereum/core}
 * accumulator can call it without a circular dependency.
 *
 * <p>Only used on the serial (non-parallel) execution path. Plain (non-synchronized) counters are
 * sufficient.
 */
public interface StateAccessTracer {

  /**
   * Called for each logical account read at the block accumulator.
   *
   * @param cacheHit {@code true} if the account was already present in the accumulator's in-block
   *     cache; {@code false} if it had to be fetched from the underlying world state.
   */
  void traceAccountRead(boolean cacheHit);

  /**
   * Called for each logical storage-slot read at the block accumulator.
   *
   * @param cacheHit {@code true} if the slot was already present in the accumulator's in-block
   *     cache; {@code false} if it had to be fetched from the underlying world state.
   */
  void traceStorageRead(boolean cacheHit);

  /**
   * Called for each {@link org.hyperledger.besu.evm.internal.CodeCache} consultation (i.e. every
   * time {@code getOrCreateCachedCode()} passes the account-local {@code code} shortcut).
   *
   * @param cacheHit {@code true} if the code was found in the cross-block {@code CodeCache}; {@code
   *     false} if it had to be fetched from storage.
   */
  void traceCodeRead(boolean cacheHit);

  /**
   * Called to measure each uncached account, storage or code read
   *
   * @param timeNs time spent accessing state excluding cache
   */
  void addStateReadTime(long timeNs);

  /**
   * Records code size whether cached or not
   *
   * @param size of code in bytes
   */
  void addCodeBytesRead(int size);
}
