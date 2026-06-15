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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Per-block storage tracker invoked by {@link FrontierRootHashTracker} to apply per-account storage
 * deltas during frontier-era receipt computation. Implementations:
 *
 * <ul>
 *   <li>{@link CachingFrontierStorageRootTracker} — keeps each account's storage trie alive in
 *       memory across {@code update} calls within a block, applying only the slot deltas since the
 *       last call.
 *   <li>{@link #NO_OP} — does nothing; used when {@code WorldStateConfig.isTrieDisabled()} is true.
 * </ul>
 */
public interface FrontierStorageRootTracker extends FrontierRootHashTracker.StorageRootUpdater {

  /** Discards any cached state. Call at the block boundary before processing a new block. */
  void reset();

  /**
   * Sentinel that performs no work. Wired in when {@code WorldStateConfig.isTrieDisabled()} is
   * true.
   *
   * <p>The frontier path is only reachable via local block execution ({@code
   * MainnetProtocolSpecs.transactionReceiptFactory}, for pre-Byzantium receipts). Local execution
   * runs on full sync, which never sets {@code isTrieDisabled}. The only setter today is {@code
   * SynchronizationServiceImpl#disableWorldStateTrie} (snap sync), which also clears the trie
   * storage and has no re-enabler; snap sync gets its receipts from peers instead of recomputing
   * them. So this NO_OP is dead code in practice; it exists to keep the caching impl branch-free on
   * the flag.
   *
   * <p>The wiring in {@code BonsaiWorldState} picks the implementation once at construction time.
   * That relies on {@code isTrieDisabled} being one-way ({@code false → true} only). If someone
   * ever adds {@code setTrieDisabled(false)} — e.g. for post-snap-sync trie healing — a state
   * constructed while disabled would keep this NO_OP forever and silently produce wrong receipts.
   * Revisit the wiring in {@code BonsaiWorldState} then, not this class.
   */
  FrontierStorageRootTracker NO_OP =
      new FrontierStorageRootTracker() {
        @Override
        public void update(
            final Address address,
            final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageUpdates) {
          // intentionally empty
        }

        @Override
        public void reset() {
          // intentionally empty
        }
      };
}
