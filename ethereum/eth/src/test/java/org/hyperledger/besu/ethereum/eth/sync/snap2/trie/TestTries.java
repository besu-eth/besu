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
package org.hyperledger.besu.ethereum.eth.sync.snap2.trie;

import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;

import java.util.function.Predicate;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tuweni.bytes.Bytes32;

/** Storage trie updates and trie builds on the calling thread, for the tests of the download. */
public final class TestTries {

  // as many as a build commits at once
  private static final int FIXES_PER_COMMIT = 10_000;

  private TestTries() {}

  /** Returns an updater without workers: the thread that finishes it applies the updates. */
  public static StorageTrieUpdater updaterWithoutWorkers(
      final SyncStateStorage storage,
      final Predicate<Runnable> withoutMoves,
      final Predicate<Bytes32> trieLeft) {
    return new StorageTrieUpdater(storage, withoutMoves, trieLeft, 0, Integer.MAX_VALUE);
  }

  /** Builds the tries of the flat state, its partitions one after the other. */
  public static WorldStateTrieBuilder.Result build(
      final SyncStateStorage storage, final Predicate<Bytes32> trieLeft) {
    return builder(storage, trieLeft).build();
  }

  /** Returns a build whose partitions run one after the other, on the thread that builds. */
  public static WorldStateTrieBuilder builder(
      final SyncStateStorage storage, final Predicate<Bytes32> trieLeft) {
    return new WorldStateTrieBuilder(
        storage, trieLeft, MoreExecutors::newDirectExecutorService, FIXES_PER_COMMIT);
  }
}
