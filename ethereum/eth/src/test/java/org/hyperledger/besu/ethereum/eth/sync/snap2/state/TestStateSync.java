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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import org.hyperledger.besu.ethereum.eth.sync.snap2.QueuedExecutor;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.TestTries;

/** State downloads on the test thread, for the tests of the sync. */
public final class TestStateSync {

  private TestStateSync() {}

  /**
   * Returns a download whose loop, writes and storage trie updates run on {@code tasks}, with a
   * clock that stands still, so that a peer left out stays out.
   */
  public static StateSync create(
      final SyncStateStorage storage,
      final RequestSender sender,
      final StateSync.Settings settings,
      final QueuedExecutor tasks) {
    return new StateSync(
        storage,
        sender,
        settings,
        tasks,
        tasks,
        () -> 0L,
        (withoutMoves, trieLeft) ->
            TestTries.updaterWithoutWorkers(storage, withoutMoves, trieLeft));
  }
}
