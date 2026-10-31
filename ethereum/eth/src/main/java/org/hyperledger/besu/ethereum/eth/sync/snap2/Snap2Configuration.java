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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;

import java.time.Duration;
import java.util.Optional;

/**
 * Tuning of the SNAP2 sync.
 *
 * @param state the sizes of the state requests
 * @param chain the sizes of the chain downloads, and where proof of stake starts
 * @param moves when the pivot moves
 * @param workerThreads threads that verify the responses; as many write them
 * @param trieBuildThreads threads of the final trie build
 * @param pollInterval how often the pivot is checked against the chain head
 */
record Snap2Configuration(
    StateSync.Settings state,
    SyncChain.Settings chain,
    PivotMoves.Settings moves,
    int workerThreads,
    int trieBuildThreads,
    Duration pollInterval) {

  /** Returns the tuning of a node, with what it configures. */
  static Snap2Configuration create(
      final int headersPerRequest,
      final boolean headersToCheckpointOnly,
      final boolean indexTransactions,
      final Optional<Difficulty> terminalTotalDifficulty) {
    final int threads = Math.max(2, Runtime.getRuntime().availableProcessors());
    return new Snap2Configuration(
        new StateSync.Settings(
            65_536, // account ranges
            512 * 1024, // bytes an answer
            256, // storages a request
            64, // ranges of a large storage
            64), // bytecodes a request
        new SyncChain.Settings(
            headersPerRequest,
            64, // header requests at once
            128, // blocks whose bodies are stored before the chain head is set
            headersToCheckpointOnly,
            indexTransactions,
            terminalTotalDifficulty),
        // the pivot 64 blocks behind the head, moved once the head is 96 blocks ahead
        new PivotMoves.Settings(64, 32),
        threads,
        threads,
        Duration.ofSeconds(2));
  }
}
