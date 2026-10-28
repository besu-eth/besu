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
package org.hyperledger.besu.ethereum.worldstate;

import org.hyperledger.besu.datatypes.Hash;

import java.util.concurrent.CompletableFuture;

/** Shared admission and completion of bounded local-state repair, owned by the synchronizer. */
@FunctionalInterface
public interface WorldStateRepairRequester {
  WorldStateRepairRequester DISABLED =
      request -> CompletableFuture.completedFuture(WorldStateRepairResult.UNSUPPORTED);

  /**
   * Joins an identical target or requests bounded repair of a locally resolved committed root.
   *
   * @param blockHash the locally resolved block
   * @param stateRoot its committed root
   * @return completion after the repair worker drains, or an explicit admission refusal
   */
  default CompletableFuture<WorldStateRepairResult> requestRecovery(
      final Hash blockHash, final Hash stateRoot) {
    return requestRecovery(new WorldStateRepairRequest(blockHash, stateRoot));
  }

  /** Requests repair including the dependency that must be verified before reporting success. */
  CompletableFuture<WorldStateRepairResult> requestRecovery(WorldStateRepairRequest request);

  /** Releases a queued target that has become available. */
  default void markAvailable(final Hash blockHash, final Hash stateRoot) {}

  /** Prevents repeated repair/resume cycles after a resumed session encounters missing state. */
  default void onRepeatedMissingState() {}

  /** Lifecycle generation; stop/start invalidates continuations admitted in earlier generations. */
  default long generation() {
    return 0;
  }

  /** Whether continuations may currently proceed. Unsupported implementations remain inert. */
  default boolean isOpen() {
    return true;
  }
}
