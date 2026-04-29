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
package org.hyperledger.besu.ethereum.trie.pathbased.common.provider;

/**
 * Thrown when a trie-log roll cannot stay within {@link TrieLogManager#getMaxLayersToLoad()}:
 * either the chain head is that many blocks (or more) above the target block, or the planned
 * trie-log op count exceeds the limit.
 *
 * <p>Propagates to engine fork choice when {@link
 * WorldStateQueryParams#shouldEnforceTrieRollLayerBudget()} is true with {@link
 * WorldStateQueryParams#shouldWorldStateUpdateHead()}. For cache / historical reads, {@link
 * PathBasedWorldStateProvider#getFullWorldStateFromCache} catches this exception and returns an
 * empty Optional.
 */
public final class TrieLogLayerBudgetExceededException extends RuntimeException {

  private final long plannedOps;
  private final long maxLayersToLoad;

  public TrieLogLayerBudgetExceededException(final long plannedOps, final long maxLayersToLoad) {
    super(
        "Planned trie-log roll ("
            + plannedOps
            + " ops) exceeds max layers to load ("
            + maxLayersToLoad
            + ")");
    this.plannedOps = plannedOps;
    this.maxLayersToLoad = maxLayersToLoad;
  }

  public long getPlannedOps() {
    return plannedOps;
  }

  public long getMaxLayersToLoad() {
    return maxLayersToLoad;
  }
}
