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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public record ReorgPlan(
    BlockHeader commonAncestor,
    BlockHeader oldPivot,
    BlockHeader newPivot,
    /**
     * Account hashes touched on the orphaned fork but absent from the canonical BALs, scoped to
     * persisted account ranges.
     */
    Set<Hash> divergedAccounts,
    /**
     * Per-account slot hashes touched on the orphaned fork but absent from the canonical BALs,
     * scoped to persisted slots (per-slot for pending accounts; all slots for completed accounts).
     */
    Map<Hash, Set<Hash>> divergedSlotsByAccount) {

  /** The first canonical block to apply BALs for (inclusive). */
  public long fromBlock() {
    return commonAncestor.getNumber() + 1;
  }

  /** The last canonical block to apply BALs for (inclusive), i.e. the new pivot. */
  public long toBlock() {
    return newPivot.getNumber();
  }

  /** Returns true if no divergence was detected (no re-fetch needed). */
  public boolean isClean() {
    return divergedAccounts.isEmpty()
        && divergedSlotsByAccount.values().stream().allMatch(Set::isEmpty);
  }

  /** Returns an unmodifiable view of the diverged slot hashes for the given account. */
  public Set<Hash> divergedSlotsFor(final Hash accountHash) {
    return Collections.unmodifiableSet(divergedSlotsByAccount.getOrDefault(accountHash, Set.of()));
  }
}
