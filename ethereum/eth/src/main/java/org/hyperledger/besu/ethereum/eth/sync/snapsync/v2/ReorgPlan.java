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
     * Persisted accounts whose canonical record must be re-fetched via GetAccountRange: either a
     * scalar field (balance, nonce, code) changed only on the orphaned fork, or the storage root
     * cannot be recomputed locally because the account is pending and its storage was touched on
     * either fork. If the account is absent at the new pivot, it is deleted.
     */
    Set<Hash> accountsToRefetch,
    /**
     * Per-account slot hashes touched on the orphaned fork but absent from the canonical BALs,
     * scoped to downloaded slots (per-slot for pending accounts; all slots for completed accounts).
     */
    Map<Hash, Set<Hash>> slotsToRefetch) {

  /** The first canonical block to apply BALs for (inclusive). */
  public long fromBlock() {
    return commonAncestor.getNumber() + 1;
  }

  /** The last canonical block to apply BALs for (inclusive), i.e. the new pivot. */
  public long toBlock() {
    return newPivot.getNumber();
  }

  /** Returns true if no account or slot needs re-fetching. */
  public boolean isClean() {
    return accountsToRefetch.isEmpty() && slotsToRefetch.values().stream().allMatch(Set::isEmpty);
  }

  /** Returns an unmodifiable view of the slot hashes to re-fetch for the given account. */
  public Set<Hash> slotsToRefetchFor(final Hash accountHash) {
    return Collections.unmodifiableSet(slotsToRefetch.getOrDefault(accountHash, Set.of()));
  }
}
