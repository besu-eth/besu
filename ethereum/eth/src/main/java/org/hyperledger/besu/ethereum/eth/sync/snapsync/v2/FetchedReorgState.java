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
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;

import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Canonical state at the new pivot, fetched from peers and proof-verified by {@link
 * SnapV2ReorgStateFetcher}, covering exactly the entries {@link ReorgPlan} flagged as changed only
 * on the orphaned fork.
 *
 * @param accounts flat accounts per requested account hash; empty means the account does not exist
 *     at the new pivot and must be deleted locally
 * @param slotsByAccount slot values per account, per requested slot hash; empty means the slot does
 *     not exist at the new pivot and must be removed locally
 * @param codeByHash canonical code, fetched for restored accounts whose code was never downloaded
 */
public record FetchedReorgState(
    Map<Hash, Optional<PmtStateTrieAccountValue>> accounts,
    Map<Hash, Map<Hash, Optional<UInt256>>> slotsByAccount,
    Map<Hash, Bytes> codeByHash) {

  public static FetchedReorgState empty() {
    return new FetchedReorgState(Map.of(), Map.of(), Map.of());
  }
}
