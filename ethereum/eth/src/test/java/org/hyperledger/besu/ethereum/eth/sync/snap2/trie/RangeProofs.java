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

import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Range proofs of test tries, as a peer sends them. */
public final class RangeProofs {

  private RangeProofs() {}

  /** Returns the proof of a range: the nodes on the paths to its origin and to its last key. */
  public static List<Bytes> proof(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie,
      final Bytes32 origin,
      final Bytes32 last) {
    final Set<Bytes> nodes = new LinkedHashSet<>();
    nodes.addAll(trie.getValueWithProof(origin).getProofRelatedNodes());
    nodes.addAll(trie.getValueWithProof(last).getProofRelatedNodes());
    return new ArrayList<>(nodes);
  }
}
