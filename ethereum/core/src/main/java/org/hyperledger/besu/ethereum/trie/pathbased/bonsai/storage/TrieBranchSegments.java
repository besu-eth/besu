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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

/** The storage segment holding each trie branch. */
public final class TrieBranchSegments {

  private TrieBranchSegments() {}

  public static SegmentIdentifier segmentFor(final TrieBranchType trieBranchType) {
    return switch (trieBranchType) {
      case PATRICIA -> TRIE_BRANCH_STORAGE;
      case BINARY ->
          throw new UnsupportedOperationException("The binary trie branch has no storage yet");
    };
  }
}
