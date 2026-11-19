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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import java.util.function.BooleanSupplier;

/** Archives trie-node history for a range of blocks that inline capture missed. */
public interface TrieNodeArchiveStep {

  /** Archives blocks after {@code fromExclusive} through {@code toInclusive}, one at a time. */
  void archiveRange(long fromExclusive, long toInclusive, BooleanSupplier keepGoing);

  /** Thrown when a block cannot be archived without leaving a gap in coverage. */
  final class ArchiveGapException extends IllegalStateException {
    public ArchiveGapException(final String reason) {
      super(
          "Trie-node archiver stopping: "
              + reason
              + "; archive coverage stays where it is until the archive is resynced");
    }
  }
}
