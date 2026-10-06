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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveReadTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link WorldStateStorageCoordinator} that routes all trie-node reads through {@link
 * ArchiveReadTrieNodeStrategy} for historical proof requests. The coverage check is done before
 * instantiation so {@code isWorldStateAvailable} always returns {@code true} here.
 */
public final class BonsaiArchiveReadWorldStateStorageCoordinator
    extends WorldStateStorageCoordinator {

  public BonsaiArchiveReadWorldStateStorageCoordinator(
      final BonsaiWorldStateKeyValueStorage keyValueStorage,
      final ArchiveHistoryReader historyReader,
      final long targetBlock) {
    super(
        keyValueStorage.withTrieNodeStrategy(
            new ArchiveReadTrieNodeStrategy(targetBlock, historyReader)));
  }

  @Override
  public boolean isWorldStateAvailable(final Bytes32 nodeHash, final Hash blockHash) {
    return true; // coverage pre-checked in getAccountProof before instantiation
  }
}
