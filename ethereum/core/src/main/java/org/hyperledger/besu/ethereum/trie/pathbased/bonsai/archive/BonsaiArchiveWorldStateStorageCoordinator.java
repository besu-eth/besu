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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveProofNodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link WorldStateStorageCoordinator} that routes account-trie and storage-trie node reads
 * through {@link ArchiveProofNodeLoader}. The coverage check is done before instantiation so {@code
 * isWorldStateAvailable} always returns {@code true} here.
 */
public final class BonsaiArchiveWorldStateStorageCoordinator extends WorldStateStorageCoordinator {

  private final ArchiveHistoryReader historyReader;
  private final long targetBlock;

  public BonsaiArchiveWorldStateStorageCoordinator(
      final BonsaiWorldStateKeyValueStorage keyValueStorage,
      final ArchiveHistoryReader historyReader,
      final long targetBlock) {
    super(keyValueStorage);
    this.historyReader = historyReader;
    this.targetBlock = targetBlock;
  }

  @Override
  public boolean isWorldStateAvailable(final Bytes32 nodeHash, final Hash blockHash) {
    return true; // coverage pre-checked in getAccountProof before instantiation
  }

  @Override
  public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    return ArchiveProofNodeLoader.forAccount(historyReader, targetBlock)
        .getNode(location, nodeHash);
  }

  @Override
  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    return ArchiveProofNodeLoader.forStorage(accountHash, historyReader, targetBlock)
        .getNode(location, nodeHash);
  }
}
