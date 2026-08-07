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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.hyperledger.besu.crypto.Hash.keccak256;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Adapts {@link TrieNodeHistoryReader} to the {@link NodeLoader} interface used for proof trie
 * walks.
 */
public final class ArchiveProofNodeLoader {

  private final TrieNodeHistoryReader historyReader;
  private final SegmentedKeyValueStorage liveStorage;
  private final long targetBlock;

  public ArchiveProofNodeLoader(
      final TrieNodeHistoryReader historyReader,
      final SegmentedKeyValueStorage liveStorage,
      final long targetBlock) {
    this.historyReader = Objects.requireNonNull(historyReader);
    this.liveStorage = Objects.requireNonNull(liveStorage);
    this.targetBlock = targetBlock;
  }

  public NodeLoader accountNodeLoader() {
    return (location, expectedHash) ->
        resolveNodeAt(location, ArchiveNodeKey.account(location), expectedHash);
  }

  public NodeLoader storageNodeLoader(final Bytes32 accountHash) {
    return (location, expectedHash) ->
        resolveNodeAt(location, ArchiveNodeKey.storage(accountHash, location), expectedHash);
  }

  private Optional<Bytes> resolveNodeAt(
      final Bytes location, final Bytes naturalKey, final Bytes32 expectedHash) {
    final Optional<Bytes> liveNode =
        liveStorage.get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe()).map(Bytes::wrap);
    if (liveNode.isPresent() && keccak256(liveNode.get()).equals(expectedHash)) {
      return liveNode;
    }

    final Optional<Bytes> reconstructed = historyReader.nodeAt(naturalKey, targetBlock);
    if (reconstructed.isEmpty()) {
      return Optional.empty();
    }
    final Bytes32 actualHash = keccak256(reconstructed.get());
    if (!actualHash.equals(expectedHash)) {
      throw new IllegalStateException(
          "trie node hash mismatch for naturalKey="
              + naturalKey
              + " at targetBlock="
              + targetBlock
              + ": expected="
              + expectedHash
              + ", actual="
              + actualHash);
    }
    return reconstructed;
  }
}
