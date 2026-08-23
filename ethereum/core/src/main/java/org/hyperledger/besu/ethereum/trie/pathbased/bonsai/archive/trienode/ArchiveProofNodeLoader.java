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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link NodeLoader} that resolves trie nodes from the archive column family for historical
 * proofs.
 */
public final class ArchiveProofNodeLoader implements NodeLoader {

  private final Function<Bytes, Bytes> naturalKeyFn;
  private final ArchiveHistoryReader historyReader;
  private final long targetBlock;

  private ArchiveProofNodeLoader(
      final Function<Bytes, Bytes> naturalKeyFn,
      final ArchiveHistoryReader historyReader,
      final long targetBlock) {
    this.naturalKeyFn = naturalKeyFn;
    this.historyReader = historyReader;
    this.targetBlock = targetBlock;
  }

  /**
   * Creates a node loader for the account state trie.
   *
   * @param historyReader archive reader providing historical node versions
   * @param targetBlock proof target block number (inclusive)
   * @return a {@link NodeLoader} for account-trie nodes
   */
  public static NodeLoader forAccount(
      final ArchiveHistoryReader historyReader, final long targetBlock) {
    return new ArchiveProofNodeLoader(ArchiveNodeKey::account, historyReader, targetBlock);
  }

  /**
   * Creates a node loader for a specific account's storage trie.
   *
   * @param accountHash the account whose storage trie we are proving
   * @param historyReader archive reader providing historical node versions
   * @param targetBlock proof target block number (inclusive)
   * @return a {@link NodeLoader} for storage-trie nodes of the given account
   */
  public static NodeLoader forStorage(
      final Hash accountHash, final ArchiveHistoryReader historyReader, final long targetBlock) {
    final Bytes accountHashBytes = accountHash.getBytes();
    return new ArchiveProofNodeLoader(
        location -> ArchiveNodeKey.storage(accountHashBytes, location), historyReader, targetBlock);
  }

  @Override
  public Optional<Bytes> getNode(final Bytes location, final Bytes32 hash) {
    if (hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    return historyReader
        .nodeAt(naturalKeyFn.apply(location), targetBlock)
        .filter(node -> hashMatches(node, hash));
  }

  private static boolean hashMatches(final Bytes node, final Bytes32 expected) {
    return Hash.hash(node).getBytes().equals(expected);
  }
}
