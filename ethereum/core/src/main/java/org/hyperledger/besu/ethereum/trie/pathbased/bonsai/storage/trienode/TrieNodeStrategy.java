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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Defines the strategy for storing and retrieving account/storage trie branch nodes in key-value
 * storage. Implementations can use different key formats or storage segments; this is independent
 * of the flat-account/storage database strategies under {@code storage.flat}.
 */
public interface TrieNodeStrategy {

  Optional<Bytes> getFlatAccountTrieNode(
      Bytes location, Bytes32 nodeHash, SegmentedKeyValueStorage storage);

  Optional<Bytes> getFlatStorageTrieNode(
      Hash accountHash, Bytes location, Bytes32 nodeHash, SegmentedKeyValueStorage storage);

  void putFlatAccountTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Bytes location,
      Bytes32 nodeHash,
      Bytes node);

  void putFlatStorageTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Hash accountHash,
      Bytes location,
      Bytes32 nodeHash,
      Bytes node);

  void removeFlatAccountStateTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Bytes location);
}
