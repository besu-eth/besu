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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Strategy for reading and writing Merkle Patricia Trie nodes to flat storage. */
public interface TrieNodeStrategy {

  /**
   * {@code nodeHash} may be null: callers that don't know the hash at read time (e.g. an
   * archive-aware decorator's removal path, which reads the prior node before deleting it) pass
   * null. Implementations must tolerate a null {@code nodeHash} and must not throw a {@link
   * NullPointerException} because of it.
   */
  Optional<Bytes> getFlatAccountTrieNode(
      Bytes location, Bytes32 nodeHash, SegmentedKeyValueStorage storage);

  /**
   * {@code nodeHash} may be null: callers that don't know the hash at read time (e.g. an
   * archive-aware decorator's removal path, which reads the prior node before deleting it) pass
   * null. Implementations must tolerate a null {@code nodeHash} and must not throw a {@link
   * NullPointerException} because of it.
   */
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

  /**
   * Applies any capture work buffered by put/remove calls to the given transaction. Called by the
   * Updater on every path that commits the composed world-state transaction, immediately before
   * that commit. Implementations that buffer per-transaction state must ignore calls whose {@code
   * transaction} is not the one that filled the buffer — a single strategy instance is shared by
   * every Updater on the same storage, and unrelated updaters (e.g. the trie-log-only updater
   * inside {@code TrieLogManager.saveTrieLog}) commit mid-block. Default: no-op (non-archive
   * strategies buffer nothing).
   */
  default void flushCaptures(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {}

  /**
   * Drops any buffered capture work owned by the given transaction. Called by the Updater on
   * rollback and on commit paths that do not commit the composed world-state transaction. The same
   * ownership rule as {@link #flushCaptures} applies: calls from a transaction that did not fill
   * the buffer must be ignored. Default: no-op.
   */
  default void discardCaptures(final SegmentedKeyValueStorageTransaction transaction) {}
}
