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
package org.hyperledger.besu.ethereum.trie.pathbased.common.trielog;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.StoredWithBlock;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

/** The trie log of a processed block, stored together with the block instead of ahead of it. */
public class PendingTrieLog implements StoredWithBlock {

  private final TrieLogManager trieLogManager;
  private final KeyValueStorage trieLogStorage;
  private final Hash blockHash;
  private final TrieLog trieLog;
  private final byte[] serializedTrieLog;

  PendingTrieLog(
      final TrieLogManager trieLogManager,
      final KeyValueStorage trieLogStorage,
      final Hash blockHash,
      final TrieLog trieLog,
      final byte[] serializedTrieLog) {
    this.trieLogManager = trieLogManager;
    this.trieLogStorage = trieLogStorage;
    this.blockHash = blockHash;
    this.trieLog = trieLog;
    this.serializedTrieLog = serializedTrieLog;
  }

  public Hash getBlockHash() {
    return blockHash;
  }

  @Override
  public void writeWith(final BlockchainStorage.Updater blockUpdater) {
    final byte[] key = blockHash.getBytes().toArrayUnsafe();
    blockUpdater
        .transactionFor(trieLogStorage)
        .ifPresentOrElse(
            transaction -> transaction.put(key, serializedTrieLog),
            () -> {
              // without a shared transaction the trie log must still be stored ahead of the block
              final KeyValueStorageTransaction transaction = trieLogStorage.startTransaction();
              transaction.put(key, serializedTrieLog);
              transaction.commit();
            });
  }

  @Override
  public void onCommitted() {
    trieLogManager.notifyTrieLogAdded(trieLog);
  }
}
