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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategyProvider.FLAT_DB_MODE;

import org.hyperledger.besu.datatypes.Hash;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes;

/** Helpers for classifying Bonsai trie keys across RocksDB column families. */
public final class BonsaiTrieStorageKeys {

  public static final byte[] STORAGE_TRIE_CF_MIGRATED_KEY =
      "storageTrieCfMigrated".getBytes(StandardCharsets.UTF_8);

  private static final int ACCOUNT_HASH_LENGTH_BYTES = 32;
  private static final int MAX_ACCOUNT_TRIE_LOCATION_BYTES = 64;

  private BonsaiTrieStorageKeys() {}

  public static byte[] storageTrieKey(final Hash accountHash, final Bytes location) {
    return Bytes.concatenate(accountHash.getBytes(), location).toArrayUnsafe();
  }

  public static boolean isMetadataKey(final byte[] key) {
    return Arrays.equals(key, WORLD_ROOT_HASH_KEY)
        || Arrays.equals(key, WORLD_BLOCK_HASH_KEY)
        || Arrays.equals(key, WORLD_BLOCK_NUMBER_KEY)
        || Arrays.equals(key, FLAT_DB_MODE)
        || Arrays.equals(key, STORAGE_TRIE_CF_MIGRATED_KEY);
  }

  public static boolean isAccountTrieLocationKey(final byte[] key) {
    return key.length <= MAX_ACCOUNT_TRIE_LOCATION_BYTES && allBytesAreNibbles(key);
  }

  public static boolean isStorageTrieKey(final byte[] key) {
    if (key.length < ACCOUNT_HASH_LENGTH_BYTES || isMetadataKey(key)) {
      return false;
    }
    if (isAccountTrieLocationKey(key)) {
      return false;
    }
    return allBytesAreNibbles(key, ACCOUNT_HASH_LENGTH_BYTES, key.length);
  }

  public static boolean allBytesAreNibbles(final byte[] key) {
    return allBytesAreNibbles(key, 0, key.length);
  }

  private static boolean allBytesAreNibbles(final byte[] key, final int from, final int to) {
    for (int i = from; i < to; i++) {
      if ((key[i] & 0xFF) > 0x0F) {
        return false;
      }
    }
    return true;
  }
}
