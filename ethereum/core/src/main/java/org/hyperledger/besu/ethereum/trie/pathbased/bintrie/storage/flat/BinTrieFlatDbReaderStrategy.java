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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.flat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** Common interface for BinTrie flat database read strategies. */
public interface BinTrieFlatDbReaderStrategy {

  /**
   * Retrieves an account from the flat database.
   *
   * @param address the account address
   * @param context the world view context
   * @param storage the key-value storage
   * @return the account if found
   */
  Optional<BinTrieAccount> getFlatAccount(
      Address address, PathBasedWorldView context, SegmentedKeyValueStorage storage);

  /**
   * Retrieves a storage value from the flat database.
   *
   * @param address the account address
   * @param storageSlotKey the storage slot key
   * @param storage the key-value storage
   * @return the storage value if found
   */
  Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      Address address, StorageSlotKey storageSlotKey, SegmentedKeyValueStorage storage);
}
