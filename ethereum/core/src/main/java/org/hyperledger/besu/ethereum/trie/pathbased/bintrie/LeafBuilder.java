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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.stateless.bintrie.BytesBitSequence;
import org.hyperledger.besu.ethereum.stateless.bintrie.adapter.TrieKeyFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.adapter.TrieKeyUtils;
import org.hyperledger.besu.ethereum.stateless.bintrie.util.SuffixTreeEncoder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Builds trie keys and values for account, code, and storage updates in the Binary Trie.
 *
 * <p>This class accumulates changes to be applied to the trie, separating removals from updates,
 * and storage updates from non-storage updates (accounts, code).
 */
public class LeafBuilder {

  private final TrieKeyFactory trieKeyFactory;
  private final Set<BytesBitSequence> keysForRemoval = new HashSet<>();
  private final Map<BytesBitSequence, Bytes32> nonStorageUpdates = new HashMap<>();
  private final Map<StorageSlotKey, Pair<BytesBitSequence, Bytes32>> storageUpdates =
      new HashMap<>();

  public LeafBuilder(final TrieKeyFactory trieKeyFactory) {
    this.trieKeyFactory = trieKeyFactory;
  }

  public void addAccountRemoval(final Address address) {
    keysForRemoval.add(trieKeyFactory.basicDataKey(address.getBytes()));
  }

  public void addCodeHashRemoval(final Address address) {
    keysForRemoval.add(trieKeyFactory.codeHashKey(address.getBytes()));
  }

  public void addCodeRemoval(final Address address, final Bytes code) {
    addCodeHashRemoval(address);
    final Bytes addressBytes = address.getBytes();
    final List<UInt256> codeChunks = TrieKeyUtils.chunkifyCode(code);
    for (int i = 0; i < codeChunks.size(); i++) {
      keysForRemoval.add(trieKeyFactory.codeChunkKey(addressBytes, UInt256.valueOf(i)));
    }
  }

  public void addStorageRemoval(final Address address, final StorageSlotKey storageKey) {
    keysForRemoval.add(
        trieKeyFactory.storageKey(address.getBytes(), storageKey.getSlotKey().orElseThrow()));
  }

  /**
   * Adds account data (version, nonce, balance) incrementally to basicData.
   *
   * @param address the account address
   * @param nonce the account nonce
   * @param balance the account balance
   */
  public void addAccountUpdate(final Address address, final long nonce, final Wei balance) {
    final BytesBitSequence basicDataKey = trieKeyFactory.basicDataKey(address.getBytes());
    Bytes32 basicDataValue = nonStorageUpdates.getOrDefault(basicDataKey, Bytes32.ZERO);

    basicDataValue = SuffixTreeEncoder.setVersionInValue(basicDataValue, Bytes.of((byte) 0));
    basicDataValue = SuffixTreeEncoder.setNonceInValue(basicDataValue, Bytes.ofUnsignedLong(nonce));
    basicDataValue = SuffixTreeEncoder.setBalanceInValue(basicDataValue, balance.slice(16));

    nonStorageUpdates.put(basicDataKey, basicDataValue);
  }

  /**
   * Adds code size incrementally to basicData.
   *
   * @param address the account address
   * @param size the code size
   */
  public void addCodeSizeUpdate(final Address address, final long size) {
    final BytesBitSequence basicDataKey = trieKeyFactory.basicDataKey(address.getBytes());
    Bytes32 basicDataValue = nonStorageUpdates.getOrDefault(basicDataKey, Bytes32.ZERO);

    basicDataValue =
        SuffixTreeEncoder.setCodeSizeInValue(basicDataValue, Bytes.ofUnsignedInt(size).slice(1));

    nonStorageUpdates.put(basicDataKey, basicDataValue);
  }

  public void addCodeHashUpdate(final Address address, final Hash codeHash) {
    nonStorageUpdates.put(
        trieKeyFactory.codeHashKey(address.getBytes()), Bytes32.wrap(codeHash.getBytes()));
  }

  /**
   * Adds code update including code size, code hash, and code chunks.
   *
   * @param address the account address
   * @param code the code bytes
   * @param codeHash the code hash
   */
  public void addCodeUpdate(final Address address, final Bytes code, final Hash codeHash) {
    addCodeSizeUpdate(address, code.size());
    addCodeHashUpdate(address, codeHash);

    final Bytes addressBytes = address.getBytes();
    final List<UInt256> codeChunks = TrieKeyUtils.chunkifyCode(code);
    for (int i = 0; i < codeChunks.size(); i++) {
      nonStorageUpdates.put(
          trieKeyFactory.codeChunkKey(addressBytes, UInt256.valueOf(i)), codeChunks.get(i));
    }
  }

  public void addStorageUpdate(
      final Address address, final StorageSlotKey storageSlotKey, final Bytes32 value) {
    storageUpdates.put(
        storageSlotKey,
        new Pair<>(
            trieKeyFactory.storageKey(
                address.getBytes(), storageSlotKey.getSlotKey().orElseThrow()),
            value));
  }

  public Set<BytesBitSequence> getKeysForRemoval() {
    return keysForRemoval;
  }

  public Map<BytesBitSequence, Bytes32> getNonStorageUpdates() {
    return nonStorageUpdates;
  }

  public Map<StorageSlotKey, Pair<BytesBitSequence, Bytes32>> getStorageUpdates() {
    return storageUpdates;
  }
}
