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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogAccumulator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * The changes a trie log is built from, copied out of an accumulator so that the accumulator can be
 * reset before the trie log is built. A reset clears the accumulator's maps, but leaves the values
 * they hold untouched, the per-account storage maps included.
 *
 * @param accounts the updated accounts
 * @param code the updated code
 * @param storage the updated storage, per account
 */
record TrieLogChanges(
    Map<Address, ? extends TrieLog.LogTuple<? extends AccountValue>> accounts,
    Map<Address, ? extends TrieLog.LogTuple<Bytes>> code,
    Map<Address, ? extends Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>>> storage)
    implements TrieLogAccumulator {

  /**
   * Maps of at least this many entries are copied on several threads: the copy is made on the
   * thread that imports the block, before it replies, and a block of many transfers changes tens of
   * thousands of accounts.
   */
  private static final int MIN_ENTRIES_TO_COPY_IN_PARALLEL = 512;

  static TrieLogChanges copyOf(final TrieLogAccumulator accumulator) {
    return new TrieLogChanges(
        copy(accumulator.getAccountsToUpdate()),
        copy(accumulator.getCodeToUpdate()),
        copy(accumulator.getStorageToUpdate()));
  }

  private static <K, V> Map<K, V> copy(final Map<K, V> map) {
    if (map.size() < MIN_ENTRIES_TO_COPY_IN_PARALLEL) {
      return new HashMap<>(map);
    }
    final Map<K, V> copy = new ConcurrentHashMap<>(map.size());
    map.entrySet().parallelStream().forEach(entry -> copy.put(entry.getKey(), entry.getValue()));
    return copy;
  }

  @Override
  public Map<Address, ? extends TrieLog.LogTuple<? extends AccountValue>> getAccountsToUpdate() {
    return accounts;
  }

  @Override
  public Map<Address, ? extends TrieLog.LogTuple<Bytes>> getCodeToUpdate() {
    return code;
  }

  @Override
  public Map<Address, ? extends Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>>>
      getStorageToUpdate() {
    return storage;
  }
}
