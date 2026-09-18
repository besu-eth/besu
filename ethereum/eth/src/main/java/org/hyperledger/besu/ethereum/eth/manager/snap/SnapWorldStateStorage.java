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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.function.Predicate;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** World state storage abstraction for snap server account/storage/trie-node serving. */
interface SnapWorldStateStorage {

  NavigableMap<Bytes32, Bytes> streamFlatAccounts(
      Bytes startKeyHash, Predicate<Pair<Bytes32, Bytes>> takeWhile);

  NavigableMap<Bytes32, Bytes> streamFlatAccounts(Bytes startKeyHash, Bytes32 endKeyHash, long max);

  NavigableMap<Bytes32, Bytes> streamFlatStorages(
      Hash accountHash, Bytes startKeyHash, Predicate<Pair<Bytes32, Bytes>> takeWhile);

  NavigableMap<Bytes32, Bytes> streamFlatStorages(
      Hash accountHash, Bytes startKeyHash, Bytes32 endKeyHash, long max);

  Optional<Bytes> getTrieNodeUnsafe(Bytes location);

  Optional<Bytes> getAccount(Hash accountHash);

  Hash getAccountStorageRoot(Hash accountHash);

  WorldStateKeyValueStorage asWorldStateKeyValueStorage();
}
