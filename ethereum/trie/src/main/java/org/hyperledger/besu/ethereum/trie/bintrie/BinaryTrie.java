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
package org.hyperledger.besu.ethereum.trie.bintrie;

import org.hyperledger.besu.ethereum.stateless.bintrie.BytesBitSequence;
import org.hyperledger.besu.ethereum.stateless.bintrie.BytesBitSequenceFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.StoredBinTrie;
import org.hyperledger.besu.ethereum.stateless.bintrie.factory.StoredNodeFactory;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class BinaryTrie {

  private final StoredBinTrie<BytesBitSequence, Bytes> binTrie;

  public BinaryTrie(final NodeLoader nodeLoader) {
    final StoredNodeFactory<BytesBitSequence, Bytes> nodeFactory =
        new StoredNodeFactory<>(nodeLoader, new BytesBitSequenceFactory(), value -> value);
    binTrie = new StoredBinTrie<>(nodeFactory);
  }

  public Optional<Bytes> get(final Bytes key) {
    return binTrie.get(BytesBitSequence.fromBytes(key));
  }

  public Optional<Bytes> put(final BytesBitSequence key, final Bytes value) {
    return binTrie.put(key, Bytes32.leftPad(value));
  }

  public void remove(final BytesBitSequence key) {
    binTrie.remove(key);
  }

  public Bytes32 getRootHash() {
    return binTrie.getRootHash();
  }

  public void commit(final NodeUpdater nodeUpdater) {
    binTrie.commit(nodeUpdater);
  }

  public String toDotTree() {
    return binTrie.toDotTree(false);
  }
}
