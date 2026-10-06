/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.proof;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.Proof;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class WorldStateProof {

  private final Optional<PmtStateTrieAccountValue> stateTrieAccountValue;

  private final Proof<Bytes> accountProof;

  private final Map<UInt256, Proof<Bytes>> storageProofs;

  public WorldStateProof(
      final PmtStateTrieAccountValue stateTrieAccountValue,
      final Proof<Bytes> accountProof,
      final SortedMap<UInt256, Proof<Bytes>> storageProofs) {
    this.stateTrieAccountValue = Optional.ofNullable(stateTrieAccountValue);
    this.accountProof = accountProof;
    this.storageProofs = storageProofs;
  }

  public WorldStateProof(final Proof<Bytes> accountProof) {
    this(accountProof, new HashMap<>());
  }

  public WorldStateProof(
      final Proof<Bytes> accountProof, final Map<UInt256, Proof<Bytes>> storageProofs) {
    this.stateTrieAccountValue = Optional.empty();
    this.accountProof = accountProof;
    this.storageProofs = storageProofs;
  }

  public Optional<PmtStateTrieAccountValue> getStateTrieAccountValue() {
    return stateTrieAccountValue;
  }

  public List<Bytes> getAccountProof() {
    return accountProof.getProofRelatedNodes();
  }

  public List<UInt256> getStorageKeys() {
    return new ArrayList<>(storageProofs.keySet());
  }

  public UInt256 getStorageValue(final UInt256 key) {
    Optional<Bytes> value = storageProofs.get(key).getValue();
    if (value.isEmpty()) {
      return UInt256.ZERO;
    } else {
      return RLP.input(value.get()).readUInt256Scalar();
    }
  }

  public List<Bytes> getStorageProof(final UInt256 key) {
    return storageProofs.get(key).getProofRelatedNodes();
  }
}
