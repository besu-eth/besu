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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Forest MPT-backed harness. Builds consistent account/storage tries and tracks the account root.
 */
final class ForestWorldStateStorageHarness implements WorldStateStorageHarness {

  private final WorldStateStorageCoordinator coordinator =
      new WorldStateStorageCoordinator(
          new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage()));

  @Override
  public WorldStateStorageCoordinator coordinator() {
    return coordinator;
  }

  @Override
  public boolean isForest() {
    return true;
  }

  @Override
  public ForestWorldStateKeyValueStorage forestStorage() {
    return coordinator.getStrategy(ForestWorldStateKeyValueStorage.class);
  }

  @Override
  public Bytes32 commitAndGetAccountRoot() {
    return forestStorage().getWorldStateRoot().orElse(MerkleTrie.EMPTY_TRIE_NODE_HASH);
  }

  private MerkleTrie<Bytes, Bytes> accountTrie() {
    final NodeLoader loader =
        (location, hash) -> coordinator.getAccountStateTrieNode(location, hash);
    final Bytes32 root =
        forestStorage().getWorldStateRoot().orElse(MerkleTrie.EMPTY_TRIE_NODE_HASH);
    return new StoredMerklePatriciaTrie<>(loader, root, Function.identity(), Function.identity());
  }

  private MerkleTrie<Bytes, Bytes> storageTrie(final Address address, final Hash storageRoot) {
    final NodeLoader loader =
        (location, hash) ->
            coordinator.getAccountStorageTrieNode(address.addressHash(), location, hash);
    return new StoredMerklePatriciaTrie<>(
        loader, Bytes32.wrap(storageRoot.getBytes()), Function.identity(), Function.identity());
  }

  private void writeAccount(final Address address, final PmtStateTrieAccountValue account) {
    final MerkleTrie<Bytes, Bytes> trie = accountTrie();
    trie.put(address.addressHash().getBytes(), RLP.encode(account::writeTo));
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    final NodeUpdater nodeUpdater =
        (location, hash, value) ->
            WorldStateStorageCoordinator.applyForStrategy(
                updater,
                onBonsai -> {
                  throw new IllegalStateException(
                      "ForestWorldStateStorageHarness used with Bonsai");
                },
                onForest -> onForest.putAccountStateTrieNode(hash, value));
    trie.commit(nodeUpdater);
    ((ForestWorldStateKeyValueStorage.Updater) updater)
        .putWorldStateRoot(Bytes32.wrap(trie.getRootHash()));
    updater.commit();
  }

  @Override
  public void seedAccount(
      final Address address,
      final long nonce,
      final Wei balance,
      final Hash storageRoot,
      final Hash codeHash) {
    writeAccount(address, new PmtStateTrieAccountValue(nonce, balance, storageRoot, codeHash));
  }

  @Override
  public void seedStorageSlot(final Address address, final UInt256 slotKey, final UInt256 value) {
    final PmtStateTrieAccountValue account =
        readAccount(address).orElseThrow(() -> new IllegalStateException("seed account first"));
    final MerkleTrie<Bytes, Bytes> trie = storageTrie(address, account.getStorageRoot());
    trie.put(
        Hash.hash(slotKey.toBytes()).getBytes(),
        RLP.encode(out -> out.writeBytes(value.toMinimalBytes())));
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    final NodeUpdater nodeUpdater =
        (location, hash, val) ->
            WorldStateStorageCoordinator.applyForStrategy(
                updater,
                onBonsai -> {
                  throw new IllegalStateException(
                      "ForestWorldStateStorageHarness used with Bonsai");
                },
                onForest -> onForest.putAccountStorageTrieNode(hash, val));
    trie.commit(nodeUpdater);
    updater.commit();
    writeAccount(
        address,
        new PmtStateTrieAccountValue(
            account.getNonce(),
            account.getBalance(),
            Hash.wrap(trie.getRootHash()),
            account.getCodeHash()));
  }

  @Override
  public void seedCode(final Address address, final Bytes code) {
    final Hash codeHash = Hash.hash(code);
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    WorldStateStorageCoordinator.applyForStrategy(
        updater,
        onBonsai -> {
          throw new IllegalStateException("ForestWorldStateStorageHarness used with Bonsai");
        },
        onForest -> onForest.putCode(Bytes32.wrap(codeHash.getBytes()), code));
    updater.commit();
    final PmtStateTrieAccountValue account =
        readAccount(address).orElseThrow(() -> new IllegalStateException("seed account first"));
    writeAccount(
        address,
        new PmtStateTrieAccountValue(
            account.getNonce(), account.getBalance(), account.getStorageRoot(), codeHash));
  }

  @Override
  public Optional<PmtStateTrieAccountValue> readAccount(final Address address) {
    return accountTrie()
        .get(address.addressHash().getBytes())
        .map(b -> PmtStateTrieAccountValue.readFrom(RLP.input(b)));
  }

  @Override
  public Optional<UInt256> readStorageSlot(final Address address, final UInt256 slotKey) {
    return readAccount(address)
        .flatMap(
            a ->
                storageTrie(address, a.getStorageRoot())
                    .get(Hash.hash(slotKey.toBytes()).getBytes()))
        .map(b -> UInt256.fromBytes(RLP.input(b).readBytes()));
  }

  @Override
  public Optional<Bytes> readCode(final Address address) {
    return readAccount(address)
        .flatMap(
            a ->
                coordinator.applyForStrategy(
                    onBonsai -> {
                      throw new IllegalStateException(
                          "ForestWorldStateStorageHarness used with Bonsai");
                    },
                    onForest -> onForest.getCode(a.getCodeHash())));
  }

  @Override
  public String label() {
    return "FOREST";
  }

  @Override
  public String toString() {
    return label();
  }
}
