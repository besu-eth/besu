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
package org.hyperledger.besu.ethereum.proof;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WorldStateProofProviderTest {

  private static final Address address =
      Address.fromHexString("0x1234567890123456789012345678901234567890");

  private BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;
  private WorldStateProofProvider worldStateProofProvider;

  @BeforeEach
  public void setup() {
    worldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    worldStateProofProvider =
        new WorldStateProofProvider(new WorldStateStorageCoordinator(worldStateKeyValueStorage));
  }

  @Test
  public void getProofWhenWorldStateNotAvailable() {
    Optional<WorldStateProof> accountProof =
        worldStateProofProvider.getAccountProof(Hash.EMPTY, address, new ArrayList<>());

    assertThat(accountProof).isEmpty();
  }

  @Test
  public void getProofWhenWorldStateAvailable() {
    final Hash addressHash = address.addressHash();
    final MerkleTrie<Bytes32, Bytes> worldStateTrie = emptyWorldStateTrie();
    final MerkleTrie<Bytes32, Bytes> storageTrie = emptyStorageTrie(addressHash);

    final BonsaiWorldStateKeyValueStorage.Updater updater = worldStateKeyValueStorage.updater();

    // Add some storage values
    writeStorageValue(storageTrie, UInt256.ONE, UInt256.valueOf(2L));
    writeStorageValue(storageTrie, UInt256.valueOf(2L), UInt256.valueOf(4L));
    writeStorageValue(storageTrie, UInt256.valueOf(3L), UInt256.valueOf(6L));
    // Save to storage
    storageTrie.commit(
        (location, hash, value) ->
            updater.putAccountStorageTrieNode(addressHash, location, hash, value));

    // Define account value
    final Hash codeHash = Hash.hash(Bytes.fromHexString("0x1122"));
    final PmtStateTrieAccountValue accountValue =
        new PmtStateTrieAccountValue(
            1L, Wei.of(2L), Hash.wrap(storageTrie.getRootHash()), codeHash);
    // Save to storage
    worldStateTrie.put(Bytes32.wrap(addressHash.getBytes()), RLP.encode(accountValue::writeTo));
    commitWorldStateTrie(worldStateTrie, updater);

    // Persist updates
    updater.commit();

    final List<UInt256> storageKeys =
        Arrays.asList(UInt256.ONE, UInt256.valueOf(3L), UInt256.valueOf(6L));
    final Optional<WorldStateProof> accountProof =
        worldStateProofProvider.getAccountProof(
            Hash.wrap(worldStateTrie.getRootHash()), address, storageKeys);

    assertThat(accountProof).isPresent();
    Assertions.assertThat(accountProof.get().getStateTrieAccountValue()).contains(accountValue);
    assertThat(accountProof.get().getAccountProof().size()).isGreaterThanOrEqualTo(1);
    // Check storage fields
    assertThat(accountProof.get().getStorageKeys()).isEqualTo(storageKeys);
    // Check key 1
    UInt256 storageKey = UInt256.ONE;
    assertThat(accountProof.get().getStorageValue(storageKey)).isEqualTo(UInt256.valueOf(2L));
    assertThat(accountProof.get().getStorageProof(storageKey).size()).isGreaterThanOrEqualTo(1);
    // Check key 3
    storageKey = UInt256.valueOf(3L);
    assertThat(accountProof.get().getStorageValue(storageKey)).isEqualTo(UInt256.valueOf(6L));
    assertThat(accountProof.get().getStorageProof(storageKey).size()).isGreaterThanOrEqualTo(1);
    // Check key 6
    storageKey = UInt256.valueOf(6L);
    assertThat(accountProof.get().getStorageValue(storageKey)).isEqualTo(UInt256.ZERO);
    assertThat(accountProof.get().getStorageProof(storageKey).size()).isGreaterThanOrEqualTo(1);
  }

  @Test
  public void getProofForNonExistentAccountReturnsStorageProofEntryPerRequestedKey() {
    final MerkleTrie<Bytes32, Bytes> worldStateTrie = emptyWorldStateTrie();

    final BonsaiWorldStateKeyValueStorage.Updater updater = worldStateKeyValueStorage.updater();

    // Add a different account so the world state is non-empty (state root is known)
    final Address otherAddress =
        Address.fromHexString("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    final PmtStateTrieAccountValue otherAccount =
        new PmtStateTrieAccountValue(0L, Wei.ZERO, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    worldStateTrie.put(
        Bytes32.wrap(otherAddress.addressHash().getBytes()), RLP.encode(otherAccount::writeTo));
    commitWorldStateTrie(worldStateTrie, updater);
    updater.commit();

    final List<UInt256> storageKeys =
        Arrays.asList(UInt256.ONE, UInt256.valueOf(2L), UInt256.valueOf(3L));
    final Optional<WorldStateProof> proof =
        worldStateProofProvider.getAccountProof(
            Hash.wrap(worldStateTrie.getRootHash()), address, storageKeys);

    assertThat(proof).isPresent();
    assertThat(proof.get().getStateTrieAccountValue()).isEmpty();
    // EIP-1186: must return one entry per requested key even for non-existent accounts
    assertThat(proof.get().getStorageKeys()).containsExactlyInAnyOrderElementsOf(storageKeys);
    // All values must be zero (account doesn't exist)
    storageKeys.forEach(
        key -> assertThat(proof.get().getStorageValue(key)).isEqualTo(UInt256.ZERO));
    // Proof nodes are empty (absent account has no storage)
    storageKeys.forEach(key -> assertThat(proof.get().getStorageProof(key)).isEmpty());
  }

  @Test
  public void getProofWhenStateTrieAccountUnavailable() {
    final Hash addressHash = address.addressHash();
    final MerkleTrie<Bytes32, Bytes> worldStateTrie = emptyWorldStateTrie();

    final BonsaiWorldStateKeyValueStorage.Updater updater = worldStateKeyValueStorage.updater();

    // Define account value
    final Hash codeHash = Hash.hash(Bytes.fromHexString("0x1122"));
    final PmtStateTrieAccountValue accountValue =
        new PmtStateTrieAccountValue(1L, Wei.of(2L), Hash.EMPTY_TRIE_HASH, codeHash);
    // Save to storage
    worldStateTrie.put(Bytes32.wrap(addressHash.getBytes()), RLP.encode(accountValue::writeTo));
    commitWorldStateTrie(worldStateTrie, updater);

    // Persist updates
    updater.commit();

    final Optional<WorldStateProof> accountProof =
        worldStateProofProvider.getAccountProof(
            Hash.wrap(worldStateTrie.getRootHash()),
            Address.ZERO,
            new ArrayList<>()); // missing address

    assertThat(accountProof).isPresent();
    assertThat(accountProof.get().getStateTrieAccountValue()).isEmpty();
    assertThat(accountProof.get().getAccountProof()).hasSize(1);
    assertThat(Hash.hash(accountProof.get().getAccountProof().getFirst()).getBytes())
        .isEqualTo(worldStateTrie.getRootHash());
  }

  private void commitWorldStateTrie(
      final MerkleTrie<Bytes32, Bytes> worldStateTrie,
      final BonsaiWorldStateKeyValueStorage.Updater updater) {
    final Bytes[] rootNodeValue = new Bytes[1];
    worldStateTrie.commit(
        (location, hash, value) -> {
          updater.putAccountStateTrieNode(location, hash, value);
          if (location.size() == 0) {
            rootNodeValue[0] = value;
          }
        });
    updater.saveWorldState(Bytes.EMPTY, worldStateTrie.getRootHash(), rootNodeValue[0]);
  }

  private void writeStorageValue(
      final MerkleTrie<Bytes32, Bytes> storageTrie, final UInt256 key, final UInt256 value) {
    storageTrie.put(storageKeyHash(key), encodeStorageValue(value));
  }

  private Bytes32 storageKeyHash(final UInt256 storageKey) {
    return Bytes32.wrap(Hash.hash(storageKey).getBytes());
  }

  private Bytes encodeStorageValue(final UInt256 storageValue) {
    return RLP.encode(out -> out.writeBytes(storageValue.toMinimalBytes()));
  }

  private MerkleTrie<Bytes32, Bytes> emptyStorageTrie(final Hash accountHash) {
    return new StoredMerklePatriciaTrie<>(
        (location, hash) ->
            worldStateKeyValueStorage.getAccountStorageTrieNode(accountHash, location, hash),
        b -> b,
        b -> b);
  }

  private MerkleTrie<Bytes32, Bytes> emptyWorldStateTrie() {
    return new StoredMerklePatriciaTrie<>(
        (location, hash) -> worldStateKeyValueStorage.getAccountStateTrieNode(location, hash),
        b -> b,
        b -> b);
  }
}
