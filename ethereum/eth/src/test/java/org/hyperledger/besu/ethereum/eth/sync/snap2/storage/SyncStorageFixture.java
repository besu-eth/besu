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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/** Random world states and in-memory sync storages for tests. */
public final class SyncStorageFixture {

  private SyncStorageFixture() {}

  /** Storage format under test. */
  public enum Format {
    BONSAI
  }

  /** A sync storage with direct access to its backing world state storage. */
  public record Storages(SyncStateStorage sync, BonsaiWorldStateKeyValueStorage bonsai) {

    /** Opens the stored account trie. */
    public MerkleTrie<Bytes32, Bytes> accountTrie(final Bytes32 root) {
      return new StoredMerklePatriciaTrie<>(
          bonsai::getTrieNode, root, Function.identity(), Function.identity());
    }

    /** Opens a stored storage trie. */
    public MerkleTrie<Bytes32, Bytes> storageTrie(final Bytes32 accountHash, final Bytes32 root) {
      return new StoredMerklePatriciaTrie<>(
          (location, hash) ->
              bonsai.getTrieNode(TrieNodeKey.of(Hash.wrap(accountHash), location), hash),
          root,
          Function.identity(),
          Function.identity());
    }
  }

  public static Storages create(final Format format) {
    final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
    return switch (format) {
      case BONSAI -> {
        final BonsaiWorldStateKeyValueStorage bonsai =
            (BonsaiWorldStateKeyValueStorage)
                provider.createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
        yield new Storages(new BonsaiSyncStateStorage(bonsai), bonsai);
      }
    };
  }

  /** Returns the slots stored for an account, keyed by slot hash, by a scan of its key alone. */
  public static NavigableMap<Bytes32, Bytes> slots(
      final SyncStateStorage storage, final Bytes32 accountHash) {
    final NavigableMap<Bytes32, Bytes> slots = new TreeMap<>();
    storage.scan(
        accountHash,
        accountHash,
        new SyncStateStorage.ScanVisitor() {
          @Override
          public boolean onAccount(final Bytes32 hash, final Bytes account) {
            return true;
          }

          @Override
          public void onSlot(final Bytes32 hash, final Bytes32 slotHash, final Bytes value) {
            slots.put(slotHash, value);
          }
        });
    return slots;
  }

  /** A flat world state keyed by hash, as the download writes it. */
  public static final class World {
    public final TreeMap<Bytes32, PatriciaTrieAccountValue> accounts = new TreeMap<>();
    public final TreeMap<Bytes32, TreeMap<Bytes32, UInt256>> storage = new TreeMap<>();
    public final Map<Bytes32, Bytes> codes = new TreeMap<>();

    /** Returns the storage root of an account, computed from its slots. */
    public Bytes32 storageRoot(final Bytes32 accountHash) {
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie =
          new SimpleMerklePatriciaTrie<>(Function.identity());
      storage
          .getOrDefault(accountHash, new TreeMap<>())
          .forEach((slot, value) -> trie.put(slot, encodeSlot(value)));
      return trie.getRootHash();
    }

    /** Returns the account with its storage root recomputed. */
    public PatriciaTrieAccountValue account(final Bytes32 accountHash) {
      final PatriciaTrieAccountValue account = accounts.get(accountHash);
      return new PatriciaTrieAccountValue(
          account.getNonce(),
          account.getBalance(),
          Hash.wrap(storageRoot(accountHash)),
          account.getCodeHash());
    }

    /** Returns the state root. */
    public Bytes32 root() {
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie =
          new SimpleMerklePatriciaTrie<>(Function.identity());
      accountLeaves().forEach(trie::put);
      return trie.getRootHash();
    }

    /** Returns the leaf values of the account trie. */
    public NavigableMap<Bytes32, Bytes> accountLeaves() {
      final TreeMap<Bytes32, Bytes> out = new TreeMap<>();
      accounts.keySet().forEach(hash -> out.put(hash, Accounts.encode(account(hash))));
      return out;
    }

    /** Returns the leaf values of a storage trie. */
    public NavigableMap<Bytes32, Bytes> storageLeaves(final Bytes32 accountHash) {
      final TreeMap<Bytes32, Bytes> out = new TreeMap<>();
      storage
          .getOrDefault(accountHash, new TreeMap<>())
          .forEach((slot, value) -> out.put(slot, encodeSlot(value)));
      return out;
    }

    /** Writes the flat state into a sync storage, with stale storage roots. */
    public void writeFlat(final SyncStateStorage sync) {
      final SyncStateStorage.Batch batch = sync.batch();
      accounts.forEach((hash, account) -> batch.putAccount(hash, Accounts.encode(account)));
      storage.forEach(
          (account, slots) ->
              slots.forEach((slot, value) -> batch.putStorage(account, slot, value)));
      codes.forEach((codeHash, code) -> batch.putCode(codeHash, code));
      batch.commit();
    }
  }

  public static World randomWorld(final Random random, final int accountCount) {
    final World world = new World();
    for (int i = 0; i < accountCount; i++) {
      final Bytes32 hash = randomKey(random);
      Hash codeHash = Hash.EMPTY;
      if (random.nextInt(3) == 0) {
        final Bytes code = randomBytes(random, 1 + random.nextInt(200));
        codeHash = Hash.hash(code);
        world.codes.put(Bytes32.wrap(codeHash.getBytes()), code);
        final int slotCount = random.nextInt(4) == 0 ? 50 + random.nextInt(300) : random.nextInt(5);
        final TreeMap<Bytes32, UInt256> slots = new TreeMap<>();
        for (int s = 0; s < slotCount; s++) {
          slots.put(randomKey(random), UInt256.valueOf(1 + random.nextInt(Integer.MAX_VALUE)));
        }
        if (!slots.isEmpty()) {
          world.storage.put(hash, slots);
        }
      }
      // storage roots are deliberately wrong, the trie build must fix them
      world.accounts.put(
          hash,
          new PatriciaTrieAccountValue(
              1 + random.nextInt(1000),
              Wei.of(1 + random.nextInt(1_000_000)),
              Hash.ZERO,
              codeHash));
    }
    return world;
  }

  private static Bytes32 randomKey(final Random random) {
    return Bytes32.wrap(randomBytes(random, 32));
  }

  private static Bytes randomBytes(final Random random, final int size) {
    final byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return Bytes.wrap(bytes);
  }

  private static Bytes encodeSlot(final UInt256 value) {
    return RLP.encodeOne(value.trimLeadingZeros());
  }
}
