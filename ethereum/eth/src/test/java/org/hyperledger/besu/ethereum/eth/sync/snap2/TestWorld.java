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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/** An address-keyed world state, with the trie leaves of its accounts and storages. */
public final class TestWorld {

  /** One account. */
  public static final class Account {
    long nonce;
    Wei balance = Wei.ZERO;
    Bytes code = Bytes.EMPTY;
    final TreeMap<UInt256, UInt256> storage = new TreeMap<>();
  }

  private final TreeMap<Address, Account> accounts = new TreeMap<>();

  public static TestWorld random(
      final Random random, final int accountCount, final int largeStorages, final int largeSize) {
    final TestWorld world = new TestWorld();
    for (int i = 0; i < accountCount; i++) {
      final Account account = new Account();
      account.nonce = random.nextInt(100);
      account.balance = Wei.of(1 + random.nextInt(1_000_000));
      // the first accounts get the large storages
      if (i < largeStorages || random.nextInt(3) == 0) {
        account.code = randomBytes(random, 1 + random.nextInt(300));
        final int slots = i < largeStorages ? largeSize : random.nextInt(8);
        for (int s = 0; s < slots; s++) {
          account.storage.put(randomSlot(random), UInt256.valueOf(1 + random.nextInt(1 << 30)));
        }
      }
      world.accounts.put(randomAddress(random), account);
    }
    return world;
  }

  /** Returns a world of accounts that each hold one slot and no bytecode. */
  public static TestWorld storagesWithoutCode(final Random random, final int accountCount) {
    final TestWorld world = new TestWorld();
    for (int i = 0; i < accountCount; i++) {
      final Account account = new Account();
      account.nonce = 1;
      account.storage.put(randomSlot(random), UInt256.ONE);
      world.accounts.put(randomAddress(random), account);
    }
    return world;
  }

  /** Returns the account trie leaves keyed by account hash. */
  public NavigableMap<Bytes32, Bytes> accountLeaves() {
    final TreeMap<Bytes32, Bytes> out = new TreeMap<>();
    accounts.forEach(
        (address, account) -> {
          final Hash codeHash = account.code.isEmpty() ? Hash.EMPTY : Hash.hash(account.code);
          final PatriciaTrieAccountValue value =
              new PatriciaTrieAccountValue(
                  account.nonce, account.balance, Hash.wrap(storageRoot(account)), codeHash);
          out.put(hash(address), Accounts.encode(value));
        });
    return out;
  }

  /** Returns the storage leaves of every account with storage, keyed by account hash. */
  public Map<Bytes32, NavigableMap<Bytes32, Bytes>> storageLeaves() {
    final Map<Bytes32, NavigableMap<Bytes32, Bytes>> out = new HashMap<>();
    accounts.forEach(
        (address, account) -> {
          if (!account.storage.isEmpty()) {
            out.put(hash(address), storageLeaves(account));
          }
        });
    return out;
  }

  /** Returns every bytecode keyed by code hash. */
  public Map<Bytes32, Bytes> codes() {
    final Map<Bytes32, Bytes> out = new HashMap<>();
    accounts.values().stream()
        .filter(account -> !account.code.isEmpty())
        .forEach(
            account -> out.put(Bytes32.wrap(Hash.hash(account.code).getBytes()), account.code));
    return out;
  }

  public Bytes32 root() {
    final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie =
        new SimpleMerklePatriciaTrie<>(Function.identity());
    accountLeaves().forEach(trie::put);
    return trie.getRootHash();
  }

  private static NavigableMap<Bytes32, Bytes> storageLeaves(final Account account) {
    final TreeMap<Bytes32, Bytes> out = new TreeMap<>();
    account.storage.forEach(
        (key, value) -> out.put(slotHash(key), RLP.encodeOne(value.trimLeadingZeros())));
    return out;
  }

  private static Bytes32 storageRoot(final Account account) {
    final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie =
        new SimpleMerklePatriciaTrie<>(Function.identity());
    storageLeaves(account).forEach(trie::put);
    return trie.getRootHash();
  }

  public static Bytes32 hash(final Address address) {
    return Bytes32.wrap(address.addressHash().getBytes());
  }

  public static Bytes32 slotHash(final UInt256 key) {
    return Bytes32.wrap(new StorageSlotKey(key).getSlotHash().getBytes());
  }

  private static Address randomAddress(final Random random) {
    return Address.wrap(randomBytes(random, 20));
  }

  private static UInt256 randomSlot(final Random random) {
    return UInt256.valueOf(random.nextLong() & Long.MAX_VALUE);
  }

  private static Bytes randomBytes(final Random random, final int size) {
    final byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return Bytes.wrap(bytes);
  }
}
