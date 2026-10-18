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
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/** An address-keyed world state that can produce block access lists of random blocks. */
public final class TestWorld {

  /** One account. */
  public static final class Account {
    long nonce;
    Wei balance = Wei.ZERO;
    Bytes code = Bytes.EMPTY;
    final TreeMap<UInt256, UInt256> storage = new TreeMap<>();

    Account copy() {
      final Account copy = new Account();
      copy.nonce = nonce;
      copy.balance = balance;
      copy.code = code;
      copy.storage.putAll(storage);
      return copy;
    }

    boolean isEmpty() {
      return nonce == 0 && balance.isZero() && code.isEmpty();
    }
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

  public TestWorld copy() {
    final TestWorld copy = new TestWorld();
    accounts.forEach((address, account) -> copy.accounts.put(address, account.copy()));
    return copy;
  }

  /**
   * Applies a random block to this world and returns its block access list.
   *
   * @param changes the number of accounts touched
   */
  public BlockAccessList mutate(final Random random, final int changes) {
    final List<Address> existing = new ArrayList<>(accounts.keySet());
    final Map<Address, AccountChanges> bal = new HashMap<>();
    for (int i = 0; i < changes; i++) {
      final int kind = random.nextInt(10);
      if (kind == 0) {
        // a new contract with storage
        final Address address = randomAddress(random);
        final Account account = new Account();
        account.nonce = 1;
        account.code = randomBytes(random, 20);
        final List<SlotChanges> slots = new ArrayList<>();
        for (int s = 0; s < 3; s++) {
          final UInt256 key = randomSlot(random);
          final UInt256 value = UInt256.valueOf(1 + random.nextInt(1000));
          account.storage.put(key, value);
          slots.add(slotChange(key, value));
        }
        accounts.put(address, account);
        bal.put(
            address,
            new AccountChanges(
                address,
                sorted(slots),
                List.of(),
                List.of(),
                List.of(new NonceChange(1, 1)),
                List.of(new CodeChange(1, account.code))));
        continue;
      }
      final Address address = existing.get(random.nextInt(existing.size()));
      if (bal.containsKey(address)) {
        continue;
      }
      final Account account = accounts.get(address);
      final List<SlotChanges> slots = new ArrayList<>();
      final List<BalanceChange> balances = new ArrayList<>();
      final List<NonceChange> nonces = new ArrayList<>();
      if (kind == 1 && account.code.isEmpty() && account.storage.isEmpty()) {
        // the account ends up empty and disappears
        account.nonce = 0;
        account.balance = Wei.ZERO;
        balances.add(new BalanceChange(1, Wei.ZERO));
        nonces.add(new NonceChange(1, 0));
      } else {
        account.balance = Wei.of(1 + random.nextInt(1_000_000));
        account.nonce++;
        balances.add(new BalanceChange(1, account.balance));
        nonces.add(new NonceChange(1, account.nonce));
        if (!account.code.isEmpty()) {
          final List<UInt256> keys = new ArrayList<>(account.storage.keySet());
          for (int s = 0; s < 1 + random.nextInt(4); s++) {
            final UInt256 key =
                keys.isEmpty() || random.nextBoolean()
                    ? randomSlot(random)
                    : keys.get(random.nextInt(keys.size()));
            final UInt256 value =
                random.nextInt(4) == 0 ? UInt256.ZERO : UInt256.valueOf(1 + random.nextInt(1000));
            if (value.isZero()) {
              account.storage.remove(key);
            } else {
              account.storage.put(key, value);
            }
            slots.add(slotChange(key, value));
          }
        }
      }
      bal.put(
          address,
          new AccountChanges(address, sorted(slots), List.of(), balances, nonces, List.of()));
    }
    accounts.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    final List<AccountChanges> sortedChanges = new ArrayList<>(bal.values());
    sortedChanges.sort(Comparator.comparing(AccountChanges::address));
    return new BlockAccessList(sortedChanges);
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
          out.put(hash(address), RLP.encode(value::writeTo));
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

  private static List<SlotChanges> sorted(final List<SlotChanges> slots) {
    final TreeMap<UInt256, SlotChanges> byKey = new TreeMap<>();
    slots.forEach(slot -> byKey.put(slot.slot().getSlotKey().orElseThrow(), slot));
    return new ArrayList<>(byKey.values());
  }

  private static SlotChanges slotChange(final UInt256 key, final UInt256 value) {
    return new SlotChanges(new StorageSlotKey(key), List.of(new StorageChange(1, value)));
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
