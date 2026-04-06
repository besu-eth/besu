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
package org.hyperledger.besu.ethereum.trie.forest.worldview;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootCommitter;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.AbstractWorldUpdater;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class ForestMutableWorldState implements MutableWorldState {

  private final EvmConfiguration evmConfiguration;
  private final ForestWorldStateKeyValueStorage worldStateKeyValueStorage;
  private final WorldStatePreimageStorage preimageStorage;

  private final MerkleTrie<Bytes32, Bytes> accountStateTrie;
  private final Map<Address, MerkleTrie<Bytes32, Bytes>> updatedStorageTries = new HashMap<>();
  private final Map<Address, Bytes> updatedAccountCode = new HashMap<>();
  private final Map<Bytes32, UInt256> newStorageKeyPreimages = new HashMap<>();
  private final Map<Bytes32, Address> newAccountKeyPreimages = new HashMap<>();

  public ForestMutableWorldState(
      final WorldStateKeyValueStorage worldStateKeyValueStorage,
      final WorldStatePreimageStorage preimageStorage,
      final EvmConfiguration evmConfiguration) {
    this(
        MerkleTrie.EMPTY_TRIE_NODE_HASH,
        worldStateKeyValueStorage,
        preimageStorage,
        evmConfiguration);
  }

  public ForestMutableWorldState(
      final Bytes32 rootHash,
      final WorldStateKeyValueStorage worldStateKeyValueStorage,
      final WorldStatePreimageStorage preimageStorage,
      final EvmConfiguration evmConfiguration) {
    this.worldStateKeyValueStorage = (ForestWorldStateKeyValueStorage) worldStateKeyValueStorage;
    this.accountStateTrie = newAccountStateTrie(rootHash);
    this.preimageStorage = preimageStorage;
    this.evmConfiguration = evmConfiguration;
  }

  public ForestMutableWorldState(
      final WorldState worldState, final EvmConfiguration evmConfiguration) {
    // TODO: this is an abstraction leak (and kind of incorrect in that we reuse the underlying
    // storage), but the reason for this is that the accounts() method is unimplemented below and
    // can't be until NC-754.
    if (!(worldState instanceof ForestMutableWorldState other)) {
      throw new UnsupportedOperationException();
    }
    this.worldStateKeyValueStorage = other.worldStateKeyValueStorage;
    this.preimageStorage = other.preimageStorage;
    this.accountStateTrie = newAccountStateTrie(other.accountStateTrie.getRootHash());
    this.evmConfiguration = evmConfiguration;
  }

  private MerkleTrie<Bytes32, Bytes> newAccountStateTrie(final Bytes32 rootHash) {
    return new StoredMerklePatriciaTrie<>(
        (location, hash) -> worldStateKeyValueStorage.getAccountStateTrieNode(hash),
        rootHash,
        b -> b,
        b -> b);
  }

  private MerkleTrie<Bytes32, Bytes> newAccountStorageTrie(final Bytes32 rootHash) {
    return new StoredMerklePatriciaTrie<>(
        (location, hash) -> worldStateKeyValueStorage.getAccountStorageTrieNode(hash),
        rootHash,
        b -> b,
        b -> b);
  }

  @Override
  public Hash rootHash() {
    return Hash.wrap(accountStateTrie.getRootHash());
  }

  @Override
  public Hash frontierRootHash() {
    return rootHash();
  }

  @Override
  public Account get(final Address address) {
    final Hash addressHash = address.addressHash();
    return accountStateTrie
        .get(Bytes32.wrap(addressHash.getBytes()))
        .map(bytes -> deserializeAccount(address, addressHash, bytes))
        .orElse(null);
  }

  private WorldStateAccount deserializeAccount(
      final Address address, final Hash addressHash, final Bytes encoded) throws RLPException {
    final RLPInput in = RLP.input(encoded);
    final PmtStateTrieAccountValue accountValue = PmtStateTrieAccountValue.readFrom(in);
    return new WorldStateAccount(address, addressHash, accountValue);
  }

  @Override
  public WorldUpdater updater() {
    return new Updater(this, evmConfiguration);
  }

  @Override
  public Stream<StreamableAccount> streamAccounts(final Bytes32 startKeyHash, final int limit) {
    return accountStateTrie.entriesFrom(startKeyHash, limit).entrySet().stream()
        .map(
            entry -> {
              final Optional<Address> address = getAccountTrieKeyPreimage(entry.getKey());
              final WorldStateAccount account =
                  deserializeAccount(
                      address.orElse(Address.ZERO), Hash.wrap(entry.getKey()), entry.getValue());
              return new StreamableAccount(address, account);
            });
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(rootHash());
  }

  @Override
  public final boolean equals(final Object other) {
    if (!(other instanceof ForestMutableWorldState that)) {
      return false;
    }

    return this.rootHash().equals(that.rootHash());
  }

  @Override
  public void persist(final BlockHeader blockHeader, final StateRootCommitter committer) {
    final ForestWorldStateKeyValueStorage.Updater stateUpdater =
        worldStateKeyValueStorage.updater();
    committer.computeRoot(() -> applyAndComputeRoot(stateUpdater), this, stateUpdater, blockHeader);
  }

  private Hash applyAndComputeRoot(final ForestWorldStateKeyValueStorage.Updater forestUpdater) {
    for (final Bytes code : updatedAccountCode.values()) {
      forestUpdater.putCode(code);
    }
    for (final MerkleTrie<Bytes32, Bytes> updatedStorage : updatedStorageTries.values()) {
      updatedStorage.commit(
          (location, hash, value) -> forestUpdater.putAccountStorageTrieNode(hash, value));
    }
    accountStateTrie.commit(
        (location, hash, value) -> forestUpdater.putAccountStateTrieNode(hash, value));

    final WorldStatePreimageStorage.Updater preimageUpdater = preimageStorage.updater();
    newStorageKeyPreimages.forEach(preimageUpdater::putStorageTrieKeyPreimage);
    newAccountKeyPreimages.forEach(preimageUpdater::putAccountTrieKeyPreimage);

    updatedStorageTries.clear();
    updatedAccountCode.clear();
    newStorageKeyPreimages.clear();

    preimageUpdater.commit();
    forestUpdater.commit();

    return rootHash();
  }

  private static UInt256 convertToUInt256(final Bytes value) {
    // TODO: we could probably have an optimized method to decode a single scalar since it's used
    // pretty often.
    final RLPInput in = RLP.input(value);
    return in.readUInt256Scalar();
  }

  private Optional<Address> getAccountTrieKeyPreimage(final Bytes32 trieKey) {
    return Optional.ofNullable(newAccountKeyPreimages.get(trieKey))
        .or(() -> preimageStorage.getAccountTrieKeyPreimage(trieKey));
  }

  // An immutable class that represents an individual account as stored in
  // the world state's underlying merkle patricia trie.
  protected class WorldStateAccount implements Account {

    private final Address address;
    private final Hash addressHash;

    final PmtStateTrieAccountValue accountValue;

    // Lazily initialized since we don't always access storage.
    private volatile MerkleTrie<Bytes32, Bytes> storageTrie;

    private WorldStateAccount(
        final Address address,
        final Hash addressHash,
        final PmtStateTrieAccountValue accountValue) {

      this.address = address;
      this.addressHash = addressHash;
      this.accountValue = accountValue;
    }

    private MerkleTrie<Bytes32, Bytes> storageTrie() {
      final MerkleTrie<Bytes32, Bytes> updatedTrie = updatedStorageTries.get(address);
      if (updatedTrie != null) {
        storageTrie = updatedTrie;
      }
      if (storageTrie == null) {
        storageTrie = newAccountStorageTrie(Bytes32.wrap(getStorageRoot().getBytes()));
      }
      return storageTrie;
    }

    @Override
    public Address getAddress() {
      return address;
    }

    @Override
    public Hash getAddressHash() {
      return addressHash;
    }

    @Override
    public long getNonce() {
      return accountValue.getNonce();
    }

    @Override
    public Wei getBalance() {
      return accountValue.getBalance();
    }

    Hash getStorageRoot() {
      return accountValue.getStorageRoot();
    }

    @Override
    public Bytes getCode() {
      final Bytes updatedCode = updatedAccountCode.get(address);
      if (updatedCode != null) {
        return updatedCode;
      }
      // No code is common, save the KV-store lookup.
      final Hash codeHash = getCodeHash();
      if (codeHash.equals(Hash.EMPTY)) {
        return Bytes.EMPTY;
      }
      return worldStateKeyValueStorage.getCode(codeHash).orElse(Bytes.EMPTY);
    }

    @Override
    public boolean hasCode() {
      return !getCode().isEmpty();
    }

    @Override
    public Hash getCodeHash() {
      return accountValue.getCodeHash();
    }

    @Override
    public UInt256 getStorageValue(final UInt256 key) {
      return storageTrie()
          .get(Bytes32.wrap(Hash.hash(key).getBytes()))
          .map(ForestMutableWorldState::convertToUInt256)
          .orElse(UInt256.ZERO);
    }

    @Override
    public UInt256 getOriginalStorageValue(final UInt256 key) {
      return getStorageValue(key);
    }

    @Override
    public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
        final Bytes32 startKeyHash, final int limit) {
      final NavigableMap<Bytes32, AccountStorageEntry> storageEntries = new TreeMap<>();
      storageTrie()
          .entriesFrom(startKeyHash, limit)
          .forEach(
              (key, value) -> {
                final AccountStorageEntry entry =
                    AccountStorageEntry.create(
                        convertToUInt256(value), key, getStorageTrieKeyPreimage(key));
                storageEntries.put(key, entry);
              });
      return storageEntries;
    }

    /**
     * Does this account have any storage slots that are set to non-zero values?
     *
     * @return true if the account has no storage values set to non-zero values. False if any
     *     storage is set.
     */
    @Override
    public boolean isStorageEmpty() {
      return Hash.EMPTY_TRIE_HASH.equals(
          storageTrie == null ? getStorageRoot() : storageTrie.getRootHash());
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder();
      builder.append("AccountState").append("{");
      builder.append("address=").append(getAddress()).append(", ");
      builder.append("nonce=").append(getNonce()).append(", ");
      builder.append("balance=").append(getBalance()).append(", ");
      builder.append("storageRoot=").append(getStorageRoot()).append(", ");
      builder.append("codeHash=").append(getCodeHash()).append(", ");
      return builder.append("}").toString();
    }

    private Optional<UInt256> getStorageTrieKeyPreimage(final Bytes32 trieKey) {
      return Optional.ofNullable(newStorageKeyPreimages.get(trieKey))
          .or(() -> preimageStorage.getStorageTrieKeyPreimage(trieKey));
    }
  }

  protected static class Updater
      extends AbstractWorldUpdater<ForestMutableWorldState, WorldStateAccount> {

    protected Updater(
        final ForestMutableWorldState world, final EvmConfiguration evmConfiguration) {
      super(world, evmConfiguration);
    }

    @Override
    protected WorldStateAccount getForMutation(final Address address) {
      final ForestMutableWorldState wrapped = wrappedWorldView();
      final Hash addressHash = address.addressHash();
      return wrapped
          .accountStateTrie
          .get(Bytes32.wrap(addressHash.getBytes()))
          .map(bytes -> wrapped.deserializeAccount(address, addressHash, bytes))
          .orElse(null);
    }

    @Override
    public Collection<? extends Account> getTouchedAccounts() {
      return new ArrayList<>(getUpdatedAccounts());
    }

    @Override
    public Collection<Address> getDeletedAccountAddresses() {
      return new ArrayList<>(getDeletedAccounts());
    }

    @Override
    public void revert() {
      getDeletedAccounts().clear();
      getUpdatedAccounts().clear();
    }

    @Override
    public void commit() {
      final ForestMutableWorldState wrapped = wrappedWorldView();

      for (final Address address : getDeletedAccounts()) {
        final Hash addressHash = address.addressHash();
        wrapped.accountStateTrie.remove(Bytes32.wrap(addressHash.getBytes()));
        wrapped.updatedStorageTries.remove(address);
        wrapped.updatedAccountCode.remove(address);
      }

      for (final UpdateTrackingAccount<WorldStateAccount> updated : getUpdatedAccounts()) {
        final WorldStateAccount origin = updated.getWrappedAccount();
        // Save the code in key-value storage ...
        Hash codeHash = origin == null ? Hash.EMPTY : origin.getCodeHash();
        if (updated.codeWasUpdated()) {
          codeHash = Hash.hash(updated.getCode());
          wrapped.updatedAccountCode.put(updated.getAddress(), updated.getCode());
        }
        // ...and storage in the account trie first.
        final boolean freshState = origin == null || updated.getStorageWasCleared();
        Hash storageRoot = freshState ? Hash.EMPTY_TRIE_HASH : origin.getStorageRoot();
        if (freshState) {
          wrapped.updatedStorageTries.remove(updated.getAddress());
        }
        final Map<UInt256, UInt256> updatedStorage = updated.getUpdatedStorage();
        if (!updatedStorage.isEmpty()) {
          // Apply any storage updates
          final MerkleTrie<Bytes32, Bytes> storageTrie =
              freshState
                  ? wrapped.newAccountStorageTrie(Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()))
                  : origin.storageTrie();
          wrapped.updatedStorageTries.put(updated.getAddress(), storageTrie);
          final TreeSet<Map.Entry<UInt256, UInt256>> entries =
              new TreeSet<>(Map.Entry.comparingByKey());
          entries.addAll(updatedStorage.entrySet());

          for (final Map.Entry<UInt256, UInt256> entry : entries) {
            final UInt256 value = entry.getValue();
            final Hash keyHash = Hash.hash(entry.getKey());
            if (value.isZero()) {
              storageTrie.remove(Bytes32.wrap(keyHash.getBytes()));
            } else {
              wrapped.newStorageKeyPreimages.put(Bytes32.wrap(keyHash.getBytes()), entry.getKey());
              storageTrie.put(
                  Bytes32.wrap(keyHash.getBytes()),
                  RLP.encode(out -> out.writeBytes(entry.getValue().toMinimalBytes())));
            }
          }
          storageRoot = Hash.wrap(storageTrie.getRootHash());
        }

        // Save address preimage
        wrapped.newAccountKeyPreimages.put(
            Bytes32.wrap(updated.getAddressHash().getBytes()), updated.getAddress());
        // Lastly, save the new account.
        final Bytes account =
            serializeAccount(updated.getNonce(), updated.getBalance(), storageRoot, codeHash);

        wrapped.accountStateTrie.put(Bytes32.wrap(updated.getAddressHash().getBytes()), account);
      }
    }

    private static Bytes serializeAccount(
        final long nonce, final Wei balance, final Hash storageRoot, final Hash codeHash) {
      final PmtStateTrieAccountValue accountValue =
          new PmtStateTrieAccountValue(nonce, balance, storageRoot, codeHash);
      return RLP.encode(accountValue::writeTo);
    }
  }
}
