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
package org.hyperledger.besu.evm.worldstate;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.collections.undo.UndoNavigableMap;
import org.hyperledger.besu.collections.undo.UndoScalar;
import org.hyperledger.besu.collections.undo.Undoable;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.account.MutableAccount;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

/**
 * An implementation of {@link MutableAccount} that tracks updates made to the account since the
 * creation of the updater this is linked to.
 *
 * <p>Note that in practice this only track the modified value of the nonce and balance, but doesn't
 * remind if those were modified or not (the reason being that any modification of an account imply
 * the underlying trie node will have to be updated, and so knowing if the nonce and balance where
 * updated or not doesn't matter, we just need their new value).
 */
public class JournaledAccount implements MutableAccount, Undoable {
  private final Address address;
  private final Hash addressHash;

  @Nullable private MutableAccount account;

  private long transactionBoundaryMark;
  private final UndoScalar<Long> nonce;
  private final UndoScalar<Wei> balance;
  private final UndoScalar<Bytes> code;
  private final UndoScalar<Hash> codeHash;
  private final UndoScalar<Boolean> deleted;

  // Only contains updated storage entries, but may contain entry with a value of 0 to signify
  // deletion.
  private final UndoNavigableMap<UInt256, UInt256> updatedStorage;
  private boolean storageWasCleared = false;

  boolean immutable;

  /**
   * Instantiates a new Update tracking account.
   *
   * @param address the address
   */
  JournaledAccount(final Address address) {
    checkNotNull(address);
    this.address = address;
    this.addressHash = this.address.addressHash();
    this.account = null;

    this.nonce = UndoScalar.of(0L);
    this.balance = UndoScalar.of(Wei.ZERO);

    this.code = UndoScalar.of(Bytes.EMPTY);
    this.codeHash = UndoScalar.of(Hash.EMPTY);
    this.deleted = UndoScalar.of(Boolean.FALSE);
    this.updatedStorage = UndoNavigableMap.of(new TreeMap<>());
    this.transactionBoundaryMark = mark();
  }

  /**
   * Instantiates a new Update tracking account.
   *
   * @param account the account
   */
  public JournaledAccount(final MutableAccount account) {
    checkNotNull(account);

    this.address = account.getAddress();
    this.addressHash =
        (account instanceof JournaledAccount journaledAccount)
            ? journaledAccount.addressHash
            : this.address.addressHash();
    this.account = account;

    if (account instanceof JournaledAccount that) {
      this.nonce = that.nonce;
      this.balance = that.balance;

      this.code = that.code;
      this.codeHash = that.codeHash;

      this.deleted = that.deleted;

      this.updatedStorage = that.updatedStorage;
    } else {
      this.nonce = UndoScalar.of(account.getNonce());
      this.balance = UndoScalar.of(account.getBalance());

      this.code = UndoScalar.of(account.getCode());
      this.codeHash = UndoScalar.of(account.getCodeHash());

      this.deleted = UndoScalar.of(Boolean.FALSE);

      this.updatedStorage = UndoNavigableMap.of(new TreeMap<>());
    }
    transactionBoundaryMark = mark();
  }

  /**
   * The original account over which this tracks updates.
   *
   * @return The original account over which this tracks updates, or {@code null} if this is a newly
   *     created account.
   */
  public MutableAccount getWrappedAccount() {
    return account;
  }

  /**
   * Sets wrapped account.
   *
   * @param account the account
   */
  public void setWrappedAccount(final MutableAccount account) {
    if (this.account == null) {
      this.account = account;
      storageWasCleared = false;
    } else {
      throw new IllegalStateException("Already tracking a wrapped account");
    }
  }

  /**
   * Whether the code of the account was modified.
   *
   * @return {@code true} if the code was updated.
   */
  public boolean codeWasUpdated() {
    return code.lastUpdate() >= transactionBoundaryMark;
  }

  /**
   * A map of the storage entries that were modified.
   *
   * @return a map containing all entries that have been modified. This <b>may</b> contain entries
   *     with a value of 0 to signify deletion.
   */
  @Override
  public Map<UInt256, UInt256> getUpdatedStorage() {
    return updatedStorage;
  }

  @Override
  public void becomeImmutable() {
    immutable = true;
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
    return nonce.get();
  }

  @Override
  public void setNonce(final long value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    nonce.set(value);
  }

  @Override
  public Wei getBalance() {
    return balance.get();
  }

  @Override
  public void setBalance(final Wei value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    balance.set(value);
  }

  @Override
  public Bytes getCode() {
    return code.get();
  }

  @Override
  public Hash getCodeHash() {
    return codeHash.get();
  }

  @Override
  public boolean hasCode() {
    return !code.get().isEmpty();
  }

  /**
   * Mark the account as deleted/not deleted
   *
   * @param accountDeleted delete or don't delete this account.
   */
  public void setDeleted(final boolean accountDeleted) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    deleted.set(accountDeleted);
  }

  /**
   * Is the account marked as deleted?
   *
   * @return is the account deleted?
   */
  public Boolean getDeleted() {
    return deleted.get();
  }

  @Override
  public void setCode(final Bytes code) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    this.code.set(code == null ? Bytes.EMPTY : code);
    this.codeHash.set(code == null ? Hash.EMPTY : Hash.hash(code));
  }

  /** Mark transaction boundary. */
  void markTransactionBoundary() {
    transactionBoundaryMark = mark();
  }

  @Override
  public UInt256 getStorageValue(final UInt256 key) {
    final UInt256 value = updatedStorage.get(key);
    if (value != null) {
      return value;
    }
    if (storageWasCleared) {
      return UInt256.ZERO;
    }

    // We haven't updated the key-value yet, so either it's a new account, and it doesn't have the
    // key, or we should query the underlying storage for its existing value (which might be 0).
    return account == null ? UInt256.ZERO : account.getStorageValue(key);
  }

  @Override
  public UInt256 getOriginalStorageValue(final UInt256 key) {
    // if storage was cleared then it is because it was an empty account, hence zero storage
    // if we have no backing account, it's a new account, hence zero storage
    // otherwise ask outside of what we are journaling, journaled change may not be original value
    return (storageWasCleared || account == null) ? UInt256.ZERO : account.getStorageValue(key);
  }

  @Override
  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Bytes32 startKeyHash, final int limit) {
    final NavigableMap<Bytes32, AccountStorageEntry> entries;
    if (account != null) {
      entries = account.storageEntriesFrom(startKeyHash, limit);
    } else {
      entries = new TreeMap<>();
    }
    updatedStorage.entrySet().stream()
        .map(entry -> AccountStorageEntry.forKeyAndValue(entry.getKey(), entry.getValue()))
        .filter(entry -> entry.getKeyHash().compareTo(startKeyHash) >= 0)
        .forEach(entry -> entries.put(entry.getKeyHash(), entry));

    while (entries.size() > limit) {
      entries.remove(entries.lastKey());
    }
    return entries;
  }

  @Override
  public void setStorageValue(final UInt256 key, final UInt256 value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    updatedStorage.put(key, value);
  }

  @Override
  public void clearStorage() {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    storageWasCleared = true;
    updatedStorage.clear();
  }

  /**
   * Does this account have any storage slots that are set to non-zero values?
   *
   * @return true if the account has no storage values set to non-zero values. False if any storage
   *     is set.
   */
  @Override
  public boolean isStorageEmpty() {
    return updatedStorage.isEmpty()
        && (storageWasCleared || account == null || account.isStorageEmpty());
  }

  /**
   * Gets storage was cleared.
   *
   * @return boolean if storage was cleared
   */
  public boolean getStorageWasCleared() {
    return storageWasCleared;
  }

  /**
   * Sets storage was cleared.
   *
   * @param storageWasCleared the storage was cleared
   */
  public void setStorageWasCleared(final boolean storageWasCleared) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    this.storageWasCleared = storageWasCleared;
  }

  @Override
  public String toString() {
    String storage = updatedStorage.isEmpty() ? "[not updated]" : updatedStorage.toString();
    if (updatedStorage.isEmpty() && storageWasCleared) {
      storage = "[cleared]";
    }
    return String.format(
        "%s -> {nonce: %s, balance:%s, code:%s, storage:%s }",
        address,
        nonce,
        balance,
        code.mark() >= transactionBoundaryMark ? "[not updated]" : code.get(),
        storage);
  }

  @Override
  public long lastUpdate() {
    return Math.max(
        nonce.lastUpdate(),
        Math.max(
            balance.lastUpdate(),
            Math.max(
                code.lastUpdate(), Math.max(codeHash.lastUpdate(), updatedStorage.lastUpdate()))));
  }

  @Override
  public void undo(final long mark) {
    nonce.undo(mark);
    balance.undo(mark);
    code.undo(mark);
    codeHash.undo(mark);
    deleted.undo(mark);
    updatedStorage.undo(mark);
  }

  /** Commit this journaled account entry to the parent, if it is not a journaled account. */
  public void commit() {
    if (!(account instanceof JournaledAccount)) {
      if (nonce.updated()) {
        account.setNonce(nonce.get());
      }
      if (balance.updated()) {
        account.setBalance(balance.get());
      }
      if (code.updated()) {
        account.setCode(code.get() == null ? Bytes.EMPTY : code.get());
      }
      if (updatedStorage.updated()) {
        updatedStorage.forEach(account::setStorageValue);
      }
    }
  }
}
