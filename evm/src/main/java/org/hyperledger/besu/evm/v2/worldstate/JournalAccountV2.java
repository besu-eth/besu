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
package org.hyperledger.besu.evm.v2.worldstate;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.CodeCache;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

/**
 * An account as a transaction on EVM v2 sees it. Every call frame of the transaction shares this
 * object, and every change is recorded in the {@link TransactionJournalV2} so that a frame can
 * revert it.
 */
public final class JournalAccountV2 implements MutableAccount {

  /** A storage slot the transaction has read, written or warmed up. */
  static final class Slot {
    // null until read or written, and again when the write that set it is reverted
    @Nullable UInt256 current;
    // the storage epoch current belongs to; a value of another epoch is not the account's storage
    int epoch;
    // value at the start of the transaction, null until SSTORE needs it
    @Nullable UInt256 original;
    // written by the transaction, so the commit writes it to the parent even if unchanged
    boolean dirty;
    // warm as per EIP-2929, which outlives clearing the storage and creating the account
    boolean warm;
    // the key as it was first warmed up, for the access list
    @Nullable Bytes32 warmKey;
  }

  private final TransactionJournalV2 journal;
  private final Address address;
  // the account in the parent updater when the transaction first looked it up
  @Nullable private final Account parentAccount;
  @Nullable private Hash addressHash;
  private boolean immutable;

  boolean exists;
  // created by the transaction: storage reads zero and the parent account is not consulted
  boolean fresh;
  long nonce;
  Wei balance;
  // set by the transaction, null while the code is the parent account's
  @Nullable Code code;
  final HashMap<UInt256, Slot> slots = new HashMap<>();
  // changes when the storage is cleared or the account created, which drops every slot value
  int storageEpoch;
  boolean storageCleared;
  // log index of the change that last touched or deleted this account, -1 if none
  int touchedAt = -1;
  int deletedAt = -1;

  JournalAccountV2(
      final TransactionJournalV2 journal,
      final Address address,
      @Nullable final Account parentAccount) {
    this.journal = journal;
    this.address = address;
    this.parentAccount = parentAccount;
    if (parentAccount == null) {
      this.balance = Wei.ZERO;
    } else {
      this.exists = true;
      this.nonce = parentAccount.getNonce();
      this.balance = parentAccount.getBalance();
    }
  }

  @Override
  public Address getAddress() {
    return address;
  }

  @Override
  public Hash getAddressHash() {
    if (addressHash == null) {
      addressHash = parentAccount != null ? parentAccount.getAddressHash() : address.addressHash();
    }
    return addressHash;
  }

  @Override
  public long getNonce() {
    return nonce;
  }

  @Override
  public void setNonce(final long value) {
    checkMutable();
    journal.logNonce(this);
    nonce = value;
  }

  @Override
  public Wei getBalance() {
    return balance;
  }

  @Override
  public void setBalance(final Wei value) {
    checkMutable();
    journal.logBalance(this);
    balance = value;
  }

  @Override
  public Bytes getCode() {
    if (code != null) {
      return code.getBytes();
    }
    return parentAccount == null ? Bytes.EMPTY : parentAccount.getCode();
  }

  @Override
  public Hash getCodeHash() {
    if (code != null) {
      return code.getCodeHash();
    }
    return parentAccount == null ? Hash.EMPTY : parentAccount.getCodeHash();
  }

  @Override
  public boolean hasCode() {
    if (code != null) {
      return code.getSize() > 0;
    }
    return parentAccount != null && parentAccount.hasCode();
  }

  @Override
  public Code getOrCreateCachedCode() {
    final Code updatedCode = code;
    if (updatedCode == null) {
      return parentAccount == null ? Code.EMPTY_CODE : parentAccount.getOrCreateCachedCode();
    }
    final CodeCache codeCache = getCodeCache();
    if (codeCache == null || updatedCode.getSize() == 0) {
      return updatedCode;
    }
    // share one instance of the new code, so its jump destinations are only analysed once
    final Hash codeHash = updatedCode.getCodeHash();
    final Code cachedCode = codeCache.getIfPresent(codeHash);
    if (cachedCode == null) {
      codeCache.put(codeHash, updatedCode);
      return updatedCode;
    }
    code = cachedCode;
    return cachedCode;
  }

  @Override
  @Nullable
  public CodeCache getCodeCache() {
    return fresh || parentAccount == null ? null : parentAccount.getCodeCache();
  }

  @Override
  public void setCode(@Nullable final Bytes code) {
    checkMutable();
    journal.logCode(this);
    if (code == null) {
      // the genesis allocation passes null for accounts without code
      this.code = null;
    } else {
      this.code = code.isEmpty() ? Code.EMPTY_CODE : new Code(code);
    }
  }

  @Override
  public UInt256 getStorageValue(final UInt256 key) {
    final Slot slot = slot(key);
    if (slot.current != null && slot.epoch == storageEpoch) {
      return slot.current;
    }
    final UInt256 value = readsParentStorage() ? parentAccount.getStorageValue(key) : UInt256.ZERO;
    if (slot.current != null) {
      // a value of another epoch comes back when the clear or creation is reverted
      journal.logSlot(slot);
      slot.dirty = false;
    }
    // the parent does not change while the transaction runs, so a first read is not journaled
    slot.current = value;
    slot.epoch = storageEpoch;
    return value;
  }

  private Slot slot(final UInt256 key) {
    Slot slot = slots.get(key);
    if (slot == null) {
      slot = new Slot();
      slot.epoch = storageEpoch;
      slots.put(key, slot);
    }
    return slot;
  }

  /**
   * Warms up a storage slot of this account as per EIP-2929.
   *
   * @param key the slot
   * @return true if the slot was already warm
   */
  public boolean warmUpStorage(final UInt256 key) {
    return warmUpStorage(key, key);
  }

  /** Warms up a slot of the access list, which no frame's revert makes cold again. */
  void warmUpFromAccessList(final Bytes32 warmKey) {
    final Slot slot = slot(UInt256.fromBytes(warmKey));
    if (!slot.warm) {
      slot.warm = true;
      slot.warmKey = warmKey;
    }
  }

  boolean warmUpStorage(final UInt256 key, final Bytes32 warmKey) {
    final Slot slot = slot(key);
    if (slot.warm) {
      return true;
    }
    journal.logWarmUp(slot);
    slot.warm = true;
    slot.warmKey = warmKey;
    return false;
  }

  @Override
  public UInt256 getOriginalStorageValue(final UInt256 key) {
    if (!readsParentStorage()) {
      return UInt256.ZERO;
    }
    final Slot slot = slot(key);
    if (slot.original == null) {
      slot.original = parentAccount.getOriginalStorageValue(key);
    }
    return slot.original;
  }

  private boolean readsParentStorage() {
    return !fresh && !storageCleared && parentAccount != null;
  }

  @Override
  public void setStorageValue(final UInt256 key, final UInt256 value) {
    checkMutable();
    final Slot slot = slot(key);
    journal.logSlot(slot);
    slot.current = value;
    slot.epoch = storageEpoch;
    slot.dirty = true;
  }

  @Override
  public void clearStorage() {
    checkMutable();
    journal.logClearStorage(this);
    storageEpoch = journal.nextStorageEpoch();
    storageCleared = true;
  }

  boolean isWritten(final Slot slot) {
    return slot.dirty && slot.epoch == storageEpoch;
  }

  @Override
  public Map<UInt256, UInt256> getUpdatedStorage() {
    final Map<UInt256, UInt256> updated = new TreeMap<>();
    slots.forEach(
        (key, slot) -> {
          if (isWritten(slot)) {
            updated.put(key, slot.current);
          }
        });
    return updated;
  }

  @Override
  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Bytes32 startKeyHash, final int limit) {
    final NavigableMap<Bytes32, AccountStorageEntry> entries =
        readsParentStorage()
            ? parentAccount.storageEntriesFrom(startKeyHash, limit)
            : new TreeMap<>();
    getUpdatedStorage().entrySet().stream()
        .map(entry -> AccountStorageEntry.forKeyAndValue(entry.getKey(), entry.getValue()))
        .filter(entry -> entry.getKeyHash().compareTo(startKeyHash) >= 0)
        .forEach(entry -> entries.put(entry.getKeyHash(), entry));
    while (entries.size() > limit) {
      entries.remove(entries.lastKey());
    }
    return entries;
  }

  @Override
  public void becomeImmutable() {
    immutable = true;
  }

  private void checkMutable() {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
  }

  @Override
  public String toString() {
    return String.format(
        "%s -> {nonce: %s, balance:%s, code:%s, storage:%s }",
        address,
        nonce,
        balance,
        code != null ? code.getBytes() : "[not updated]",
        storageCleared ? "[cleared] " + getUpdatedStorage() : getUpdatedStorage());
  }
}
