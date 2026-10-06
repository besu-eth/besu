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
package org.hyperledger.besu.evm.toy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.account.MutableAccount;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class ToyAccount implements MutableAccount {

  private final Account parent;

  private boolean immutable = false;

  private Address address;
  private final Supplier<Hash> addressHash =
      Suppliers.memoize(() -> address == null ? Hash.ZERO : address.addressHash());
  private long nonce;
  private Wei balance;
  private Bytes code;
  private Supplier<Hash> codeHash =
      Suppliers.memoize(() -> code == null ? Hash.EMPTY : Hash.hash(code));
  private final Map<UInt256, UInt256> storage = new HashMap<>();

  public ToyAccount(
      final Account parent,
      final Address address,
      final long nonce,
      final Wei balance,
      final Bytes code) {
    this.parent = parent;
    this.address = address;
    this.nonce = nonce;
    this.balance = balance;
    this.code = code;
  }

  @Override
  public Address getAddress() {
    return address;
  }

  @Override
  public Hash getAddressHash() {
    return addressHash.get();
  }

  @Override
  public long getNonce() {
    return nonce;
  }

  @Override
  public Wei getBalance() {
    return balance;
  }

  @Override
  public Bytes getCode() {
    return code;
  }

  @Override
  public Hash getCodeHash() {
    return codeHash.get();
  }

  @Override
  public UInt256 getStorageValue(final UInt256 key) {
    if (storage.containsKey(key)) {
      return storage.get(key);
    } else if (parent != null) {
      return getOriginalStorageValue(key);
    } else {
      return UInt256.ZERO;
    }
  }

  @Override
  public UInt256 getOriginalStorageValue(final UInt256 key) {
    if (parent != null) {
      return parent.getStorageValue(key);
    } else {
      return getStorageValue(key);
    }
  }

  @Override
  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Bytes32 startKeyHash, final int limit) {
    throw new UnsupportedOperationException("Storage iteration not supported in toy evm");
  }

  @Override
  public void setNonce(final long value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    nonce = value;
  }

  @Override
  public void setBalance(final Wei value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    balance = value;
  }

  @Override
  public void setCode(final Bytes code) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    this.code = code;
    codeHash = Suppliers.memoize(() -> this.code == null ? Hash.EMPTY : Hash.hash(this.code));
  }

  @Override
  public void setStorageValue(final UInt256 key, final UInt256 value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    storage.put(key, value);
  }

  @Override
  public void clearStorage() {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    storage.clear();
  }

  @Override
  public Map<UInt256, UInt256> getUpdatedStorage() {
    return storage;
  }

  @Override
  public void becomeImmutable() {
    immutable = true;
  }
}
