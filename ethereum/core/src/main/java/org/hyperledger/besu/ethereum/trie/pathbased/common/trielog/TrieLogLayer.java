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
package org.hyperledger.besu.ethereum.trie.pathbased.common.trielog;

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * This class encapsulates the changes that are done to transition one block to the next. This
 * includes serialization and deserialization tasks for storing this log to off-memory storage.
 *
 * <p>In this particular formulation only the "Leaves" are tracked Future layers may track patricia
 * trie changes as well.
 */
@SuppressWarnings("unchecked")
public class TrieLogLayer implements TrieLog {

  protected Hash blockHash;
  protected Optional<Long> blockNumber = Optional.empty();

  Map<Address, PathBasedValue<AccountValue>> getAccounts() {
    return accounts;
  }

  Map<Address, PathBasedValue<Bytes>> getCode() {
    return code;
  }

  Map<Address, Map<StorageSlotKey, PathBasedValue<UInt256>>> getStorage() {
    return storage;
  }

  protected final Map<Address, PathBasedValue<AccountValue>> accounts;
  protected final Map<Address, PathBasedValue<Bytes>> code;
  protected final Map<Address, Map<StorageSlotKey, PathBasedValue<UInt256>>> storage;
  protected boolean frozen = false;

  public TrieLogLayer() {
    // TODO when tuweni fixes zero length byte comparison consider TreeMap
    this.accounts = new HashMap<>();
    this.code = new HashMap<>();
    this.storage = new HashMap<>();
  }

  /** Locks the layer so no new changes can be added; */
  @Override
  public void freeze() {
    frozen = true; // The code never bothered me anyway 🥶
  }

  @Override
  public Hash getBlockHash() {
    return blockHash;
  }

  public TrieLogLayer setBlockHash(final Hash blockHash) {
    checkState(!frozen, "Layer is Frozen");
    this.blockHash = blockHash;
    return this;
  }

  @Override
  public Optional<Long> getBlockNumber() {
    return blockNumber;
  }

  public TrieLogLayer setBlockNumber(final long blockNumber) {
    checkState(!frozen, "Layer is Frozen");
    this.blockNumber = Optional.of(blockNumber);
    return this;
  }

  public TrieLogLayer addAccountChange(
      final Address address, final AccountValue oldValue, final AccountValue newValue) {
    checkState(!frozen, "Layer is Frozen");
    accounts.put(address, new PathBasedValue<>(oldValue, newValue));
    return this;
  }

  public TrieLogLayer addCodeChange(
      final Address address, final Bytes oldValue, final Bytes newValue, final Hash blockHash) {
    checkState(!frozen, "Layer is Frozen");
    code.put(address, new PathBasedValue<>(oldValue, newValue, newValue == null));
    return this;
  }

  public TrieLogLayer addStorageChange(
      final Address address,
      final StorageSlotKey slot,
      final UInt256 oldValue,
      final UInt256 newValue) {
    checkState(!frozen, "Layer is Frozen");
    storage
        .computeIfAbsent(address, a -> new TreeMap<>())
        .put(slot, new PathBasedValue<>(oldValue, newValue));
    return this;
  }

  @Override
  public Map<Address, PathBasedValue<AccountValue>> getAccountChanges() {
    return accounts;
  }

  @Override
  public Map<Address, PathBasedValue<Bytes>> getCodeChanges() {
    return code;
  }

  @Override
  public Map<Address, Map<StorageSlotKey, PathBasedValue<UInt256>>> getStorageChanges() {
    return storage;
  }

  public boolean hasStorageChanges(final Address address) {
    return storage.containsKey(address);
  }

  @Override
  public Map<StorageSlotKey, PathBasedValue<UInt256>> getStorageChanges(final Address address) {
    return storage.getOrDefault(address, Map.of());
  }

  @Override
  public Optional<Bytes> getPriorCode(final Address address) {
    return Optional.ofNullable(code.get(address)).map(PathBasedValue::getPrior);
  }

  @Override
  public Optional<Bytes> getCode(final Address address) {
    return Optional.ofNullable(code.get(address)).map(PathBasedValue::getUpdated);
  }

  @Override
  public Optional<UInt256> getPriorStorageByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    return Optional.ofNullable(storage.get(address))
        .map(i -> i.get(storageSlotKey))
        .map(PathBasedValue::getPrior);
  }

  @Override
  public Optional<UInt256> getStorageByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    return Optional.ofNullable(storage.get(address))
        .map(i -> i.get(storageSlotKey))
        .map(PathBasedValue::getUpdated);
  }

  @Override
  public Optional<AccountValue> getPriorAccount(final Address address) {
    return Optional.ofNullable(accounts.get(address)).map(PathBasedValue::getPrior);
  }

  @Override
  public Optional<AccountValue> getAccount(final Address address) {
    return Optional.ofNullable(accounts.get(address)).map(PathBasedValue::getUpdated);
  }

  public String dump() {
    final StringBuilder sb = new StringBuilder();
    sb.append("TrieLog{" + "blockHash=").append(blockHash).append(frozen).append('}');
    sb.append("accounts\n");
    for (final Map.Entry<Address, PathBasedValue<AccountValue>> account : accounts.entrySet()) {
      sb.append(" : ").append(account.getKey()).append("\n");
      if (Objects.equals(account.getValue().getPrior(), account.getValue().getUpdated())) {
        sb.append("   = ").append(account.getValue().getUpdated()).append("\n");
      } else {
        sb.append("   - ").append(account.getValue().getPrior()).append("\n");
        sb.append("   + ").append(account.getValue().getUpdated()).append("\n");
      }
    }
    sb.append("code").append("\n");
    for (final Map.Entry<Address, PathBasedValue<Bytes>> code : code.entrySet()) {
      sb.append(" : ").append(code.getKey()).append("\n");
      if (Objects.equals(code.getValue().getPrior(), code.getValue().getUpdated())) {
        sb.append("   = ").append(code.getValue().getPrior()).append("\n");
      } else {
        sb.append("   - ").append(code.getValue().getPrior()).append("\n");
        sb.append("   + ").append(code.getValue().getUpdated()).append("\n");
      }
    }
    sb.append("Storage").append("\n");
    for (final Map.Entry<Address, Map<StorageSlotKey, PathBasedValue<UInt256>>> storage :
        storage.entrySet()) {
      sb.append(" : ").append(storage.getKey()).append("\n");
      for (final Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> slot :
          storage.getValue().entrySet()) {
        final UInt256 originalValue = slot.getValue().getPrior();
        final UInt256 updatedValue = slot.getValue().getUpdated();
        sb.append("   : ").append(slot.getKey()).append("\n");
        if (Objects.equals(originalValue, updatedValue)) {
          sb.append("     = ")
              .append((originalValue == null) ? "null" : originalValue.toShortHexString())
              .append("\n");
        } else {
          sb.append("     - ")
              .append((originalValue == null) ? "null" : originalValue.toShortHexString())
              .append("\n");
          sb.append("     + ")
              .append((updatedValue == null) ? "null" : updatedValue.toShortHexString())
              .append("\n");
        }
      }
    }
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TrieLogLayer that = (TrieLogLayer) o;
    return new EqualsBuilder()
        .append(frozen, that.frozen)
        .append(blockHash, that.blockHash)
        .append(accounts, that.accounts)
        .append(code, that.code)
        .append(storage, that.storage)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(blockHash)
        .append(frozen)
        .append(accounts)
        .append(code)
        .append(storage)
        .toHashCode();
  }

  @Override
  public DataStorageFormat getDataStorageFormat() {
    return DataStorageFormat.BONSAI;
  }
}
