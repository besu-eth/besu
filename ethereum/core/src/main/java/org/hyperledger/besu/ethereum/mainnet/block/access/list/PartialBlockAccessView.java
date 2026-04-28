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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Represents a partial view of a Block Access List (BAL) for a single transaction within a block.
 *
 * <p>This class captures the subset of account and storage access information that results from the
 * execution of a single transaction. Each {@link PartialBlockAccessView} instance corresponds to
 * one transaction (identified by its transaction index) and contains the state changes and reads
 * made by that transaction, such as account balance updates, nonce changes, code updates, and
 * storage slot reads/writes.
 *
 * <p>Because it only reflects the access list from one transaction, it does not represent the
 * complete block access list. To reconstruct the full block-level access list, all partial views
 * from each transaction in the block must be merged together.
 *
 * <p>This class is primarily used as an intermediate representation when building or aggregating
 * access lists at the block level, and it supports convenient construction through the {@link
 * PartialBlockAccessViewBuilder}.
 */
public final class PartialBlockAccessView {

  private final long txIndex;
  private final List<AccountChanges> accountChanges;

  public PartialBlockAccessView(final List<AccountChanges> accountChanges, final long txIndex) {
    this.accountChanges = accountChanges;
    this.txIndex = txIndex;
  }

  @Override
  public String toString() {
    return "PartialBlockAccessView{"
        + "txIndex="
        + txIndex
        + ", accountChanges="
        + accountChanges
        + '}';
  }

  public long getTxIndex() {
    return txIndex;
  }

  public List<AccountChanges> accountChanges() {
    return accountChanges;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (PartialBlockAccessView) obj;
    return Objects.equals(this.accountChanges, that.accountChanges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountChanges);
  }

  public record SlotChange(StorageSlotKey slot, UInt256 newValue) {
    @Override
    public String toString() {
      return "SlotChange{newValue=" + newValue + '}';
    }
  }

  public static final class AccountChanges {
    private final Address address;
    private Optional<Wei> postBalance;
    private final Optional<Long> nonceChange;
    private final Optional<Bytes> newCode;
    private final List<StorageSlotKey> storageReads;
    private final List<SlotChange> storageChanges;

    public AccountChanges(
        final Address address,
        final Optional<Wei> postBalance,
        final Optional<Long> nonceChange,
        final Optional<Bytes> newCode,
        final List<StorageSlotKey> storageReads,
        final List<SlotChange> storageChanges) {
      this.address = address;
      this.postBalance = postBalance;
      this.nonceChange = nonceChange;
      this.newCode = newCode;
      this.storageReads = storageReads;
      this.storageChanges = storageChanges;
    }

    public Address getAddress() {
      return address;
    }

    public Optional<Wei> getPostBalance() {
      return postBalance;
    }

    public void setPostBalance(final Wei postBalance) {
      this.postBalance = Optional.ofNullable(postBalance);
    }

    public Optional<Long> getNonceChange() {
      return nonceChange;
    }

    public Optional<Bytes> getNewCode() {
      return newCode;
    }

    public List<StorageSlotKey> getStorageReads() {
      return storageReads;
    }

    public List<SlotChange> getStorageChanges() {
      return storageChanges;
    }

    @Override
    public String toString() {
      return "AccountChanges{"
          + "address="
          + address
          + ", storageChanges="
          + storageChanges
          + ", storageReads="
          + storageReads
          + ", postBalance="
          + postBalance
          + ", postBalance="
          + postBalance
          + ", newCode="
          + newCode
          + '}';
    }
  }

  /** Builder for PartialBlockAccessView. */
  public static class PartialBlockAccessViewBuilder {
    private long txIndex;
    private final Map<Address, AccountChangesBuilder> accountBuilders = new HashMap<>();

    public PartialBlockAccessViewBuilder withTxIndex(final long txIndex) {
      this.txIndex = txIndex;
      return this;
    }

    public AccountChangesBuilder getOrCreateAccountBuilder(final Address address) {
      return accountBuilders.computeIfAbsent(address, AccountChangesBuilder::new);
    }

    public PartialBlockAccessView build() {
      List<AccountChanges> accountChanges =
          accountBuilders.values().stream()
              .map(AccountChangesBuilder::build)
              .sorted(
                  Comparator.comparing(ac -> ac.getAddress().getBytes().toUnprefixedHexString()))
              .toList();
      return new PartialBlockAccessView(accountChanges, txIndex);
    }
  }

  public static class AccountChangesBuilder {
    private final Address address;
    private Optional<Wei> postBalance = Optional.empty();
    private Optional<Long> nonceChange = Optional.empty();
    private Optional<Bytes> newCode = Optional.empty();
    private final List<StorageSlotKey> storageReads = new ArrayList<>();
    private final List<SlotChange> storageChanges = new ArrayList<>();

    public AccountChangesBuilder(final Address address) {
      this.address = address;
    }

    public AccountChangesBuilder withPostBalance(final Wei postBalance) {
      this.postBalance = Optional.ofNullable(postBalance);
      return this;
    }

    public AccountChangesBuilder withNonceChange(final Long nonceChange) {
      this.nonceChange = Optional.ofNullable(nonceChange);
      return this;
    }

    public AccountChangesBuilder withNewCode(final Bytes newCode) {
      this.newCode = Optional.ofNullable(newCode);
      return this;
    }

    public AccountChangesBuilder addStorageRead(final StorageSlotKey slotRead) {
      storageReads.add(slotRead);
      return this;
    }

    public AccountChangesBuilder addStorageChange(
        final StorageSlotKey slot, final UInt256 newValue) {
      storageChanges.add(new SlotChange(slot, newValue));
      return this;
    }

    public AccountChanges build() {
      return new AccountChanges(
          address, postBalance, nonceChange, newCode, storageReads, storageChanges);
    }
  }
}
