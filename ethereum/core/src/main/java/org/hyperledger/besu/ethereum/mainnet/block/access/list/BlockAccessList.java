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
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public record BlockAccessList(List<AccountChanges> accountChanges, Optional<Bytes> rawRlp) {

  public BlockAccessList(final List<AccountChanges> accountChanges) {
    this(accountChanges, Optional.empty());
  }

  public BlockAccessList(final List<AccountChanges> accountChanges, final Bytes rawRlp) {
    this(accountChanges, Optional.of(rawRlp));
  }

  @JsonCreator
  public static BlockAccessList fromBytes(final Bytes bytes) {
    return BlockAccessListDecoder.decode(new BytesValueRLPInput(bytes, false));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockAccessList that)) {
      return false;
    }
    return Objects.equals(accountChanges, that.accountChanges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountChanges);
  }

  public boolean isEmpty() {
    return accountChanges.isEmpty();
  }

  public long eip7928ItemCount() {
    long totalStorageKeys = 0;
    for (AccountChanges accountChange : accountChanges) {
      totalStorageKeys += accountChange.storageChanges().size();
      totalStorageKeys += accountChange.storageReads().size();
    }
    return (long) accountChanges.size() + totalStorageKeys;
  }

  public void writeTo(final RLPOutput out) {
    BlockAccessListEncoder.encode(this, out);
  }

  @JsonValue
  public Bytes encode() {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    writeTo(output);
    return output.encoded();
  }

  public static BlockAccessListBuilder builder() {
    return new BlockAccessListBuilder();
  }

  @Override
  public String toString() {
    return "BlockAccessList{" + "accountChanges=" + accountChanges + '}';
  }

  public record StorageChange(long txIndex, UInt256 newValue) {
    @Override
    public String toString() {
      return "StorageChange{txIndex=" + txIndex + ", newValue=" + newValue + '}';
    }
  }

  public record BalanceChange(long txIndex, Wei postBalance) {
    @Override
    public String toString() {
      return "BalanceChange{txIndex=" + txIndex + ", postBalance=" + postBalance + '}';
    }
  }

  public record NonceChange(long txIndex, long newNonce) {
    @Override
    public String toString() {
      return "NonceChange{txIndex=" + txIndex + ", newNonce=" + newNonce + '}';
    }
  }

  public record CodeChange(long txIndex, Bytes newCode) {
    @Override
    public String toString() {
      return "CodeChange{txIndex=" + txIndex + ", newCode=" + newCode + '}';
    }
  }

  public record SlotChanges(StorageSlotKey slot, List<StorageChange> changes) {
    @Override
    public String toString() {
      return "SlotChanges{slot=" + slot + ", changes=" + changes + '}';
    }
  }

  public record SlotRead(StorageSlotKey slot) {
    @Override
    public String toString() {
      return "SlotRead{slot=" + slot + '}';
    }
  }

  public record AccountChanges(
      Address address,
      List<SlotChanges> storageChanges,
      List<SlotRead> storageReads,
      List<BalanceChange> balanceChanges,
      List<NonceChange> nonceChanges,
      List<CodeChange> codeChanges) {

    public boolean hasAnyChange() {
      return !balanceChanges.isEmpty()
          || !nonceChanges.isEmpty()
          || !codeChanges.isEmpty()
          || !storageChanges.isEmpty();
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
          + ", balanceChanges="
          + balanceChanges
          + ", nonceChanges="
          + nonceChanges
          + ", codeChanges="
          + codeChanges
          + '}';
    }
  }

  private record SortableSlotChanges(byte[] sortKey, SlotChanges value) {}

  private record SortableSlotRead(byte[] sortKey, StorageSlotKey value) {}

  public static class BlockAccessListBuilder {
    private final Map<Address, AccountBuilder> accountChangesBuilders = new HashMap<>();

    // Kept up to date on every change so a budget check per transaction does not walk the list.
    private long itemCount;

    public static AccessLocationTracker createPreExecutionAccessLocationTracker() {
      return new AccessLocationTracker(0, true);
    }

    public static AccessLocationTracker createPostExecutionAccessLocationTracker(
        final int numberOfTransactions) {
      return new AccessLocationTracker((long) numberOfTransactions + 1L, true);
    }

    public static AccessLocationTracker createTransactionAccessLocationTracker(
        final int transactionLocation) {
      return new AccessLocationTracker((long) transactionLocation + 1L, false);
    }

    public AccountBuilder getOrCreateAccountBuilder(final Address address) {
      AccountBuilder accountBuilder = accountChangesBuilders.get(address);
      if (accountBuilder == null) {
        accountBuilder = new AccountBuilder(this, address);
        accountChangesBuilders.put(address, accountBuilder);
        itemCount++;
      }
      return accountBuilder;
    }

    public void apply(
        final AccessLocationTracker accessLocationTracker, final WorldUpdater updater) {
      apply(accessLocationTracker.createPartialBlockAccessView(updater));
    }

    public void apply(final PartialBlockAccessView partialBlockAccessView) {
      partialBlockAccessView
          .accountChanges()
          .forEach(
              account -> {
                final AccountBuilder builder = getOrCreateAccountBuilder(account.getAddress());
                if (partialBlockAccessView.isSharedIndex()) {
                  builder.removeChangesAt(partialBlockAccessView.getTxIndex());
                }
                account
                    .getStorageChanges()
                    .forEach(
                        slotChange -> {
                          builder.addStorageWrite(
                              slotChange.slot(),
                              partialBlockAccessView.getTxIndex(),
                              slotChange.newValue());
                        });

                account.getStorageReads().forEach(builder::addStorageRead);

                account
                    .getPostBalance()
                    .ifPresent(
                        change -> {
                          builder.addBalanceChange(partialBlockAccessView.getTxIndex(), change);
                        });

                account
                    .getNonceChange()
                    .ifPresent(
                        change -> {
                          builder.addNonceChange(partialBlockAccessView.getTxIndex(), change);
                        });
                account
                    .getNewCode()
                    .ifPresent(
                        change -> {
                          builder.addCodeChange(partialBlockAccessView.getTxIndex(), change);
                        });
              });
    }

    /**
     * Replays an immutable {@link BlockAccessList} into this builder so {@link #eip7928ItemCount()}
     * matches incremental {@link #apply(PartialBlockAccessView)} calls that produced {@code bal}.
     */
    public void mergeFrom(final BlockAccessList bal) {
      for (AccountChanges ac : bal.accountChanges()) {
        final AccountBuilder ab = getOrCreateAccountBuilder(ac.address());
        for (SlotChanges sc : ac.storageChanges()) {
          if (sc.changes().isEmpty()) {
            throw new IllegalArgumentException(
                "Block access list slot changes must contain at least one storage change");
          }
          for (StorageChange change : sc.changes()) {
            ab.addStorageWrite(sc.slot(), change.txIndex(), change.newValue());
          }
        }
        for (SlotRead sr : ac.storageReads()) {
          ab.addStorageRead(sr.slot());
        }
        for (BalanceChange bc : ac.balanceChanges()) {
          ab.addBalanceChange(bc.txIndex(), bc.postBalance());
        }
        for (NonceChange nc : ac.nonceChanges()) {
          ab.addNonceChange(nc.txIndex(), nc.newNonce());
        }
        for (CodeChange cc : ac.codeChanges()) {
          ab.addCodeChange(cc.txIndex(), cc.newCode());
        }
      }
    }

    public BlockAccessList build() {
      final List<AccountChanges> accountChanges = new ArrayList<>(accountChangesBuilders.size());
      for (AccountBuilder accountBuilder : accountChangesBuilders.values()) {
        accountChanges.add(accountBuilder.build());
      }
      accountChanges.sort(
          (left, right) ->
              Arrays.compareUnsigned(
                  left.address().getBytes().toArrayUnsafe(),
                  right.address().getBytes().toArrayUnsafe()));
      return new BlockAccessList(accountChanges);
    }

    public long eip7928ItemCount() {
      return itemCount;
    }

    /**
     * Returns the {@link #eip7928ItemCount()} this builder would have after {@link
     * #apply(PartialBlockAccessView)}, without changing the builder.
     */
    public long eip7928ItemCountIfApplied(final PartialBlockAccessView view) {
      if (view.isSharedIndex()) {
        return sharedIndexItemCountIfApplied(view);
      }
      // Without a shared index applying only adds accounts and slots, and a slot counts once
      // whether read or written, so only those not in the list yet add to the count.
      final Map<Address, Set<StorageSlotKey>> newSlotsByAccount = new HashMap<>();
      long count = itemCount;
      for (PartialBlockAccessView.AccountChanges account : view.accountChanges()) {
        final Address address = account.getAddress();
        final AccountBuilder existing = accountChangesBuilders.get(address);
        Set<StorageSlotKey> newSlots = newSlotsByAccount.get(address);
        if (newSlots == null) {
          newSlots = new HashSet<>();
          newSlotsByAccount.put(address, newSlots);
          if (existing == null) {
            count++;
          }
        }
        for (PartialBlockAccessView.SlotChange change : account.getStorageChanges()) {
          if (isNewSlot(existing, newSlots, change.slot())) {
            count++;
          }
        }
        for (StorageSlotKey read : account.getStorageReads()) {
          if (isNewSlot(existing, newSlots, read)) {
            count++;
          }
        }
      }
      return count;
    }

    private static boolean isNewSlot(
        final AccountBuilder existing,
        final Set<StorageSlotKey> newSlots,
        final StorageSlotKey slot) {
      return (existing == null || !existing.hasSlot(slot)) && newSlots.add(slot);
    }

    private long sharedIndexItemCountIfApplied(final PartialBlockAccessView view) {
      // Applying a shared-index view first drops earlier writes at its index, so replay it on
      // copies of just the accounts it touches.
      final BlockAccessListBuilder touchedAccounts = new BlockAccessListBuilder();
      for (PartialBlockAccessView.AccountChanges account : view.accountChanges()) {
        final Address address = account.getAddress();
        final AccountBuilder existing = accountChangesBuilders.get(address);
        if (existing != null && !touchedAccounts.accountChangesBuilders.containsKey(address)) {
          existing.copySlotsTo(touchedAccounts.getOrCreateAccountBuilder(address));
        }
      }
      final long itemCountBefore = touchedAccounts.itemCount;
      touchedAccounts.apply(view);
      return itemCount + touchedAccounts.itemCount - itemCountBefore;
    }

    public static class AccountBuilder {
      private final BlockAccessListBuilder owner;
      final Address address;
      private final Map<StorageSlotKey, List<StorageChange>> slotWrites = new HashMap<>();
      private final Set<StorageSlotKey> slotReads = new HashSet<>();
      final List<BalanceChange> balances = new ArrayList<>();
      final List<NonceChange> nonces = new ArrayList<>();
      final List<CodeChange> codes = new ArrayList<>();

      AccountBuilder(final BlockAccessListBuilder owner, final Address address) {
        this.owner = owner;
        this.address = address;
      }

      Optional<UInt256> getLastWriteValue(final UInt256 slot) {
        final StorageSlotKey slotKeyObj = new StorageSlotKey(slot);
        final List<StorageChange> storageChanges = this.slotWrites.get(slotKeyObj);
        if (storageChanges != null && !storageChanges.isEmpty()) {
          return Optional.of(storageChanges.getLast().newValue());
        } else {
          return Optional.empty();
        }
      }

      Optional<Wei> getLastBalance() {
        if (this.balances.isEmpty()) {
          return Optional.empty();
        }
        final BalanceChange balanceChange = this.balances.getLast();
        if (balanceChange != null) {
          return Optional.of(Wei.fromHexString(balanceChange.postBalance().toHexString()));
        } else {
          return Optional.empty();
        }
      }

      Optional<Long> getLastNonce() {
        if (this.nonces.isEmpty()) {
          return Optional.empty();
        }
        final NonceChange nonceChange = this.nonces.getLast();
        if (nonceChange != null) {
          return Optional.of(nonceChange.newNonce());
        } else {
          return Optional.empty();
        }
      }

      Optional<Bytes> getLastCode() {
        if (this.codes.isEmpty()) {
          return Optional.empty();
        }
        final CodeChange codeChange = this.codes.getLast();
        if (codeChange != null) {
          return Optional.of(codeChange.newCode());
        } else {
          return Optional.empty();
        }
      }

      void addStorageWrite(final StorageSlotKey slot, final long txIndex, final UInt256 value) {
        List<StorageChange> changes = slotWrites.get(slot);
        if (changes == null) {
          changes = new ArrayList<>();
          slotWrites.put(slot, changes);
          owner.itemCount++;
        }
        if (slotReads.remove(slot)) {
          owner.itemCount--;
        }
        changes.add(new StorageChange(txIndex, value));
      }

      void addStorageRead(final StorageSlotKey slot) {
        if (!slotWrites.containsKey(slot) && slotReads.add(slot)) {
          owner.itemCount++;
        }
      }

      boolean hasSlot(final StorageSlotKey slot) {
        return slotWrites.containsKey(slot) || slotReads.contains(slot);
      }

      void copySlotsTo(final AccountBuilder target) {
        slotWrites.forEach(
            (slot, changes) ->
                changes.forEach(
                    change -> target.addStorageWrite(slot, change.txIndex(), change.newValue())));
        slotReads.forEach(target::addStorageRead);
      }

      void removeChangesAt(final long txIndex) {
        slotWrites.values().forEach(changes -> changes.removeIf(c -> c.txIndex() == txIndex));
        final int writtenSlots = slotWrites.size();
        slotWrites.values().removeIf(List::isEmpty);
        owner.itemCount -= writtenSlots - slotWrites.size();
        balances.removeIf(c -> c.txIndex() == txIndex);
        nonces.removeIf(c -> c.txIndex() == txIndex);
        codes.removeIf(c -> c.txIndex() == txIndex);
      }

      void addBalanceChange(final long txIndex, final Wei postBalance) {
        balances.add(new BalanceChange(txIndex, postBalance));
      }

      void addNonceChange(final long txIndex, final long newNonce) {
        nonces.add(new NonceChange(txIndex, newNonce));
      }

      void addCodeChange(final long txIndex, final Bytes code) {
        codes.add(new CodeChange(txIndex, code));
      }

      AccountChanges build() {
        final List<BalanceChange> sortedBalances = new ArrayList<>(balances);
        sortedBalances.sort(Comparator.comparingLong(BalanceChange::txIndex));

        final List<NonceChange> sortedNonces = new ArrayList<>(nonces);
        sortedNonces.sort(Comparator.comparingLong(NonceChange::txIndex));

        final List<CodeChange> sortedCodes = new ArrayList<>(codes);
        sortedCodes.sort(Comparator.comparingLong(CodeChange::txIndex));

        return new AccountChanges(
            address,
            sortedSlotChanges(),
            sortedSlotReads(),
            sortedBalances,
            sortedNonces,
            sortedCodes);
      }

      private List<SlotChanges> sortedSlotChanges() {
        final int n = slotWrites.size();
        if (n == 0) {
          return List.of();
        }
        final SortableSlotChanges[] entries = new SortableSlotChanges[n];
        int i = 0;
        for (Map.Entry<StorageSlotKey, List<StorageChange>> e : slotWrites.entrySet()) {
          final List<StorageChange> changes = new ArrayList<>(e.getValue());
          if (changes.isEmpty()) {
            throw new IllegalStateException(
                "Block access list builder cannot emit slot changes without storage changes");
          }
          changes.sort(Comparator.comparingLong(StorageChange::txIndex));
          entries[i++] =
              new SortableSlotChanges(
                  e.getKey().getSlotKey().orElseThrow().toArray(),
                  new SlotChanges(e.getKey(), changes));
        }
        Arrays.sort(entries, (a, b) -> Arrays.compareUnsigned(a.sortKey(), b.sortKey()));
        final List<SlotChanges> result = new ArrayList<>(n);
        for (SortableSlotChanges e : entries) {
          result.add(e.value());
        }
        return result;
      }

      private List<SlotRead> sortedSlotReads() {
        final int n = slotReads.size();
        if (n == 0) {
          return List.of();
        }
        final SortableSlotRead[] entries = new SortableSlotRead[n];
        int i = 0;
        for (StorageSlotKey k : slotReads) {
          entries[i++] = new SortableSlotRead(k.getSlotKey().orElseThrow().toArray(), k);
        }
        Arrays.sort(entries, (a, b) -> Arrays.compareUnsigned(a.sortKey(), b.sortKey()));
        final List<SlotRead> result = new ArrayList<>(n);
        for (SortableSlotRead e : entries) {
          result.add(new SlotRead(e.value()));
        }
        return result;
      }
    }
  }
}
