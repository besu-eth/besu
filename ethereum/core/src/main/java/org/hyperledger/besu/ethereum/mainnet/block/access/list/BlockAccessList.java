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
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public record BlockAccessList(List<AccountChanges> accountChanges) {

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockAccessList)) {
      return false;
    }
    final BlockAccessList that = (BlockAccessList) o;
    return Objects.equals(accountChanges, that.accountChanges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountChanges);
  }

  public void writeTo(final RLPOutput out) {
    BlockAccessListEncoder.encode(this, out);
  }

  public static BlockAccessListBuilder builder() {
    return new BlockAccessListBuilder();
  }

  @Override
  public String toString() {
    return "BlockAccessList{" + "accountChanges=" + accountChanges + '}';
  }

  public record StorageChange(int txIndex, UInt256 newValue) {
    @Override
    public String toString() {
      return "StorageChange{txIndex=" + txIndex + ", newValue=" + newValue + '}';
    }
  }

  public record BalanceChange(int txIndex, Wei postBalance) {
    @Override
    public String toString() {
      return "BalanceChange{txIndex=" + txIndex + ", postBalance=" + postBalance + '}';
    }
  }

  public record NonceChange(int txIndex, long newNonce) {
    @Override
    public String toString() {
      return "NonceChange{txIndex=" + txIndex + ", newNonce=" + newNonce + '}';
    }
  }

  public record CodeChange(int txIndex, Bytes newCode) {
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

  public static class BlockAccessListBuilder {
    final Map<Address, AccountBuilder> accountChangesBuilders = new HashMap<>();

    public static AccessLocationTracker createPreExecutionAccessLocationTracker() {
      return new AccessLocationTracker(0);
    }

    public static AccessLocationTracker createPostExecutionAccessLocationTracker(
        final int numberOfTransactions) {
      return new AccessLocationTracker(numberOfTransactions + 1);
    }

    public static AccessLocationTracker createTransactionAccessLocationTracker(
        final int transactionLocation) {
      return new AccessLocationTracker(transactionLocation + 1);
    }

    public AccountBuilder getOrCreateAccountBuilder(final Address address) {
      return accountChangesBuilders.computeIfAbsent(address, __ -> new AccountBuilder(address));
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

    public static class AccountBuilder {
      final Address address;
      final Map<StorageSlotKey, List<StorageChange>> slotWrites = new TreeMap<>();
      final Set<StorageSlotKey> slotReads = new TreeSet<>();
      final List<BalanceChange> balances = new ArrayList<>();
      final List<NonceChange> nonces = new ArrayList<>();
      final List<CodeChange> codes = new ArrayList<>();

      AccountBuilder(final Address address) {
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

      void addStorageWrite(final StorageSlotKey slot, final int txIndex, final UInt256 value) {
        final List<StorageChange> changes =
            slotWrites.computeIfAbsent(slot, __ -> new ArrayList<>());
        slotReads.remove(slot);
        changes.add(new StorageChange(txIndex, value));
      }

      void addStorageRead(final StorageSlotKey slot) {
        if (!slotWrites.containsKey(slot)) {
          slotReads.add(slot);
        }
      }

      void addBalanceChange(final int txIndex, final Wei postBalance) {
        balances.add(new BalanceChange(txIndex, postBalance));
      }

      void addNonceChange(final int txIndex, final long newNonce) {
        nonces.add(new NonceChange(txIndex, newNonce));
      }

      void addCodeChange(final int txIndex, final Bytes code) {
        codes.add(new CodeChange(txIndex, code));
      }

      AccountChanges build() {
        final List<BalanceChange> sortedBalances = new ArrayList<>(balances);
        sortedBalances.sort(Comparator.comparingInt(BalanceChange::txIndex));

        final List<NonceChange> sortedNonces = new ArrayList<>(nonces);
        sortedNonces.sort(Comparator.comparingInt(NonceChange::txIndex));

        final List<CodeChange> sortedCodes = new ArrayList<>(codes);
        sortedCodes.sort(Comparator.comparingInt(CodeChange::txIndex));

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
          changes.sort(Comparator.comparingInt(StorageChange::txIndex));
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

      private record SortableSlotChanges(byte[] sortKey, SlotChanges value) {}

      private record SortableSlotRead(byte[] sortKey, StorageSlotKey value) {}
    }
  }
}
