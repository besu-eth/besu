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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

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

  public static class BlockAccessListBuilder {
    final Map<Address, AccountBuilder> accountChangesBuilders = new HashMap<>();

    public static AccessLocationTracker createPreExecutionAccessLocationTracker() {
      return new AccessLocationTracker(0);
    }

    public static AccessLocationTracker createPostExecutionAccessLocationTracker(
        final int numberOfTransactions) {
      return new AccessLocationTracker((long) numberOfTransactions + 1L);
    }

    public static AccessLocationTracker createTransactionAccessLocationTracker(
        final int transactionLocation) {
      return new AccessLocationTracker((long) transactionLocation + 1L);
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

    /**
     * Replays an immutable {@link BlockAccessList} into this builder so {@link #eip7928ItemCount()}
     * matches incremental {@link #apply(PartialBlockAccessView)} calls that produced {@code bal}.
     */
    public void mergeFrom(final BlockAccessList bal) {
      for (AccountChanges ac : bal.accountChanges()) {
        final AccountBuilder ab = getOrCreateAccountBuilder(ac.address());
        for (SlotChanges sc : ac.storageChanges()) {
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

      return new BlockAccessList(
          accountChangesBuilders.values().stream()
              .map(AccountBuilder::build)
              .sorted(Comparator.comparing(ac -> ac.address().getBytes().toUnprefixedHexString()))
              .toList());
    }

    public long eip7928ItemCount() {
      long count = accountChangesBuilders.size();
      for (AccountBuilder ab : accountChangesBuilders.values()) {
        count += (long) ab.slotWrites.size() + ab.slotReads.size();
      }
      return count;
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

      void addStorageWrite(final StorageSlotKey slot, final long txIndex, final UInt256 value) {
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
        final List<SlotChanges> slotChanges =
            slotWrites.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().getSlotKey().orElseThrow().toBytes()))
                .map(
                    e ->
                        new SlotChanges(
                            e.getKey(),
                            e.getValue().stream()
                                .sorted(Comparator.comparingLong(StorageChange::txIndex))
                                .collect(Collectors.toList())))
                .collect(Collectors.toList());

        final List<SlotRead> reads =
            slotReads.stream()
                .sorted(Comparator.comparing(e -> e.getSlotKey().orElseThrow().toBytes()))
                .map(SlotRead::new)
                .collect(Collectors.toList());

        return new AccountChanges(
            address,
            slotChanges,
            reads,
            balances.stream().sorted(Comparator.comparingLong(BalanceChange::txIndex)).toList(),
            nonces.stream().sorted(Comparator.comparingLong(NonceChange::txIndex)).toList(),
            codes.stream().sorted(Comparator.comparingLong(CodeChange::txIndex)).toList());
      }
    }
  }
}
