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
package org.hyperledger.besu.evm.frame;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

/**
 * The state diff of an executing frame transaction, as the EIP-7906 assertion operations observe it
 * from an ASSERT frame.
 *
 * <p>The diff compares the transaction prestate (the state before the transaction, which includes
 * the earlier transactions of the block) with the state as of the read. Values the transaction
 * restored leave no entry. The tables are in the canonical enumeration order: balance changes and
 * deployments ascending by address as uint160, slot changes ascending by address and then by key as
 * uint256, and events in emission order.
 *
 * <p>Instances are immutable. The processor computes one with {@link #compute} from the
 * transaction's write layers, and tests build them with {@link #builder()}.
 */
public final class TransactionDiff {

  /** The diff of a transaction that changed nothing and emitted no event. */
  public static final TransactionDiff EMPTY = builder().build();

  /**
   * An entry of the {@code balances_changed} table.
   *
   * @param address the account whose balance changed
   * @param before the balance in the transaction prestate
   * @param after the balance as of the read
   */
  public record BalanceChange(Address address, Wei before, Wei after) {}

  /**
   * An entry of the {@code slots_changed} table.
   *
   * @param address the account whose storage changed
   * @param key the storage slot key
   * @param before the slot value in the transaction prestate
   * @param after the slot value as of the read
   */
  public record SlotChange(Address address, UInt256 key, UInt256 before, UInt256 after) {}

  /**
   * An entry of the {@code contracts_deployed} table.
   *
   * @param address the address of the deployed contract
   * @param codeHash the code hash as of the read
   */
  public record DeployedContract(Address address, Hash codeHash) {}

  /**
   * An entry of the events table.
   *
   * @param logger the emitting address
   * @param topics the topics, at most four
   * @param data the non-indexed data
   */
  public record Event(Address logger, List<Bytes32> topics, Bytes data) {

    /**
     * Normalizes a log into an event.
     *
     * @param log the log
     * @return the event
     */
    public static Event of(final Log log) {
      final List<Bytes32> topics = new ArrayList<>(log.getTopics().size());
      for (final LogTopic topic : log.getTopics()) {
        topics.add(Bytes32.leftPad(topic.getBytes()));
      }
      return new Event(log.getLogger(), List.copyOf(topics), log.getData());
    }
  }

  /**
   * The account fields the diff compares.
   *
   * @param nonce the nonce
   * @param balance the balance
   * @param codeHash the raw code hash, {@link Hash#EMPTY} for an account without code
   */
  public record AccountFields(long nonce, Wei balance, Hash codeHash) {

    /** The fields of an account that does not exist. */
    public static final AccountFields EMPTY = new AccountFields(0L, Wei.ZERO, Hash.EMPTY);

    /**
     * Reads the fields of an account.
     *
     * @param account the account, or null when it does not exist
     * @return the fields, {@link #EMPTY} for a missing account
     */
    public static AccountFields of(final @Nullable Account account) {
      if (account == null) {
        return EMPTY;
      }
      final Hash codeHash = account.getCodeHash();
      return new AccountFields(
          account.getNonce(), account.getBalance(), codeHash == null ? Hash.EMPTY : codeHash);
    }
  }

  private final List<BalanceChange> balances;
  private final List<SlotChange> slots;
  private final List<DeployedContract> deployed;
  private final List<Event> events;

  private TransactionDiff(
      final List<BalanceChange> balances,
      final List<SlotChange> slots,
      final List<DeployedContract> deployed,
      final List<Event> events) {
    this.balances = balances;
    this.slots = slots;
    this.deployed = deployed;
    this.events = events;
  }

  /**
   * Computes the diff of a frame transaction from the updater layers that hold its writes.
   *
   * <p>The candidates are every account and slot written in a layer: a superset of what changed,
   * since reads through {@code getAccount} and value-neutral {@code getOrCreate} calls also create
   * trackers. Comparing each candidate's prestate and current value filters them out, together with
   * writes that restored the prestate. A deletion or a storage clear in a layer drops the slot keys
   * written below it, so wiped slots that were not written again are not enumerated.
   *
   * <p>Only {@link WorldView#get} is called, which creates no tracker in any updater and records
   * nothing in the block access list. The prestate view must not see the transaction's writes,
   * which holds for every {@code StackedUpdater} chain (the default updater mode) but not for a
   * {@code JournaledUpdater}, whose layers share their accounts. The JOURNALED mode cannot reach
   * this code: ProtocolSpecBuilder rejects it for every fork with a block access list, and every
   * fork with frame transactions has one (pinned by ProtocolScheduleBuilderTest).
   *
   * @param prestate the transaction prestate
   * @param post the state as of the read, which sees every write layer
   * @param writeLayers the updaters holding the transaction's surviving writes, outermost first
   * @param logs the logs of the completed frames, in emission order
   * @return the diff
   */
  public static TransactionDiff compute(
      final WorldView prestate,
      final WorldView post,
      final List<? extends WorldUpdater> writeLayers,
      final List<Log> logs) {
    final Set<Address> written = new TreeSet<>();
    final Map<Address, Set<UInt256>> writtenSlots = new TreeMap<>();
    for (final WorldUpdater layer : writeLayers) {
      for (final Address deleted : layer.getDeletedAccountAddresses()) {
        written.add(deleted);
        writtenSlots.remove(deleted);
      }
      for (final Account account : layer.getTouchedAccounts()) {
        final Address address = account.getAddress();
        written.add(address);
        if (account instanceof UpdateTrackingAccount<?> tracker && tracker.getStorageWasCleared()) {
          writtenSlots.remove(address);
        }
        if (account instanceof MutableAccount mutable && !mutable.getUpdatedStorage().isEmpty()) {
          writtenSlots
              .computeIfAbsent(address, __ -> new TreeSet<>())
              .addAll(mutable.getUpdatedStorage().keySet());
        }
      }
    }

    final Builder builder = builder();
    for (final Address address : written) {
      final AccountFields before = AccountFields.of(prestate.get(address));
      final Account afterAccount = post.get(address);
      final AccountFields after = AccountFields.of(afterAccount);
      // Only a deployment candidate needs its code: the delegation check excludes EIP-7702
      // designators from the deployed table.
      final boolean afterIsDelegation =
          afterAccount != null
              && before.codeHash().equals(Hash.EMPTY)
              && !after.codeHash().equals(Hash.EMPTY)
              && CodeDelegationHelper.hasCodeDelegation(afterAccount.getCode());
      builder.account(address, before, after, afterIsDelegation);
    }
    for (final Map.Entry<Address, Set<UInt256>> entry : writtenSlots.entrySet()) {
      final Address address = entry.getKey();
      final Account before = prestate.get(address);
      final Account after = post.get(address);
      for (final UInt256 key : entry.getValue()) {
        builder.slot(
            address,
            key,
            before == null ? UInt256.ZERO : before.getStorageValue(key),
            after == null ? UInt256.ZERO : after.getStorageValue(key));
      }
    }
    return builder.logs(logs).build();
  }

  /**
   * Starts a diff.
   *
   * @return a builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * The {@code balances_changed} table, ascending by address.
   *
   * @return the balance changes
   */
  public List<BalanceChange> balances() {
    return balances;
  }

  /**
   * The {@code slots_changed} table, ascending by address and then by key.
   *
   * @return the slot changes
   */
  public List<SlotChange> slots() {
    return slots;
  }

  /**
   * The {@code contracts_deployed} table, ascending by address.
   *
   * @return the deployed contracts
   */
  public List<DeployedContract> deployed() {
    return deployed;
  }

  /**
   * The events table, in emission order.
   *
   * @return the events
   */
  public List<Event> events() {
    return events;
  }

  /** Collects the candidate entries of a diff and builds its tables. */
  public static final class Builder {

    private record AccountEntry(
        AccountFields before, AccountFields after, boolean afterIsDelegation) {}

    private final Map<Address, AccountEntry> accounts = new TreeMap<>();
    private final Map<Address, Map<UInt256, SlotChange>> slots = new TreeMap<>();
    private final List<Event> events = new ArrayList<>();

    private Builder() {}

    /**
     * Adds a candidate account whose current code is not an EIP-7702 delegation designator.
     *
     * @param address the account
     * @param before its fields in the transaction prestate
     * @param after its fields as of the read
     * @return this builder
     */
    public Builder account(
        final Address address, final AccountFields before, final AccountFields after) {
      return account(address, before, after, false);
    }

    /**
     * Adds a candidate account. A later call for the same address replaces the earlier one.
     *
     * @param address the account
     * @param before its fields in the transaction prestate
     * @param after its fields as of the read
     * @param afterIsDelegation whether the current code is an EIP-7702 delegation designator, which
     *     is not a deployment
     * @return this builder
     */
    public Builder account(
        final Address address,
        final AccountFields before,
        final AccountFields after,
        final boolean afterIsDelegation) {
      accounts.put(address, new AccountEntry(before, after, afterIsDelegation));
      return this;
    }

    /**
     * Adds a candidate slot. A later call for the same slot replaces the earlier one.
     *
     * @param address the account
     * @param key the slot key
     * @param before the value in the transaction prestate
     * @param after the value as of the read
     * @return this builder
     */
    public Builder slot(
        final Address address, final UInt256 key, final UInt256 before, final UInt256 after) {
      slots
          .computeIfAbsent(address, __ -> new TreeMap<>())
          .put(key, new SlotChange(address, key, before, after));
      return this;
    }

    /**
     * Appends logs to the events table.
     *
     * @param logs the logs, in emission order
     * @return this builder
     */
    public Builder logs(final List<Log> logs) {
      for (final Log log : logs) {
        events.add(Event.of(log));
      }
      return this;
    }

    /**
     * Builds the tables, dropping every candidate whose value did not change.
     *
     * @return the diff
     */
    public TransactionDiff build() {
      final List<BalanceChange> balanceTable = new ArrayList<>();
      final List<DeployedContract> deployedTable = new ArrayList<>();
      for (final Map.Entry<Address, AccountEntry> entry : accounts.entrySet()) {
        final Address address = entry.getKey();
        final AccountFields before = entry.getValue().before();
        final AccountFields after = entry.getValue().after();
        if (!before.balance().equals(after.balance())) {
          balanceTable.add(new BalanceChange(address, before.balance(), after.balance()));
        }
        if (before.codeHash().equals(Hash.EMPTY)
            && !after.codeHash().equals(Hash.EMPTY)
            && !entry.getValue().afterIsDelegation()) {
          deployedTable.add(new DeployedContract(address, after.codeHash()));
        }
      }

      final List<SlotChange> slotTable = new ArrayList<>();
      for (final Map<UInt256, SlotChange> account : slots.values()) {
        for (final SlotChange change : account.values()) {
          if (!change.before().equals(change.after())) {
            slotTable.add(change);
          }
        }
      }

      return new TransactionDiff(
          List.copyOf(balanceTable),
          List.copyOf(slotTable),
          List.copyOf(deployedTable),
          List.copyOf(events));
    }
  }
}
