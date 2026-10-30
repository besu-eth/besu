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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration.WorldUpdaterMode;
import org.hyperledger.besu.evm.v2.worldstate.JournaledWorldUpdaterV2;
import org.hyperledger.besu.evm.worldstate.StackedUpdater;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Runs random sequences of world updater calls, in the patterns the message processors use, on a
 * transaction's stacked updaters and on the EVM v2 journal, and checks that every read agrees and
 * that both leave the transaction updater, its block access list view and the state root alike.
 */
class JournaledWorldUpdaterV2DifferentialTest {

  // the sender, which the transaction updater tracks before the call frames run
  private static final Address SENDER = Address.fromHexString("0x1000");
  private static final Address CONTRACT = Address.fromHexString("0x1001");
  private static final Address OTHER_CONTRACT = Address.fromHexString("0x1002");
  private static final Address EOA = Address.fromHexString("0x1003");
  // an existing empty account, which a failing frame force-deletes like RIPEMD-160
  private static final Address EMPTY = Address.fromHexString("0x1004");
  private static final Address ABSENT = Address.fromHexString("0x1005");
  private static final Address[] ADDRESSES = {SENDER, CONTRACT, OTHER_CONTRACT, EOA, EMPTY, ABSENT};
  private static final UInt256[] SLOTS = {
    UInt256.ZERO, UInt256.ONE, UInt256.valueOf(2), UInt256.valueOf(3)
  };
  private static final UInt256[] VALUES = {UInt256.ZERO, UInt256.ONE, UInt256.valueOf(2)};
  private static final Bytes[] CODES = {
    Bytes.EMPTY, Bytes.fromHexString("0x6001"), Bytes.fromHexString("0x60026000")
  };

  static List<Arguments> seeds() {
    final List<Arguments> seeds = new ArrayList<>();
    for (long seed = 0; seed < 1500; seed++) {
      for (final WorldUpdaterMode mode : WorldUpdaterMode.values()) {
        seeds.add(Arguments.of(seed, mode, false));
        seeds.add(Arguments.of(seed, mode, true));
      }
    }
    return seeds;
  }

  /**
   * The stacked updaters stay the reference in both modes, while the journal commits to a
   * transaction updater of the given mode, as on a node running in that mode, on a Forest or a
   * Bonsai world state.
   */
  @ParameterizedTest
  @MethodSource("seeds")
  void journalMatchesStackedUpdaters(
      final long seed, final WorldUpdaterMode mode, final boolean bonsai) {
    final Side stacked = new Side(false, WorldUpdaterMode.STACKED, bonsai);
    final Side journal = new Side(true, mode, bonsai);
    // a journaled transaction updater drops a storage clear when it commits, for v1 frames as well
    final boolean clearsStorage = mode == WorldUpdaterMode.STACKED;
    final Random random = new Random(seed);
    final int steps = 20 + random.nextInt(80);
    for (int step = 0; step < steps; step++) {
      final Function<Side, Object> op = randomOp(random, clearsStorage);
      final Object expected = op.apply(stacked);
      final Object actual = op.apply(journal);
      assertThat(actual).as("seed %s step %s", seed, step).isEqualTo(expected);
    }
    final boolean commitTransaction = random.nextInt(8) != 0;
    final List<Integer> closes = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      closes.add(random.nextInt(4));
    }
    stacked.finish(closes, commitTransaction);
    journal.finish(closes, commitTransaction);

    // a block access list needs the stacked mode, and journaled updaters track other accounts
    if (mode == WorldUpdaterMode.STACKED) {
      assertThat(journal.transactionUpdaterState())
          .as("seed %s transaction updater", seed)
          .isEqualTo(stacked.transactionUpdaterState());
      assertThat(journal.blockAccessList())
          .as("seed %s block access list", seed)
          .isEqualTo(stacked.blockAccessList());
    }
    assertThat(journal.persistedRoot())
        .as("seed %s state root", seed)
        .isEqualTo(stacked.persistedRoot());
  }

  private static Function<Side, Object> randomOp(final Random random, final boolean clearsStorage) {
    final Address address = ADDRESSES[random.nextInt(ADDRESSES.length)];
    final UInt256 slot = SLOTS[random.nextInt(SLOTS.length)];
    final UInt256 value = VALUES[random.nextInt(VALUES.length)];
    final Bytes code = CODES[random.nextInt(CODES.length)];
    final long amount = random.nextInt(3);
    final int choice = random.nextInt(100);
    if (choice < 10) {
      return side -> side.push();
    } else if (choice < 18) {
      return side -> side.close(0);
    } else if (choice < 23) {
      return side -> side.close(1);
    } else if (choice < 25) {
      return side -> side.close(2);
    } else if (choice < 28) {
      return side -> side.close(3);
    } else if (choice < 40) {
      return side -> describe(side.top().get(address), slot);
    } else if (choice < 50) {
      return side -> {
        final Account account = side.top().get(address);
        return account == null ? "absent" : account.getStorageValue(slot);
      };
    } else if (choice < 72) {
      final int mutation = random.nextInt(6);
      return side -> {
        final MutableAccount account = side.top().getAccount(address);
        if (account == null) {
          return "absent";
        }
        mutate(account, mutation, slot, value, code, amount, clearsStorage);
        return describe(account, slot);
      };
    } else if (choice < 82) {
      final int mutation = random.nextInt(6);
      return side -> {
        // An account deleted and created again within a transaction differs on purpose: the
        // journal treats it as new, while a stacked updater that tracked it before the deletion
        // keeps its tracker and earlier writes. Call frames only delete an empty RIPEMD-160.
        if (side.deleted.contains(address)) {
          return "skipped";
        }
        final MutableAccount account = side.top().getOrCreate(address);
        mutate(account, mutation, slot, value, code, amount, clearsStorage);
        return describe(account, slot);
      };
    } else if (choice < 86) {
      return side -> {
        side.top().deleteAccount(address);
        side.deleted.add(address);
        return "deleted";
      };
    } else if (choice < 92) {
      final int depth = random.nextInt(4);
      return side -> side.readOuter(depth, address);
    } else {
      return side -> touchedAndDeleted(side.top());
    }
  }

  private static void mutate(
      final MutableAccount account,
      final int mutation,
      final UInt256 slot,
      final UInt256 value,
      final Bytes code,
      final long amount,
      final boolean clearsStorage) {
    switch (mutation) {
      case 0 -> account.setBalance(Wei.of(amount));
      case 1 -> account.incrementNonce();
      case 2, 3 -> account.setStorageValue(slot, value);
      case 4 -> account.setCode(code);
      default -> {
        if (amount != 0) {
          // a write of the value the slot already holds
          account.setStorageValue(slot, account.getStorageValue(slot));
        } else if (clearsStorage) {
          account.clearStorage();
        }
      }
    }
  }

  private static String describe(final Account account, final UInt256 slot) {
    if (account == null) {
      return "absent";
    }
    return account.getAddress()
        + " nonce="
        + account.getNonce()
        + " balance="
        + account.getBalance().toShortHexString()
        + " code="
        + account.getCode()
        + " codeHash="
        + account.getCodeHash()
        + " hasCode="
        + account.hasCode()
        + " empty="
        + account.isEmpty()
        + " slot="
        + account.getStorageValue(slot)
        + " original="
        + account.getOriginalStorageValue(slot);
  }

  private static String touchedAndDeleted(final WorldUpdater updater) {
    final Set<Address> touched =
        updater.getTouchedAccounts().stream()
            .map(Account::getAddress)
            .collect(Collectors.toCollection(TreeSet::new));
    final Set<Address> deleted = new TreeSet<>(updater.getDeletedAccountAddresses());
    return "touched=" + touched + " deleted=" + deleted;
  }

  /** One world state with a transaction updater and the call frame updaters on top of it. */
  private static final class Side {
    private final MutableWorldState worldState;
    private final WorldUpdater blockUpdater;
    private final WorldUpdater transactionUpdater;
    private final WorldUpdater frameUpdater;
    private final Deque<WorldUpdater> frames = new ArrayDeque<>();
    private final Set<Address> deleted = new HashSet<>();

    Side(final boolean journaled, final WorldUpdaterMode mode, final boolean bonsai) {
      final EvmConfiguration evmConfiguration = new EvmConfiguration(32_000L, mode, true);
      worldState =
          bonsai
              ? bonsaiWorldState(evmConfiguration)
              : InMemoryKeyValueStorageProvider.createInMemoryWorldState(evmConfiguration);
      final WorldUpdater genesis = worldState.updater();
      genesis.createAccount(SENDER, 1, Wei.of(10));
      final MutableAccount contract = genesis.createAccount(CONTRACT, 1, Wei.of(2));
      contract.setCode(CODES[1]);
      contract.setStorageValue(SLOTS[0], VALUES[1]);
      contract.setStorageValue(SLOTS[1], VALUES[2]);
      final MutableAccount otherContract = genesis.createAccount(OTHER_CONTRACT, 1, Wei.ZERO);
      otherContract.setCode(CODES[2]);
      otherContract.setStorageValue(SLOTS[2], VALUES[1]);
      genesis.createAccount(EOA, 3, Wei.of(5));
      genesis.createAccount(EMPTY, 0, Wei.ZERO);
      genesis.commit();
      worldState.persist(null);

      blockUpdater = worldState.updater();
      transactionUpdater = blockUpdater.updater();
      // the transaction processor charges the sender before the call frames run
      transactionUpdater.getAccount(SENDER).incrementNonce();
      frameUpdater =
          journaled
              ? new JournaledWorldUpdaterV2(transactionUpdater)
              : transactionUpdater.updater();
      frames.push(frameUpdater.updater());
    }

    WorldUpdater top() {
      return frames.peek();
    }

    Object push() {
      frames.push(top().updater());
      return "pushed";
    }

    /**
     * Reads through a frame below the innermost one, as a tracer may between frames. The journal
     * sees the changes of the frames above, which a stacked updater does not, so the reads are not
     * compared: the frames above must stay usable.
     */
    Object readOuter(final int depth, final Address address) {
      final List<WorldUpdater> outer = new ArrayList<>(frames);
      outer.removeFirst();
      outer.add(frameUpdater);
      final WorldUpdater updater = outer.get(depth % outer.size());
      updater.get(address);
      updater.getTouchedAccounts();
      updater.getDeletedAccountAddresses();
      return "read";
    }

    /**
     * Ends the innermost frame: 0 commits, 1 reverts and commits as a failing frame does, 2
     * abandons it, 3 fails it the way the message processors do, force-deleting empty accounts.
     */
    Object close(final int how) {
      if (frames.size() < 2) {
        return "no frame";
      }
      final WorldUpdater frame = frames.pop();
      switch (how) {
        case 0 -> frame.commit();
        case 1 -> {
          frame.revert();
          frame.commit();
        }
        // the journal discards an abandoned frame at the next change through the frame below
        case 2 -> top().updater().commit();
        default -> {
          final List<Address> forceDelete =
              frame.getTouchedAccounts().stream()
                  .filter(account -> account.getAddress().equals(EMPTY) && account.isEmpty())
                  .map(Account::getAddress)
                  .toList();
          frame.revert();
          forceDelete.forEach(frame::deleteAccount);
          deleted.addAll(forceDelete);
          frame.commit();
          return "failed, force-deleted " + forceDelete;
        }
      }
      return "closed " + how;
    }

    void finish(final List<Integer> closes, final boolean commitTransaction) {
      int i = 0;
      while (frames.size() > 1) {
        close(closes.get(i++ % closes.size()));
      }
      frames.pop().commit();
      if (commitTransaction) {
        frameUpdater.commit();
      }
    }

    String transactionUpdaterState() {
      final StackedUpdater<?, ?> updater = (StackedUpdater<?, ?>) transactionUpdater;
      final Map<Address, String> tracked = new TreeMap<>();
      for (final UpdateTrackingAccount<?> account : updater.getUpdatedAccounts()) {
        tracked.put(
            account.getAddress(),
            "wrapped="
                + (account.getWrappedAccount() != null)
                + " nonce="
                + account.getNonce()
                + " balance="
                + account.getBalance().toShortHexString()
                + " codeUpdated="
                + account.codeWasUpdated()
                + " code="
                + account.getCode()
                + " cleared="
                + account.getStorageWasCleared()
                + " storage="
                + new TreeMap<>(account.getUpdatedStorage()));
      }
      return "tracked="
          + tracked
          + " deleted="
          + new TreeSet<>(updater.getDeletedAccountAddresses());
    }

    BlockAccessList blockAccessList() {
      final AccessLocationTracker tracker =
          BlockAccessListBuilder.createTransactionAccessLocationTracker(0);
      for (final Address address : ADDRESSES) {
        tracker.addTouchedAccount(address);
        for (final UInt256 slot : SLOTS) {
          tracker.addSlotAccessForAccount(address, slot);
        }
      }
      final BlockAccessListBuilder builder = BlockAccessList.builder();
      builder.apply(tracker, transactionUpdater);
      return builder.build();
    }

    private static MutableWorldState bonsaiWorldState(final EvmConfiguration evmConfiguration) {
      final BonsaiWorldStateKeyValueStorage storage =
          new BonsaiWorldStateKeyValueStorage(
              new InMemoryKeyValueStorageProvider(),
              new NoOpMetricsSystem(),
              DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
      return new BonsaiWorldState(
          storage,
          new NoOpBonsaiCachedMerkleTrieLoader(),
          new NoOpBonsaiWorldStateCacheManager(storage, evmConfiguration, new BonsaiCodeCache()),
          new NoOpTrieLogManager(),
          evmConfiguration,
          createStatefulConfigWithTrie(),
          new BonsaiCodeCache());
    }

    String persistedRoot() {
      transactionUpdater.commit();
      blockUpdater.commit();
      worldState.persist(null);
      return worldState.rootHash().toHexString();
    }
  }
}
