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
import static org.assertj.core.groups.Tuple.tuple;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.TransactionDiff;
import org.hyperledger.besu.evm.frame.TransactionDiff.BalanceChange;
import org.hyperledger.besu.evm.frame.TransactionDiff.DeployedContract;
import org.hyperledger.besu.evm.frame.TransactionDiff.SlotChange;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@link TransactionDiff#compute} over a real Bonsai world and the updater layers the frame
 * transaction processor builds: the block updater, the transaction prestate, the transaction
 * updater holding the validation prefix's writes, and the execution body's layer above it.
 */
class TransactionDiffComputeTest {

  private static final Address PAYER =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address WRITER =
      Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final Address RECIPIENT =
      Address.fromHexString("0x3000000000000000000000000000000000000003");
  private static final Address READ_ONLY =
      Address.fromHexString("0x4000000000000000000000000000000000000004");
  private static final Address CREATED =
      Address.fromHexString("0x5000000000000000000000000000000000000005");
  private static final Address STORAGE_ONLY =
      Address.fromHexString("0x6000000000000000000000000000000000000006");
  private static final Address ENTRY_POINT = Address.fromHexString("0xaa");

  private static final Bytes WRITER_CODE = Bytes.fromHexString("0x60015f5500");

  private MutableWorldState worldState;
  private WorldUpdater blockUpdater;

  @BeforeEach
  void setUp() {
    worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final WorldUpdater genesis = worldState.updater();
    genesis.createAccount(PAYER, 0, Wei.of(1_000_000));
    final MutableAccount writer = genesis.createAccount(WRITER, 1, Wei.ZERO);
    writer.setCode(WRITER_CODE);
    writer.setStorageValue(UInt256.valueOf(0xb), UInt256.valueOf(5));
    writer.setStorageValue(UInt256.valueOf(0xd), UInt256.valueOf(3));
    genesis.createAccount(RECIPIENT, 0, Wei.of(1_000));
    genesis.createAccount(READ_ONLY, 0, Wei.of(77));
    final MutableAccount storageOnly = genesis.createAccount(STORAGE_ONLY, 0, Wei.ZERO);
    storageOnly.setStorageValue(UInt256.ONE, UInt256.valueOf(5));
    storageOnly.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(9));
    genesis.commit();
    blockUpdater = worldState.updater();
  }

  /** Runs {@code writes} in a frame updater committed into {@code parent}, as a frame does. */
  private static void frame(final WorldUpdater parent, final Consumer<WorldUpdater> writes) {
    final WorldUpdater frame = parent.updater();
    writes.accept(frame);
    frame.commit();
  }

  @Test
  void enumeratesThePrefixAndTheBodyAgainstTheTransactionPrestate() {
    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    // validation prefix: the gas escrow and the nonce
    frame(
        transaction,
        u -> {
          final MutableAccount payer = u.getAccount(PAYER);
          payer.decrementBalance(Wei.of(1_000));
          payer.incrementNonce();
        });
    final WorldUpdater body = transaction.updater();
    frame(
        body,
        u -> {
          u.getAccount(PAYER).decrementBalance(Wei.of(7));
          u.getAccount(RECIPIENT).incrementBalance(Wei.of(7));
          final MutableAccount writer = u.getAccount(WRITER);
          writer.setStorageValue(UInt256.valueOf(0xc), UInt256.valueOf(9));
          writer.setStorageValue(UInt256.valueOf(0xb), UInt256.valueOf(7));
          writer.setStorageValue(UInt256.valueOf(0xa), UInt256.ONE);
          writer.setStorageValue(UInt256.valueOf(0xc), UInt256.ZERO);
        });

    final TransactionDiff diff =
        TransactionDiff.compute(prestate, body, List.of(transaction, body), List.of());

    assertThat(diff.balances())
        .containsExactly(
            new BalanceChange(PAYER, Wei.of(1_000_000), Wei.of(1_000_000 - 1_000 - 7)),
            new BalanceChange(RECIPIENT, Wei.of(1_000), Wei.of(1_007)));
    assertThat(diff.slots())
        .containsExactly(
            new SlotChange(WRITER, UInt256.valueOf(0xa), UInt256.ZERO, UInt256.ONE),
            new SlotChange(WRITER, UInt256.valueOf(0xb), UInt256.valueOf(5), UInt256.valueOf(7)));
    assertThat(diff.deployed()).isEmpty();
    assertThat(diff.accountChangeFlags(PAYER))
        .isEqualTo(TransactionDiff.FLAG_NONCE | TransactionDiff.FLAG_BALANCE);
    assertThat(diff.accountChangeFlags(WRITER)).isEqualTo(TransactionDiff.FLAG_STORAGE);
  }

  @Test
  void aSlotWrittenInBothLayersIsOneEntryWithThePrestateBefore() {
    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    frame(
        transaction,
        u -> u.getAccount(WRITER).setStorageValue(UInt256.valueOf(0xb), UInt256.valueOf(6)));
    final WorldUpdater body = transaction.updater();
    frame(
        body, u -> u.getAccount(WRITER).setStorageValue(UInt256.valueOf(0xb), UInt256.valueOf(8)));

    final TransactionDiff diff =
        TransactionDiff.compute(prestate, body, List.of(transaction, body), List.of());

    assertThat(diff.slots())
        .containsExactly(
            new SlotChange(WRITER, UInt256.valueOf(0xb), UInt256.valueOf(5), UInt256.valueOf(8)));
  }

  @Test
  void valueNeutralTrackersCreateNoEntry() {
    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    final WorldUpdater body = transaction.updater();
    frame(
        body,
        u -> {
          // a static frame's zero-value call: the caller and the target get trackers
          u.getOrCreate(ENTRY_POINT);
          u.getOrCreate(READ_ONLY);
          u.getAccount(WRITER);
          // a write that restores the prestate value
          u.getAccount(WRITER).setStorageValue(UInt256.valueOf(0xd), UInt256.valueOf(3));
        });
    assertThat(body.getTouchedAccounts()).isNotEmpty();

    final TransactionDiff diff =
        TransactionDiff.compute(prestate, body, List.of(transaction, body), List.of());

    assertThat(diff.balances()).isEmpty();
    assertThat(diff.slots()).isEmpty();
    assertThat(diff.deployed()).isEmpty();
    assertThat(diff.accountChangeFlags(ENTRY_POINT)).isZero();
    assertThat(diff.accountChangeFlags(READ_ONLY)).isZero();
    assertThat(diff.accountChangeFlags(WRITER)).isZero();
  }

  @Test
  void aCreatedAccountIsDeployedAndItsSlotsStartFromZero() {
    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    final WorldUpdater body = transaction.updater();
    frame(
        body,
        u -> {
          final MutableAccount created = u.getOrCreate(CREATED);
          created.setNonce(1);
          created.clearStorage();
          created.setStorageValue(UInt256.ONE, UInt256.valueOf(4));
          created.setCode(Bytes.of(0x00));
        });

    final TransactionDiff diff =
        TransactionDiff.compute(prestate, body, List.of(transaction, body), List.of());

    assertThat(diff.deployed())
        .containsExactly(new DeployedContract(CREATED, Hash.hash(Bytes.of(0x00))));
    assertThat(diff.slots())
        .containsExactly(new SlotChange(CREATED, UInt256.ONE, UInt256.ZERO, UInt256.valueOf(4)));
    assertThat(diff.accountChangeFlags(CREATED))
        .isEqualTo(
            TransactionDiff.FLAG_NONCE | TransactionDiff.FLAG_CODE | TransactionDiff.FLAG_STORAGE);
  }

  @Test
  void aDelegationDesignatorIsNotADeployment() {
    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    final Bytes designator = Bytes.fromHexString("0xef0100" + "11".repeat(20));
    frame(transaction, u -> u.getAccount(RECIPIENT).setCode(designator));

    final TransactionDiff diff =
        TransactionDiff.compute(prestate, transaction, List.of(transaction), List.of());

    assertThat(diff.deployed()).isEmpty();
    assertThat(diff.accountChangeFlags(RECIPIENT)).isEqualTo(TransactionDiff.FLAG_CODE);
  }

  @Test
  void aDeletedAccountReadsAsEmptyAfterwards() {
    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    frame(
        transaction, u -> u.getAccount(WRITER).setStorageValue(UInt256.valueOf(0xa), UInt256.ONE));
    final WorldUpdater body = transaction.updater();
    frame(body, u -> u.deleteAccount(WRITER));

    final TransactionDiff diff =
        TransactionDiff.compute(prestate, body, List.of(transaction, body), List.of());

    // the deletion wipes what the prefix wrote, so the slot is not enumerated
    assertThat(diff.slots()).isEmpty();
    assertThat(diff.accountChangeFlags(WRITER))
        .isEqualTo(TransactionDiff.FLAG_NONCE | TransactionDiff.FLAG_CODE);
  }

  /**
   * EELS and Besu both allow CREATE over an account that holds only storage, which wipes it. The
   * EIP-7906 fixtures skip this case until EIP-8253, so this pins the EELS semantics: the slot the
   * creation wrote again reports its true prestate value, a wiped slot that nothing wrote again is
   * not enumerated, and neither is a slot the prefix wrote before the wipe. geth would report 0 as
   * the before value; no storage-only account exists on bogota-devnet-1.
   */
  @Test
  void aCreationOverStorageFollowsTheExecutionSpecs() {
    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    frame(
        transaction,
        u -> u.getAccount(STORAGE_ONLY).setStorageValue(UInt256.valueOf(3), UInt256.valueOf(8)));
    final WorldUpdater body = transaction.updater();
    frame(
        body,
        u -> {
          final MutableAccount created = u.getOrCreate(STORAGE_ONLY);
          created.setNonce(1);
          created.clearStorage();
          created.setStorageValue(UInt256.ONE, UInt256.valueOf(7));
        });

    final TransactionDiff diff =
        TransactionDiff.compute(prestate, body, List.of(transaction, body), List.of());

    assertThat(diff.slots())
        .containsExactly(
            new SlotChange(STORAGE_ONLY, UInt256.ONE, UInt256.valueOf(5), UInt256.valueOf(7)));
    // a keyed lookup still sees the wiped slot: its prestate value, and 0 now
    assertThat(prestate.get(STORAGE_ONLY).getStorageValue(UInt256.valueOf(2)))
        .isEqualTo(UInt256.valueOf(9));
    assertThat(body.get(STORAGE_ONLY).getStorageValue(UInt256.valueOf(2))).isEqualTo(UInt256.ZERO);
  }

  @Test
  void thePrestateIncludesEarlierTransactionsOfTheBlock() {
    final WorldUpdater earlier = blockUpdater.updater();
    earlier.getAccount(RECIPIENT).incrementBalance(Wei.of(50));
    earlier.getAccount(WRITER).setStorageValue(UInt256.valueOf(0xb), UInt256.valueOf(6));
    earlier.commit();

    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    frame(
        transaction,
        u -> {
          u.getAccount(RECIPIENT).incrementBalance(Wei.of(1));
          u.getAccount(WRITER).setStorageValue(UInt256.valueOf(0xb), UInt256.valueOf(7));
        });

    final TransactionDiff diff =
        TransactionDiff.compute(prestate, transaction, List.of(transaction), List.of());

    assertThat(diff.balances())
        .containsExactly(new BalanceChange(RECIPIENT, Wei.of(1_050), Wei.of(1_051)));
    assertThat(diff.slots())
        .extracting(SlotChange::key, SlotChange::before, SlotChange::after)
        .containsExactly(tuple(UInt256.valueOf(0xb), UInt256.valueOf(6), UInt256.valueOf(7)));
  }

  @Test
  void eventsAreTheGivenLogsInOrder() {
    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    final Log first =
        new Log(WRITER, Bytes.of(1, 2), List.of(LogTopic.create(Bytes32.leftPad(Bytes.of(1)))));
    final Log second = new Log(PAYER, Bytes.EMPTY, List.of());

    final TransactionDiff diff =
        TransactionDiff.compute(
            prestate, transaction, List.of(transaction), List.of(first, second));

    assertThat(diff.events())
        .extracting(TransactionDiff.Event::logger)
        .containsExactly(WRITER, PAYER);
    assertThat(diff.events().get(0).data()).isEqualTo(Bytes.of(1, 2));
  }

  /**
   * The block access list is built from the transaction updater's tracked accounts, so computing
   * the diff must not add any: it only reads through {@code get}.
   */
  @Test
  void computingTheDiffTracksNothing() {
    final WorldUpdater prestate = blockUpdater.updater();
    final WorldUpdater transaction = prestate.updater();
    frame(transaction, u -> u.getAccount(PAYER).decrementBalance(Wei.of(1_000)));
    final WorldUpdater body = transaction.updater();
    frame(body, u -> u.getAccount(WRITER).setStorageValue(UInt256.valueOf(0xa), UInt256.ONE));
    final List<WorldUpdater> layers = List.of(prestate, transaction, body);
    final List<Set<String>> before =
        layers.stream().map(TransactionDiffComputeTest::tracked).toList();

    TransactionDiff.compute(prestate, body, List.of(transaction, body), List.of());

    assertThat(layers.stream().map(TransactionDiffComputeTest::tracked).toList()).isEqualTo(before);
    assertThat(tracked(prestate)).isEmpty();
  }

  private static Set<String> tracked(final WorldUpdater updater) {
    final Set<String> tracked = new TreeSet<>();
    for (final Account account : updater.getTouchedAccounts()) {
      tracked.add(account.getAddress().toHexString());
    }
    for (final Address deleted : updater.getDeletedAccountAddresses()) {
      tracked.add("deleted " + deleted.toHexString());
    }
    return tracked;
  }
}
