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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Set;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Several calls at one post-execution index, each applying a view as SystemCallProcessor does, and
 * transactions at their own indexes.
 */
class AccessLocationTrackerTest {

  private static final Address CONTRACT = Address.fromHexString("0x1000");
  private static final UInt256 SLOT = UInt256.ONE;
  private static final Set<Address> ADOPTER = Set.of(CONTRACT);
  private static final Bytes INITIAL_CODE = Bytes.fromHexString("0x00");
  private static final Bytes CODE_A = Bytes.fromHexString("0x6001");
  private static final Bytes CODE_B = Bytes.fromHexString("0x6002");

  private final MutableWorldState worldState =
      InMemoryKeyValueStorageProvider.createInMemoryWorldState();
  private final AccessLocationTracker tracker =
      BlockAccessListBuilder.createPostExecutionAccessLocationTracker(0);
  private final BlockAccessListBuilder builder = BlockAccessList.builder();

  @Test
  void writesAtSharedIndexAreNettedAgainstIndexStart() {
    createContract(account -> account.setNonce(1));

    runCall(
        account -> {
          account.setStorageValue(SLOT, UInt256.ONE);
          account.incrementNonce();
        });
    runCall(
        account -> {
          account.setStorageValue(SLOT, UInt256.ZERO);
          account.incrementNonce();
        });

    final AccountChanges changes = accountChanges();
    assertThat(changes.storageChanges()).isEmpty();
    assertThat(changes.storageReads())
        .containsExactly(new BlockAccessList.SlotRead(new StorageSlotKey(SLOT)));
    assertThat(changes.nonceChanges()).containsExactly(new BlockAccessList.NonceChange(1, 3));
  }

  @Test
  void nonceDecreaseAtSharedIndexIsRecorded() {
    createContract(account -> account.setNonce(5));

    runCall(account -> account.setNonce(1));
    runCall(account -> {});

    assertThat(accountChanges().nonceChanges())
        .containsExactly(new BlockAccessList.NonceChange(1, 1));
  }

  @Test
  void deletedAccountKeepsBalanceChangeThroughLaterViews() {
    createContract(account -> account.setBalance(Wei.of(5)));

    final WorldUpdater blockUpdater = worldState.updater();
    final WorldUpdater callUpdater = blockUpdater.updater();
    tracker.addTouchedAccount(CONTRACT);
    callUpdater.deleteAccount(CONTRACT);
    builder.apply(tracker, callUpdater);
    callUpdater.commit();
    blockUpdater.commit();

    // A later call and the final flush at the same index must keep the change.
    runCall(account -> {});
    builder.apply(tracker, worldState.updater().updater());

    assertThat(accountChanges().balanceChanges())
        .containsExactly(new BlockAccessList.BalanceChange(1, Wei.ZERO));
  }

  @Test
  void adoptedCodeKeepsHashFormThroughLaterCallsAtSharedIndex() {
    createContract(account -> account.setCode(INITIAL_CODE));

    runCall(account -> account.setCode(CODE_A), ADOPTER);
    runCall(account -> {});
    builder.apply(tracker, worldState.updater().updater());

    assertThat(accountChanges().codeChanges())
        .containsExactly(CodeChange.adoptedCode(1, Hash.hash(CODE_A)));
  }

  @Test
  void plainCodeChangeAfterAdoptionAtSharedIndexRecordsBytes() {
    createContract(account -> account.setCode(INITIAL_CODE));

    runCall(account -> account.setCode(CODE_A), ADOPTER);
    runCall(account -> account.setCode(CODE_B));
    runCall(account -> {});

    assertThat(accountChanges().codeChanges()).containsExactly(new CodeChange(1, CODE_B));
  }

  @Test
  void secondAdoptionAtSharedIndexRecordsItsHash() {
    createContract(account -> account.setCode(INITIAL_CODE));

    runCall(account -> account.setCode(CODE_A), ADOPTER);
    runCall(account -> account.setCode(CODE_B), ADOPTER);

    assertThat(accountChanges().codeChanges())
        .containsExactly(CodeChange.adoptedCode(1, Hash.hash(CODE_B)));
  }

  @Test
  void adoptingIndexStartCodeAtSharedIndexRecordsNoChange() {
    createContract(account -> account.setCode(INITIAL_CODE));

    runCall(account -> account.setCode(CODE_A), ADOPTER);
    runCall(account -> account.setCode(INITIAL_CODE), ADOPTER);

    assertThat(accountChanges().codeChanges()).isEmpty();
  }

  @Test
  void adoptingCodeAnEarlierCallWroteAtSharedIndexKeepsBytes() {
    createContract(account -> account.setCode(INITIAL_CODE));

    // EELS keeps the earlier entry when a call ends with the code it started with.
    runCall(account -> account.setCode(CODE_A));
    runCall(account -> account.setCode(CODE_A), ADOPTER);

    assertThat(accountChanges().codeChanges()).containsExactly(new CodeChange(1, CODE_A));
  }

  @ParameterizedTest(name = "existing account {0}, adopter {1}")
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  void transactionRecordsHashFormOnlyForAdopter(
      final boolean existingAccount, final boolean adopter) {
    if (existingAccount) {
      createContract(account -> account.setCode(INITIAL_CODE));
    }

    runTransaction(0, account -> account.setCode(CODE_A), adopter ? ADOPTER : Set.of());

    assertThat(accountChanges().codeChanges())
        .containsExactly(
            adopter ? CodeChange.adoptedCode(1, Hash.hash(CODE_A)) : new CodeChange(1, CODE_A));
  }

  @Test
  void adoptionDoesNotCarryOverToLaterTransaction() {
    createContract(account -> account.setCode(INITIAL_CODE));

    runTransaction(0, account -> account.setCode(CODE_A), ADOPTER);
    runTransaction(1, account -> account.setCode(CODE_B), Set.of());
    runTransaction(2, account -> {}, Set.of());

    assertThat(accountChanges().codeChanges())
        .containsExactly(CodeChange.adoptedCode(1, Hash.hash(CODE_A)), new CodeChange(2, CODE_B));
  }

  private void createContract(final Consumer<MutableAccount> init) {
    final WorldUpdater updater = worldState.updater();
    init.accept(updater.getOrCreate(CONTRACT));
    updater.commit();
    worldState.persist(null);
  }

  private void runCall(final Consumer<MutableAccount> call) {
    runCall(call, Set.of());
  }

  private void runCall(final Consumer<MutableAccount> call, final Set<Address> adoptedCode) {
    final WorldUpdater blockUpdater = worldState.updater();
    final WorldUpdater callUpdater = blockUpdater.updater();
    final MutableAccount account = callUpdater.getAccount(CONTRACT);
    tracker.addSlotAccessForAccount(CONTRACT, SLOT);
    if (account != null) {
      call.accept(account);
    }
    builder.apply(tracker.createPartialBlockAccessView(callUpdater, adoptedCode));
    callUpdater.commit();
    blockUpdater.commit();
  }

  private void runTransaction(
      final int location,
      final Consumer<MutableAccount> transaction,
      final Set<Address> adoptedCode) {
    final AccessLocationTracker transactionTracker =
        BlockAccessListBuilder.createTransactionAccessLocationTracker(location);
    final WorldUpdater blockUpdater = worldState.updater();
    final WorldUpdater transactionUpdater = blockUpdater.updater();
    transactionTracker.addTouchedAccount(CONTRACT);
    transaction.accept(transactionUpdater.getOrCreate(CONTRACT));
    builder.apply(transactionTracker.createPartialBlockAccessView(transactionUpdater, adoptedCode));
    transactionUpdater.commit();
    blockUpdater.commit();
  }

  private AccountChanges accountChanges() {
    return builder.build().accountChanges().stream()
        .filter(changes -> changes.address().equals(CONTRACT))
        .findFirst()
        .orElseThrow();
  }
}
