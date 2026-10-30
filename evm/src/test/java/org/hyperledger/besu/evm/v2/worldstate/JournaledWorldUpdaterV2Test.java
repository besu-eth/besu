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
package org.hyperledger.besu.evm.v2.worldstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JournaledWorldUpdaterV2Test {

  private static final Address CONTRACT = Address.fromHexString("0x1000");
  private static final Address ABSENT = Address.fromHexString("0x2000");
  private static final UInt256 SLOT = UInt256.ONE;
  private static final UInt256 OTHER_SLOT = UInt256.valueOf(2);

  private ToyWorld transactionUpdater;
  private JournaledWorldUpdaterV2 journal;

  @BeforeEach
  void setUp() {
    final ToyWorld world = new ToyWorld();
    final MutableAccount contract = world.createAccount(CONTRACT, 1, Wei.of(10));
    contract.setStorageValue(SLOT, UInt256.valueOf(7));
    contract.setStorageValue(OTHER_SLOT, UInt256.valueOf(8));
    transactionUpdater = new ToyWorld(world);
    journal = new JournaledWorldUpdaterV2(transactionUpdater);
  }

  @Test
  void readsDoNotReachTheParentOnCommit() {
    final WorldUpdater frame = journal.updater();
    assertThat(frame.get(CONTRACT).getStorageValue(SLOT)).isEqualTo(UInt256.valueOf(7));
    assertThat(frame.get(ABSENT)).isNull();
    frame.commit();
    journal.commit();

    assertThat(transactionUpdater.getTouchedAccounts()).isEmpty();
  }

  @Test
  void nestedRevertsRestoreTheValuesTheFramesReplaced() {
    final WorldUpdater outer = journal.updater();
    outer.getAccount(CONTRACT).setStorageValue(SLOT, UInt256.valueOf(1));
    final WorldUpdater inner = outer.updater();
    inner.getAccount(CONTRACT).setStorageValue(SLOT, UInt256.valueOf(2));
    final WorldUpdater innermost = inner.updater();
    innermost.getAccount(CONTRACT).setStorageValue(SLOT, UInt256.valueOf(3));
    innermost.commit();
    assertThat(inner.get(CONTRACT).getStorageValue(SLOT)).isEqualTo(UInt256.valueOf(3));

    inner.revert();
    inner.commit();
    final Account contract = outer.get(CONTRACT);
    assertThat(contract.getStorageValue(SLOT)).isEqualTo(UInt256.valueOf(1));
    assertThat(contract.getOriginalStorageValue(SLOT)).isEqualTo(UInt256.valueOf(7));

    outer.revert();
    outer.commit();
    assertThat(journal.get(CONTRACT).getStorageValue(SLOT)).isEqualTo(UInt256.valueOf(7));
  }

  @Test
  void anAbandonedFrameIsDiscarded() {
    final WorldUpdater outer = journal.updater();
    final WorldUpdater abandoned = outer.updater();
    abandoned.getAccount(CONTRACT).setBalance(Wei.of(99));
    abandoned.createAccount(ABSENT);

    outer.commit();
    assertThat(outer.get(CONTRACT).getBalance()).isEqualTo(Wei.of(10));
    assertThat(outer.get(ABSENT)).isNull();
    journal.commit();
    assertThat(transactionUpdater.getTouchedAccounts()).isEmpty();
  }

  @Test
  void anAbandonedFrameCannotBeUsedAgain() {
    final WorldUpdater outer = journal.updater();
    final WorldUpdater abandoned = outer.updater();
    abandoned.getAccount(CONTRACT).setBalance(Wei.of(99));
    outer.getAccount(CONTRACT);

    assertThatThrownBy(() -> abandoned.getAccount(CONTRACT))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(abandoned::revert).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(abandoned::commit).isInstanceOf(IllegalStateException.class);
    assertThat(outer.get(CONTRACT).getBalance()).isEqualTo(Wei.of(10));
  }

  @Test
  void aReadThroughAnOuterFrameKeepsTheInnerFrameOpen() {
    final WorldUpdater outer = journal.updater();
    final WorldUpdater inner = outer.updater();
    // a tracer reads the calling frame after CALL created the frame it calls
    outer.get(CONTRACT);
    outer.getTouchedAccounts();
    inner.getAccount(CONTRACT).setBalance(Wei.of(99));

    inner.revert();
    inner.commit();
    assertThat(outer.get(CONTRACT).getBalance()).isEqualTo(Wei.of(10));
  }

  @Test
  void aRevertedClearRestoresTheStorage() {
    final WorldUpdater outer = journal.updater();
    outer.getAccount(CONTRACT).setStorageValue(OTHER_SLOT, UInt256.valueOf(9));
    final WorldUpdater inner = outer.updater();
    final MutableAccount cleared = inner.getAccount(CONTRACT);
    cleared.clearStorage();
    assertThat(cleared.getStorageValue(SLOT)).isEqualTo(UInt256.ZERO);
    assertThat(cleared.getOriginalStorageValue(SLOT)).isEqualTo(UInt256.ZERO);

    inner.revert();
    inner.commit();
    final Account contract = outer.get(CONTRACT);
    assertThat(contract.getStorageValue(SLOT)).isEqualTo(UInt256.valueOf(7));
    assertThat(contract.getStorageValue(OTHER_SLOT)).isEqualTo(UInt256.valueOf(9));
  }

  @Test
  void createdAndDeletedAccountsComeBackOnRevert() {
    final WorldUpdater frame = journal.updater();
    frame.getOrCreate(ABSENT).setStorageValue(SLOT, UInt256.ONE);
    frame.deleteAccount(CONTRACT);
    assertThat(frame.get(ABSENT).getStorageValue(SLOT)).isEqualTo(UInt256.ONE);
    assertThat(frame.get(CONTRACT)).isNull();
    assertThat(frame.getDeletedAccountAddresses()).containsExactly(CONTRACT);

    frame.revert();
    frame.commit();
    assertThat(journal.get(ABSENT)).isNull();
    assertThat(journal.get(CONTRACT).getStorageValue(SLOT)).isEqualTo(UInt256.valueOf(7));
    assertThat(journal.getDeletedAccountAddresses()).isEmpty();
  }

  @Test
  void aCreatedAccountStartsWithEmptyStorage() {
    final WorldUpdater frame = journal.updater();
    frame.deleteAccount(CONTRACT);
    final MutableAccount recreated = frame.createAccount(CONTRACT);
    assertThat(recreated.getStorageValue(SLOT)).isEqualTo(UInt256.ZERO);
    assertThat(recreated.getOriginalStorageValue(SLOT)).isEqualTo(UInt256.ZERO);
    assertThat(recreated.getNonce()).isZero();
    assertThat(recreated.hasCode()).isFalse();
  }

  @Test
  void touchedAccountsAreThoseOfTheFrameAndTheFramesItCommitted() {
    final WorldUpdater outer = journal.updater();
    final WorldUpdater inner = outer.updater();
    inner.getAccount(CONTRACT);
    assertThat(inner.getTouchedAccounts())
        .extracting(Account::getAddress)
        .containsExactly(CONTRACT);
    inner.commit();
    assertThat(outer.getTouchedAccounts())
        .extracting(Account::getAddress)
        .containsExactly(CONTRACT);

    final WorldUpdater reverted = outer.updater();
    reverted.getAccount(CONTRACT);
    assertThat(reverted.getTouchedAccounts()).hasSize(1);
    reverted.revert();
    assertThat(reverted.getTouchedAccounts()).isEmpty();
    reverted.commit();
    // the outer frame still touched it before
    assertThat(outer.getTouchedAccounts()).hasSize(1);
  }

  @Test
  void commitWritesTheTransactionsChangesToTheParent() {
    final WorldUpdater frame = journal.updater();
    final MutableAccount contract = frame.getAccount(CONTRACT);
    contract.incrementNonce();
    contract.setStorageValue(SLOT, UInt256.valueOf(7));
    contract.setStorageValue(OTHER_SLOT, UInt256.ZERO);
    contract.setCode(Bytes.fromHexString("0x6001"));
    frame.getOrCreate(ABSENT).setBalance(Wei.of(3));
    frame.commit();
    journal.commit();

    final Account committed = transactionUpdater.get(CONTRACT);
    assertThat(committed.getNonce()).isEqualTo(2);
    assertThat(committed.getCode()).isEqualTo(Bytes.fromHexString("0x6001"));
    // a write of the value the slot already held is still a write
    assertThat(((MutableAccount) committed).getUpdatedStorage())
        .containsEntry(SLOT, UInt256.valueOf(7))
        .containsEntry(OTHER_SLOT, UInt256.ZERO);
    assertThat(transactionUpdater.get(ABSENT).getBalance()).isEqualTo(Wei.of(3));
  }

  @Test
  void commitSetsTheNonceAndBalanceOfACreatedAccountTheParentHandsBack() {
    // the Bonsai accumulator hands back an empty account it still tracks, without the values
    final WorldUpdater parent = mock(WorldUpdater.class);
    final MutableAccount trackedEmptyAccount = mock(MutableAccount.class);
    when(parent.createAccount(ABSENT, 1, Wei.of(5))).thenReturn(trackedEmptyAccount);
    final JournaledWorldUpdaterV2 journal = new JournaledWorldUpdaterV2(parent);
    final WorldUpdater frame = journal.updater();
    frame.createAccount(ABSENT, 1, Wei.of(5));
    frame.commit();
    journal.commit();

    verify(trackedEmptyAccount).setNonce(1);
    verify(trackedEmptyAccount).setBalance(Wei.of(5));
  }

  @Test
  void aTransactionThatIsNotCommittedLeavesTheParentAlone() {
    final WorldUpdater frame = journal.updater();
    frame.getAccount(CONTRACT).setBalance(Wei.ZERO);
    frame.commit();

    assertThat(transactionUpdater.getTouchedAccounts()).isEmpty();
    assertThat(transactionUpdater.get(CONTRACT).getBalance()).isEqualTo(Wei.of(10));
  }

  @Test
  void aValueWrittenBeforeARevertedClearComesBack() {
    final WorldUpdater outer = journal.updater();
    outer.getAccount(CONTRACT).setStorageValue(SLOT, UInt256.valueOf(5));
    final WorldUpdater inner = outer.updater();
    final MutableAccount cleared = inner.getAccount(CONTRACT);
    cleared.clearStorage();
    assertThat(cleared.getStorageValue(SLOT)).isEqualTo(UInt256.ZERO);
    cleared.setStorageValue(OTHER_SLOT, UInt256.ONE);

    inner.revert();
    inner.commit();
    final MutableAccount contract = outer.getAccount(CONTRACT);
    assertThat(contract.getStorageValue(SLOT)).isEqualTo(UInt256.valueOf(5));
    assertThat(contract.getStorageValue(OTHER_SLOT)).isEqualTo(UInt256.valueOf(8));
    assertThat(contract.getUpdatedStorage()).containsOnlyKeys(SLOT);
  }

  @Test
  void aRevertedFrameUndoesItsWarmUps() {
    final JournalAccountV2 contract = (JournalAccountV2) journal.getAccount(CONTRACT);
    final WorldUpdater outer = journal.updater();
    assertThat(contract.warmUpStorage(SLOT)).isFalse();
    final WorldUpdater inner = outer.updater();
    assertThat(contract.warmUpStorage(SLOT)).isTrue();
    assertThat(contract.warmUpStorage(OTHER_SLOT)).isFalse();

    inner.revert();
    inner.commit();
    assertThat(contract.warmUpStorage(SLOT)).isTrue();
    assertThat(contract.warmUpStorage(OTHER_SLOT)).isFalse();
    outer.revert();
    assertThat(contract.warmUpStorage(SLOT)).isFalse();
  }

  @Test
  void warmSlotsStayWarmWhenTheStorageIsClearedOrTheAccountCreated() {
    journal.warmUpStorage(ABSENT, SLOT);
    final WorldUpdater frame = journal.updater();
    final JournalAccountV2 contract = (JournalAccountV2) frame.getAccount(CONTRACT);
    assertThat(contract.warmUpStorage(SLOT)).isFalse();
    contract.clearStorage();
    assertThat(contract.warmUpStorage(SLOT)).isTrue();

    final JournalAccountV2 created = (JournalAccountV2) frame.createAccount(ABSENT);
    assertThat(created.warmUpStorage(SLOT)).isTrue();
    assertThat(journal.getWarmedUpStorage().rowKeySet()).containsExactly(CONTRACT, ABSENT);
  }

  @Test
  void theAccessListLoadsAnAccountOnlyWhenTheTransactionLooksItUp() {
    final WorldUpdater parent = spy(transactionUpdater);
    final JournaledWorldUpdaterV2 journal = new JournaledWorldUpdaterV2(parent);
    assertThat(journal.warmUpStorage(CONTRACT, SLOT)).isFalse();
    assertThat(journal.warmUpStorage(CONTRACT, SLOT)).isTrue();
    assertThat(journal.getWarmedUpStorage().row(CONTRACT)).containsOnlyKeys(SLOT);
    verify(parent, never()).get(any());

    final WorldUpdater frame = journal.updater();
    final JournalAccountV2 contract = (JournalAccountV2) frame.getAccount(CONTRACT);
    verify(parent).get(CONTRACT);
    frame.revert();
    frame.commit();
    // the access list's warm-up is not the frame's, so its revert keeps the slot warm
    assertThat(contract.warmUpStorage(SLOT)).isTrue();
  }

  @Test
  void theAccessListWarmsSlotsOfAccountsThatDoNotExist() {
    assertThat(journal.warmUpStorage(ABSENT, SLOT)).isFalse();
    assertThat(journal.warmUpStorage(ABSENT, SLOT)).isTrue();
    assertThat(journal.get(ABSENT)).isNull();
    journal.commit();
    assertThat(transactionUpdater.getTouchedAccounts()).isEmpty();
  }
}
