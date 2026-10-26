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
package org.hyperledger.besu.evm.worldstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.collections.undo.Undoable;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.MutableAccount;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class JournaledAccountTest {

  // PUSH1 4, JUMP, STOP, JUMPDEST, STOP
  private static final Bytes CODE = Bytes.fromHexString("0x600456005b00");
  // PUSH1 5, JUMP, STOP, STOP, JUMPDEST, STOP
  private static final Bytes OTHER_CODE = Bytes.fromHexString("0x60055600005b00");

  @Test
  void settingTheSameCodeIsNotAnUpdateWhenTheCodeIsNotLoaded() {
    final MutableAccount wrapped = contract(CODE);
    final JournaledAccount account = new JournaledAccount(wrapped);

    account.setCode(CODE);
    assertThat(account.codeWasUpdated()).isFalse();

    account.commit();
    verify(wrapped, never()).setCode(any());
    verify(wrapped, never()).getCode();
  }

  @Test
  void settingOtherCodeIsAnUpdateWhenTheCodeIsNotLoaded() {
    final MutableAccount wrapped = contract(CODE);
    final JournaledAccount account = new JournaledAccount(wrapped);

    account.setCode(OTHER_CODE);
    assertThat(account.codeWasUpdated()).isTrue();
    assertThat(account.getCodeHash()).isEqualTo(Hash.hash(OTHER_CODE));

    account.commit();
    verify(wrapped).setCode(OTHER_CODE);
  }

  @Test
  void readsTheCodeOfTheWrappedAccountOnlyWhenItIsRequested() {
    final MutableAccount wrapped = contract(CODE);
    final JournaledAccount account = new JournaledAccount(wrapped);

    assertThat(account.hasCode()).isTrue();
    verify(wrapped, never()).getCode();

    assertThat(account.getCode()).isEqualTo(CODE);
    verify(wrapped).getCode();
  }

  @Test
  void undoingSetCodeGoesBackToTheCodeOfTheWrappedAccount() {
    final MutableAccount wrapped = contract(CODE);
    final JournaledAccount account = new JournaledAccount(wrapped);
    final long mark = account.mark();

    account.setCode(OTHER_CODE);
    assertThat(account.getCode()).isEqualTo(OTHER_CODE);

    account.undo(mark);
    assertThat(account.codeWasUpdated()).isFalse();
    assertThat(account.getCodeHash()).isEqualTo(Hash.hash(CODE));
    assertThat(account.getCode()).isEqualTo(CODE);

    account.commit();
    verify(wrapped, never()).setCode(any());
  }

  @Test
  void isNotUpdatedWhenCreatedWithTheMarkAtZero() {
    final long mark = Undoable.markState.get();
    // as in a fresh JVM, where nothing has advanced the mark yet
    Undoable.markState.set(0);
    try {
      final JournaledAccount account = new JournaledAccount(contract(CODE));

      assertThat(account.codeWasUpdated()).isFalse();
    } finally {
      Undoable.markState.set(mark);
    }
  }

  private static MutableAccount contract(final Bytes code) {
    final MutableAccount account = mock(MutableAccount.class);
    when(account.getAddress()).thenReturn(Address.fromHexString("0x1234"));
    when(account.getBalance()).thenReturn(Wei.ZERO);
    when(account.getCodeHash()).thenReturn(Hash.hash(code));
    when(account.getCode()).thenReturn(code);
    return account;
  }
}
