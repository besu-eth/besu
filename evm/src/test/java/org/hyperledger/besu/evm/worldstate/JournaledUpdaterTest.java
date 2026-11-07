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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.fluent.SimpleAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JournaledUpdaterTest {

  private static final Address ADDRESS = Address.fromHexString("0x1234");
  private static final UInt256 SLOT_1 = UInt256.ONE;
  private static final UInt256 SLOT_2 = UInt256.valueOf(2);
  private static final UInt256 STORED = UInt256.valueOf(5);

  private final RootUpdater root = new RootUpdater();
  private WorldUpdater transaction;

  @BeforeEach
  void setUp() {
    final SimpleAccount account = new SimpleAccount(ADDRESS, 0, Wei.ONE);
    account.setStorageValue(SLOT_1, STORED);
    account.setStorageValue(SLOT_2, STORED);
    root.stored.put(ADDRESS, account);
    transaction = new JournaledUpdater<>(root, EvmConfiguration.DEFAULT);
  }

  @Test
  void commitKeepsAStorageClearMadeInAFrame() {
    final WorldUpdater frame = transaction.updater();
    final MutableAccount account = frame.getAccount(ADDRESS);
    account.clearStorage();
    account.setStorageValue(SLOT_1, UInt256.ONE);
    frame.commit();
    transaction.commit();

    assertThat(root.get(ADDRESS).getStorageValue(SLOT_1)).isEqualTo(UInt256.ONE);
    assertThat(root.get(ADDRESS).getStorageValue(SLOT_2)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void revertingAFrameUndoesItsStorageClear() {
    // loaded before the frame, so the revert keeps the account in the journal
    final MutableAccount account = transaction.getAccount(ADDRESS);
    final WorldUpdater frame = transaction.updater();
    frame.getAccount(ADDRESS).clearStorage();
    frame.revert();

    assertThat(account.getStorageValue(SLOT_2)).isEqualTo(STORED);
    assertThat(account.getOriginalStorageValue(SLOT_2)).isEqualTo(STORED);

    transaction.commit();
    assertThat(root.get(ADDRESS).getStorageValue(SLOT_2)).isEqualTo(STORED);
  }

  @Test
  void anAccountCreatedAfterItWasDeletedSurvivesTheCommit() {
    // a failed frame force-deletes an empty account only after reverting, so the journal no longer
    // holds it and the next frame has to create it
    final WorldUpdater failedFrame = transaction.updater();
    failedFrame.getAccount(ADDRESS);
    failedFrame.revert();
    failedFrame.deleteAccount(ADDRESS);
    failedFrame.commit();

    final WorldUpdater frame = transaction.updater();
    frame.getOrCreate(ADDRESS).incrementBalance(Wei.ONE);
    frame.commit();
    transaction.commit();

    final Account account = root.get(ADDRESS);
    assertThat(account).isNotNull();
    assertThat(account.getBalance()).isEqualTo(Wei.ONE);
    assertThat(account.getStorageValue(SLOT_1)).isEqualTo(UInt256.ZERO);
  }

  /** A block-level updater over stored accounts, in place of a world state. */
  private static final class RootUpdater extends AbstractWorldUpdater<WorldView, MutableAccount> {
    private final Map<Address, MutableAccount> stored = new HashMap<>();

    RootUpdater() {
      super(null, EvmConfiguration.DEFAULT);
    }

    @Override
    protected MutableAccount getForMutation(final Address address) {
      return stored.get(address);
    }

    @Override
    public Collection<? extends Account> getTouchedAccounts() {
      return getUpdatedAccounts();
    }

    @Override
    public Collection<Address> getDeletedAccountAddresses() {
      return getDeletedAccounts();
    }

    @Override
    public void revert() {
      reset();
    }

    @Override
    public void commit() {
      throw new UnsupportedOperationException();
    }
  }
}
