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
import org.hyperledger.besu.datatypes.KeyHashCache;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.function.Consumer;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Several calls at one post-execution index, each applying a view as SystemCallProcessor does. */
class AccessLocationTrackerTest {

  private static final Address CONTRACT = Address.fromHexString("0x1000");
  private static final UInt256 SLOT = UInt256.ONE;

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
  void keyHashesReuseTheHashesOfTheBlock() {
    final KeyHashCache blockKeyHashes = new KeyHashCache();
    final Hash known = blockKeyHashes.slotHash(SLOT);
    final AccessLocationTracker txTracker =
        BlockAccessListBuilder.createTransactionAccessLocationTracker(0, blockKeyHashes);
    txTracker.addSlotAccessForAccount(CONTRACT, SLOT);

    final PartialBlockAccessView view =
        txTracker.createPartialBlockAccessView(worldState.updater().updater());

    final StorageSlotKey read = view.accountChanges().getFirst().getStorageReads().getFirst();
    assertThat(read).isEqualTo(new StorageSlotKey(SLOT));
    // same instance: the tracker reused the block's hash instead of computing it again
    assertThat(read.getSlotHash()).isSameAs(known);
  }

  private void createContract(final Consumer<MutableAccount> init) {
    final WorldUpdater updater = worldState.updater();
    init.accept(updater.getOrCreate(CONTRACT));
    updater.commit();
    worldState.persist(null);
  }

  private void runCall(final Consumer<MutableAccount> call) {
    final WorldUpdater blockUpdater = worldState.updater();
    final WorldUpdater callUpdater = blockUpdater.updater();
    final MutableAccount account = callUpdater.getAccount(CONTRACT);
    tracker.addSlotAccessForAccount(CONTRACT, SLOT);
    if (account != null) {
      call.accept(account);
    }
    builder.apply(tracker, callUpdater);
    callUpdater.commit();
    blockUpdater.commit();
  }

  private AccountChanges accountChanges() {
    return builder.build().accountChanges().stream()
        .filter(changes -> changes.address().equals(CONTRACT))
        .findFirst()
        .orElseThrow();
  }
}
