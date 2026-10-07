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
import org.hyperledger.besu.datatypes.KeyHashCache;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

import java.util.List;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BlockAccessListTest {

  private static final StorageSlotKey CHANGED = new StorageSlotKey(UInt256.valueOf(1));
  private static final StorageSlotKey READ = new StorageSlotKey(UInt256.valueOf(2));

  private static final BlockAccessList BLOCK_ACCESS_LIST =
      new BlockAccessList(
          List.of(
              new AccountChanges(
                  Address.fromHexString("0x01"),
                  List.of(new SlotChanges(CHANGED, List.of(new StorageChange(1, UInt256.ONE)))),
                  List.of(new SlotRead(READ)),
                  List.of(),
                  List.of(),
                  List.of())));

  @Test
  void storageSlotKeysListsChangedAndReadSlots() {
    assertThat(BLOCK_ACCESS_LIST.storageSlotKeys()).containsExactly(CHANGED, READ);
  }

  @Test
  void slotKeysOfTheBlockReuseTheDecodedHashes() {
    final KeyHashCache keyHashes = new KeyHashCache();

    BLOCK_ACCESS_LIST.storageSlotKeys().forEach(keyHashes::add);

    assertThat(keyHashes.slotHash(CHANGED.getSlotKey().orElseThrow()))
        .isSameAs(CHANGED.getSlotHash());
    assertThat(keyHashes.slotHash(READ.getSlotKey().orElseThrow())).isSameAs(READ.getSlotHash());
  }
}
