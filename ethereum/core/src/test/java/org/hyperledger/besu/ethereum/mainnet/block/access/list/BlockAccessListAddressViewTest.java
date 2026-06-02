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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

import java.util.List;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BlockAccessListAddressViewTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.ONE);

  @Test
  void looksUpAccountAndSlotByAddress() {
    final Address other = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ADDRESS,
                    List.of(new SlotChanges(SLOT, List.of(new StorageChange(0, UInt256.ONE)))),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of()),
                new AccountChanges(other, List.of(), List.of(), List.of(), List.of(), List.of())));

    final BlockAccessListAddressView addressView = BlockAccessListAddressView.of(bal);

    assertThat(addressView.getAccountChanges(ADDRESS)).isPresent();
    assertThat(addressView.getAccountChanges(other)).isPresent();
    assertThat(addressView.getSlotChanges(ADDRESS, SLOT).map(BlockAccessList.SlotChanges::slot))
        .contains(SLOT);
    assertThat(addressView.getSlotChanges(other, SLOT)).isEmpty();
  }
}
