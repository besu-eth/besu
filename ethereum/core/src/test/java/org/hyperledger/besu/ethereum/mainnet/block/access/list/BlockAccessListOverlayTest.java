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
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.evm.account.MutableAccount;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BlockAccessListOverlayTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.ONE);

  @Test
  void usesLatestChangeBeforeMaxTxIndex() {
    final AccountChanges accountChanges =
        new AccountChanges(
            ADDRESS,
            List.of(
                new SlotChanges(
                    SLOT,
                    List.of(
                        new StorageChange(0, UInt256.valueOf(1)),
                        new StorageChange(2, UInt256.valueOf(9))))),
            List.of(),
            List.of(
                new BalanceChange(0, Wei.of(100)),
                new BalanceChange(1, Wei.of(200)),
                new BalanceChange(3, Wei.of(999))),
            List.of(new NonceChange(0, 1L), new NonceChange(2, 5L)),
            List.of(
                new CodeChange(0, Bytes.fromHexString("0xAA")),
                new CodeChange(1, Bytes.fromHexString("0xBB"))));

    final BlockAccessListOverlay overlay =
        new BlockAccessListOverlay(new BlockAccessList(List.of(accountChanges)), 2L);

    assertThat(overlay.hasPriorAccountState(ADDRESS)).isTrue();
    assertThat(overlay.getCode(ADDRESS)).contains(Bytes.fromHexString("0xBB"));
    assertThat(overlay.getStorageValue(ADDRESS, SLOT, UInt256.ZERO)).isEqualTo(UInt256.valueOf(1));
    assertThat(overlay.hasPriorStorageChange(ADDRESS, SLOT)).isTrue();

    final MutableAccount account = Mockito.mock(MutableAccount.class);
    overlay.applyToAccount(ADDRESS, account);
    Mockito.verify(account).setBalance(Wei.of(200));
    Mockito.verify(account).setNonce(1L);
    Mockito.verify(account).setCode(Bytes.fromHexString("0xBB"));
  }

  @Test
  void excludesChangesAtOrAfterMaxTxIndex() {
    final AccountChanges accountChanges =
        new AccountChanges(
            ADDRESS,
            List.of(new SlotChanges(SLOT, List.of(new StorageChange(2, UInt256.valueOf(7))))),
            List.of(),
            List.of(new BalanceChange(2, Wei.of(300))),
            List.of(new NonceChange(2, 9L)),
            List.of(new CodeChange(2, Bytes.fromHexString("0xCC"))));

    final BlockAccessListOverlay overlay =
        new BlockAccessListOverlay(new BlockAccessList(List.of(accountChanges)), 2L);

    assertThat(overlay.hasPriorAccountState(ADDRESS)).isFalse();
    assertThat(overlay.getCode(ADDRESS)).isEmpty();
    assertThat(overlay.getStorageValue(ADDRESS, SLOT, UInt256.valueOf(42)))
        .isEqualTo(UInt256.valueOf(42));
    assertThat(overlay.hasPriorStorageChange(ADDRESS, SLOT)).isFalse();
  }

  @Test
  void treatsClearedStorageAsZero() {
    final AccountChanges accountChanges =
        new AccountChanges(
            ADDRESS,
            List.of(
                new SlotChanges(
                    SLOT, List.of(new StorageChange(0, null), new StorageChange(1, null)))),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    final BlockAccessListOverlay overlay =
        new BlockAccessListOverlay(new BlockAccessList(List.of(accountChanges)), 2L);

    assertThat(overlay.getStorageValue(ADDRESS, SLOT, UInt256.valueOf(99))).isEqualTo(UInt256.ZERO);
    assertThat(overlay.hasPriorStorageChange(ADDRESS, SLOT)).isTrue();
  }
}
