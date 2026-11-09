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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.mainnet.block.access.list.RecordingPreBlockCodeReader.noPreBlockCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.evm.account.MutableAccount;

import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BlockAccessListOverlayTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address ADOPTER =
      Address.fromHexString("0x00000000000000000000000000000000000000ad");
  private static final Address SOURCE =
      Address.fromHexString("0x00000000000000000000000000000000000000c0");
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
        new BlockAccessListOverlay(
            BlockAccessListAccountLookup.of(new BlockAccessList(List.of(accountChanges))), 2L);

    final BonsaiValue<UInt256> storageValue =
        new BonsaiValue<>(UInt256.valueOf(99), UInt256.valueOf(99));
    overlay.applyToStorage(ADDRESS, SLOT, storageValue::setUpdated);
    assertThat(storageValue.getUpdated()).isEqualTo(UInt256.valueOf(1));

    final BonsaiValue<Bytes> codeValue = new BonsaiValue<>(Bytes.EMPTY, Bytes.EMPTY);
    overlay.applyToCode(ADDRESS, noPreBlockCode(), codeValue::setUpdated);
    assertThat(codeValue.getUpdated()).isEqualTo(Bytes.fromHexString("0xBB"));

    final MutableAccount account = mock(MutableAccount.class);
    assertThat(overlay.applyToAccountState(ADDRESS, () -> account)).contains(account);
    verify(account).setBalance(Wei.of(200));
    verify(account).setNonce(1L);
    verify(account).setCode(eq(Bytes.fromHexString("0xBB")));
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
        new BlockAccessListOverlay(
            BlockAccessListAccountLookup.of(new BlockAccessList(List.of(accountChanges))), 2L);

    final BonsaiValue<UInt256> storageValue =
        new BonsaiValue<>(UInt256.valueOf(42), UInt256.valueOf(42));
    overlay.applyToStorage(ADDRESS, SLOT, storageValue::setUpdated);
    assertThat(storageValue.getUpdated()).isEqualTo(UInt256.valueOf(42));

    final BonsaiValue<Bytes> codeValue = new BonsaiValue<>(Bytes.EMPTY, Bytes.EMPTY);
    overlay.applyToCode(ADDRESS, noPreBlockCode(), codeValue::setUpdated);
    assertThat(codeValue.getUpdated()).isEqualTo(Bytes.EMPTY);

    final MutableAccount account = mock(MutableAccount.class);
    assertThat(overlay.applyToAccountState(ADDRESS, () -> account)).isEmpty();
    verify(account, never()).setBalance(any());
    verify(account, never()).setCode(any(Bytes.class));
  }

  @Test
  void resolvesHashFormCodeFromAnotherAccountsCodeChange() {
    final Bytes adoptedCode = Bytes.fromHexString("0xBB");
    final BlockAccessListAccountLookup lookup =
        BlockAccessListAccountLookup.of(
            new BlockAccessList(
                List.of(
                    codeChanges(ADDRESS, new CodeChange(0, Bytes.fromHexString("0xAA"))),
                    codeChanges(
                        ADOPTER,
                        new CodeChange(0, Bytes.fromHexString("0xAA")),
                        CodeChange.adoptedCode(2, Hash.hash(adoptedCode))),
                    codeChanges(SOURCE, new CodeChange(1, adoptedCode)))));

    final BlockAccessListOverlay beforeAdoption = new BlockAccessListOverlay(lookup, 2L);
    assertThat(beforeAdoption.getCode(ADOPTER, noPreBlockCode()))
        .contains(Bytes.fromHexString("0xAA"));

    final BlockAccessListOverlay afterAdoption = new BlockAccessListOverlay(lookup, 3L);
    assertThat(afterAdoption.getCode(ADOPTER, noPreBlockCode())).contains(adoptedCode);
    final BonsaiValue<Bytes> codeValue = new BonsaiValue<>(Bytes.EMPTY, Bytes.EMPTY);
    afterAdoption.applyToCode(ADOPTER, noPreBlockCode(), codeValue::setUpdated);
    assertThat(codeValue.getUpdated()).isEqualTo(adoptedCode);
    final MutableAccount account = mock(MutableAccount.class);
    assertThat(afterAdoption.applyToAccountState(ADOPTER, () -> account)).contains(account);
    verify(account).setCode(eq(adoptedCode));
  }

  @Test
  void resolvesHashFormCodeFromPreBlockCodeOfABalAccount() {
    final Bytes adoptedCode = Bytes.fromHexString("0xBB");
    final Hash adoptedCodeHash = Hash.hash(adoptedCode);
    final BlockAccessListAccountLookup lookup =
        BlockAccessListAccountLookup.of(
            new BlockAccessList(
                List.of(
                    codeChanges(ADOPTER, CodeChange.adoptedCode(1, adoptedCodeHash)),
                    codeChanges(SOURCE))));
    final RecordingPreBlockCodeReader preBlockCode =
        new RecordingPreBlockCodeReader(
            Map.of(ADOPTER, Bytes.fromHexString("0xAA"), SOURCE, adoptedCode));

    final BlockAccessListOverlay overlay = new BlockAccessListOverlay(lookup, 2L);
    assertThat(overlay.getCode(ADOPTER, preBlockCode)).contains(adoptedCode);
    final BonsaiValue<Bytes> codeValue = new BonsaiValue<>(Bytes.EMPTY, Bytes.EMPTY);
    overlay.applyToCode(ADOPTER, preBlockCode, codeValue::setUpdated);
    assertThat(codeValue.getUpdated()).isEqualTo(adoptedCode);
    // the lookup keeps the code it read, the second read does not read the accounts again
    assertThat(preBlockCode.codeHashReads).containsExactly(ADOPTER, SOURCE);
    assertThat(preBlockCode.codeReads).containsExactly(SOURCE);
  }

  @Test
  void rejectsHashFormCodeThatDoesNotResolve() {
    final BlockAccessListAccountLookup lookup =
        BlockAccessListAccountLookup.of(
            new BlockAccessList(
                List.of(
                    codeChanges(
                        ADDRESS,
                        new CodeChange(0, Bytes.fromHexString("0xAA")),
                        CodeChange.adoptedCode(1, Hash.hash(Bytes.fromHexString("0xBB")))),
                    codeChanges(SOURCE))));

    final BlockAccessListOverlay beforeAdoption = new BlockAccessListOverlay(lookup, 1L);
    assertThat(beforeAdoption.getCode(ADDRESS, noPreBlockCode()))
        .contains(Bytes.fromHexString("0xAA"));

    final BlockAccessListOverlay afterAdoption = new BlockAccessListOverlay(lookup, 2L);
    final MutableAccount account = mock(MutableAccount.class);
    assertThatThrownBy(() -> afterAdoption.getCode(ADDRESS, noPreBlockCode()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> afterAdoption.applyToCode(ADDRESS, noPreBlockCode(), code -> {}))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> afterAdoption.applyToAccountState(ADDRESS, () -> account))
        .isInstanceOf(IllegalStateException.class);
    verify(account, never()).setCode(any(Bytes.class));
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
        new BlockAccessListOverlay(
            BlockAccessListAccountLookup.of(new BlockAccessList(List.of(accountChanges))), 2L);

    final BonsaiValue<UInt256> storageValue =
        new BonsaiValue<>(UInt256.valueOf(99), UInt256.valueOf(99));
    overlay.applyToStorage(ADDRESS, SLOT, storageValue::setUpdated);
    assertThat(storageValue.getUpdated()).isEqualTo(UInt256.ZERO);
  }

  private static AccountChanges codeChanges(final Address address, final CodeChange... changes) {
    return new AccountChanges(
        address, List.of(), List.of(), List.of(), List.of(), List.of(changes));
  }
}
