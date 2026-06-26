/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAddressView;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.function.Consumer;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockAccessListOverlayAccumulatorTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  private ExecutionContextTestFixture contextTestFixture;
  private ProtocolContext protocolContext;
  private BlockHeader chainHeadHeader;

  @BeforeEach
  void setUp() {
    contextTestFixture =
        ExecutionContextTestFixture.builder(GenesisConfig.mainnet())
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    protocolContext = contextTestFixture.getProtocolContext();
    chainHeadHeader = contextTestFixture.getBlockchain().getChainHeadHeader();
  }

  @AfterEach
  void tearDown() throws Exception {
    contextTestFixture.getStateArchive().close();
  }

  @Test
  void firstReadAppliesBalOverlayWithoutChangingPrior() {
    final Wei balBalance = Wei.of(2_000);
    final long balNonce = 5L;

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ADDRESS,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balBalance)),
                    List.of(new NonceChange(0, balNonce)),
                    List.of())));

    withAccumulator(
        bal,
        1L,
        accumulator -> {
          final Account readAccount = accumulator.get(ADDRESS);
          assertThat(readAccount).isNotNull();
          assertThat(readAccount.getBalance()).isEqualTo(balBalance);
          assertThat(readAccount.getNonce()).isEqualTo(balNonce);

          final PathBasedValue<BonsaiAccount> tracked =
              accumulator.getAccountsToUpdate().get(ADDRESS);
          assertThat(tracked.getPrior()).isNull();
          assertThat(tracked.getUpdated().getBalance()).isEqualTo(balBalance);
          assertThat(tracked.getUpdated().getNonce()).isEqualTo(balNonce);
        });
  }

  @Test
  void firstStorageReadAppliesBalOverlayWithoutChangingPrior() {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000dd");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(7));
    final UInt256 balValue = UInt256.valueOf(99);

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(new SlotChanges(slotKey, List.of(new StorageChange(0, balValue)))),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of())));

    withAccumulator(
        bal,
        1L,
        accumulator -> {
          assertThat(accumulator.getStorageValue(address, UInt256.valueOf(7))).isEqualTo(balValue);

          final var slotValue = accumulator.getStorageToUpdate().get(address).get(slotKey);
          assertThat(slotValue.getPrior()).isNull();
          assertThat(slotValue.getUpdated()).isEqualTo(balValue);
        });
  }

  @Test
  void missingAccountInParentIsCreatedFromBalOnFirstRead() {
    final Address balOnlyAddress =
        Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final Wei balBalance = Wei.of(5_000);
    final long balNonce = 3L;

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    balOnlyAddress,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balBalance)),
                    List.of(new NonceChange(0, balNonce)),
                    List.of())));

    withAccumulator(
        bal,
        1L,
        accumulator -> {
          final Account readAccount = accumulator.get(balOnlyAddress);
          assertThat(readAccount).isNotNull();
          assertThat(readAccount.getBalance()).isEqualTo(balBalance);
          assertThat(readAccount.getNonce()).isEqualTo(balNonce);

          final PathBasedValue<BonsaiAccount> tracked =
              accumulator.getAccountsToUpdate().get(balOnlyAddress);
          assertThat(tracked.getPrior()).isNull();
          assertThat(tracked.getUpdated().getBalance()).isEqualTo(balBalance);
        });
  }

  @Test
  void missingStorageSlotInParentUsesBalValueInUpdated() {
    final Address balOnlyAddress =
        Address.fromHexString("0x00000000000000000000000000000000000000cc");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(7));
    final UInt256 balValue = UInt256.valueOf(77);

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    balOnlyAddress,
                    List.of(new SlotChanges(slotKey, List.of(new StorageChange(0, balValue)))),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of())));

    withAccumulator(
        bal,
        1L,
        accumulator -> {
          assertThat(accumulator.getStorageValue(balOnlyAddress, UInt256.valueOf(7)))
              .isEqualTo(balValue);

          final var slotValue = accumulator.getStorageToUpdate().get(balOnlyAddress).get(slotKey);
          assertThat(slotValue.getPrior()).isNull();
          assertThat(slotValue.getUpdated()).isEqualTo(balValue);
        });
  }

  @Test
  void subsequentReadsDoNotReapplyBalOverlay() {
    final Wei balBalance = Wei.of(2_000);
    final Wei txBalance = Wei.of(9_000);

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ADDRESS,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balBalance)),
                    List.of(),
                    List.of())));

    withAccumulator(
        bal,
        1L,
        accumulator -> {
          assertThat(accumulator.get(ADDRESS).getBalance()).isEqualTo(balBalance);

          accumulator.getAccount(ADDRESS).setBalance(txBalance);
          assertThat(accumulator.get(ADDRESS).getBalance()).isEqualTo(txBalance);
        });
  }

  @Test
  void overlayRespectsMaxTxIndexExclusive() {
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ADDRESS,
                    List.of(),
                    List.of(),
                    List.of(
                        new BalanceChange(0, Wei.of(2_000)), new BalanceChange(1, Wei.of(3_000))),
                    List.of(),
                    List.of())));

    withAccumulator(
        bal,
        1L,
        accumulator -> assertThat(accumulator.get(ADDRESS).getBalance()).isEqualTo(Wei.of(2_000)));
  }

  private void withAccumulator(
      final BlockAccessList bal,
      final long maxTxIndexExclusive,
      final Consumer<BonsaiWorldStateUpdateAccumulator> consumer) {
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            protocolContext
                .getWorldStateArchive()
                .getWorldState(
                    WorldStateQueryParams.newBuilder()
                        .withBlockHeader(chainHeadHeader)
                        .withShouldWorldStateUpdateHead(false)
                        .withBalOverlay(BlockAccessListAddressView.of(bal), maxTxIndexExclusive)
                        .build())
                .orElseThrow();
    try {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      consumer.accept(accumulator);
    } finally {
      worldState.close();
    }
  }
}
