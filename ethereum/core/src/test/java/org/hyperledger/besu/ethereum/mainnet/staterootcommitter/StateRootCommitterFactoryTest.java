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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StateRootCommitterFactoryTest {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  private ExecutionContextTestFixture contextTestFixture;
  private ProtocolContext protocolContext;
  private BlockHeader chainHeadHeader;
  private StateRootCommitterFactory factory;

  @BeforeEach
  void setUp() {
    contextTestFixture =
        ExecutionContextTestFixture.builder(GenesisConfig.mainnet())
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    protocolContext = contextTestFixture.getProtocolContext();
    chainHeadHeader = contextTestFixture.getBlockchain().getChainHeadHeader();
    factory = new StateRootCommitterFactory(balConfig());
  }

  @AfterEach
  void tearDown() throws Exception {
    contextTestFixture.getStateArchive().close();
  }

  @Nested
  class FactorySelection {

    @Test
    void factoryReturnsDefault_whenBalNotPresent() {
      final BlockHeader blockHeader = childHeader(chainHeadHeader.getStateRoot());

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.empty(), worldState.isStorageFrozen());

        assertThat(committer).isInstanceOf(DefaultStateRootCommitter.class);
      }
    }

    @Test
    void factoryReturnsDefault_whenBalStateRootDisabled() {
      final BlockAccessList bal =
          new BlockAccessList(
              List.of(
                  new AccountChanges(
                      Address.fromHexString("0x00000000000000000000000000000000000000a1"),
                      List.of(),
                      List.of(),
                      List.of(),
                      List.of(),
                      List.of())));

      final BlockHeader blockHeader = childHeader(chainHeadHeader.getStateRoot());
      final StateRootCommitterFactory disabledFactory =
          new StateRootCommitterFactory(
              ImmutableBalConfiguration.builder()
                  .isBalStateRootEnabled(false)
                  .balStateRootTimeout(DEFAULT_TIMEOUT)
                  .build());

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            disabledFactory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());

        assertThat(committer).isInstanceOf(DefaultStateRootCommitter.class);
      }
    }

    @Test
    void factoryReturnsBal_whenBalPresentAndEnabled() {
      final BlockAccessList bal = balanceAndNonceBal(testAddress("a1"), Wei.of(1), 1L);
      final BlockHeader blockHeader = childHeader(chainHeadHeader.getStateRoot());

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());

        assertThat(committer).isInstanceOf(BalStateRootCommitter.class);
      }
    }

    @Test
    void factoryReturnsForest_whenForestArchive() throws Exception {
      final ExecutionContextTestFixture forestFixture =
          ExecutionContextTestFixture.builder(GenesisConfig.mainnet()).build();
      try {
        final ProtocolContext forestContext = forestFixture.getProtocolContext();
        final BlockHeader blockHeader =
            forestFixture.getBlockchain().getChainHeadHeader();
        final BlockHeader child =
            new BlockHeaderTestFixture()
                .parentHash(blockHeader.getHash())
                .number(blockHeader.getNumber() + 1L)
                .stateRoot(blockHeader.getStateRoot())
                .buildHeader();
        final BlockAccessList bal = balanceAndNonceBal(testAddress("c0"), Wei.of(1), 0L);

        final StateRootCommitter committer =
            factory.forBlock(forestContext, child, Optional.of(bal), false);

        assertThat(committer).isSameAs(ForestStateRootCommitter.INSTANCE);
      } finally {
        forestFixture.getStateArchive().close();
      }
    }

    @Test
    void factoryReturnsDefault_whenTrieDisabled() {
      final BlockAccessList bal = balanceAndNonceBal(testAddress("d1"), Wei.of(42), 2L);
      final BlockHeader blockHeader = childHeader(chainHeadHeader.getStateRoot());
      final BonsaiWorldStateProvider archive =
          (BonsaiWorldStateProvider) protocolContext.getWorldStateArchive();
      archive.getWorldStateSharedSpec().setTrieDisabled(true);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());

        assertThat(committer).isInstanceOf(DefaultStateRootCommitter.class);
      } finally {
        archive.getWorldStateSharedSpec().setTrieDisabled(false);
      }
    }
  }

  @Nested
  class BalCommitterRootEquivalence {

    @Test
    void defaultAndBalCommitterProduceSameRoot() {
      final Address address = testAddress("a1");
      final Wei newBalance = Wei.of(999_999L);
      final long newNonce = 5L;
      final BlockAccessList bal = balanceAndNonceBal(address, newBalance, newNonce);
      final Hash expectedRoot = computeRootFromAccumulator(address, newBalance, newNonce);
      final BlockHeader blockHeader = childHeader(expectedRoot);

      final Hash defaultRoot;
      try (BonsaiWorldState worldState = getWorldState(false)) {
        applyBalanceAndNonce(worldState, address, newBalance, newNonce);
        worldState.persist(blockHeader, new DefaultStateRootCommitter());
        defaultRoot = worldState.rootHash();
      }

      final Hash balRoot;
      try (BonsaiWorldState worldState = getWorldState(false)) {
        applyBalanceAndNonce(worldState, address, newBalance, newNonce);
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);
        balRoot = worldState.rootHash();
      }

      assertThat(defaultRoot).isEqualTo(expectedRoot);
      assertThat(balRoot).isEqualTo(expectedRoot);
    }

    @Test
    void multipleAccountChanges_producesCorrectRoot() {
      final Address address1 = testAddress("4a");
      final Address address2 = testAddress("5b");
      final Wei balance1 = Wei.of(1_111_111L);
      final Wei balance2 = Wei.of(2_222_222L);
      final long nonce1 = 10L;
      final long nonce2 = 20L;

      final BlockAccessList bal =
          new BlockAccessList(
              List.of(
                  new AccountChanges(
                      address1,
                      List.of(),
                      List.of(),
                      List.of(new BalanceChange(0, balance1)),
                      List.of(new NonceChange(0, nonce1)),
                      List.of()),
                  new AccountChanges(
                      address2,
                      List.of(),
                      List.of(),
                      List.of(new BalanceChange(0, balance2)),
                      List.of(new NonceChange(0, nonce2)),
                      List.of())));

      final Hash expectedRoot;
      try (BonsaiWorldState expectedWorldState = getWorldState(false)) {
        applyBalanceAndNonce(expectedWorldState, address1, balance1, nonce1);
        applyBalanceAndNonce(expectedWorldState, address2, balance2, nonce2);
        expectedRoot = expectedWorldState.rootHash();
      }

      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        applyBalanceAndNonce(worldState, address1, balance1, nonce1);
        applyBalanceAndNonce(worldState, address2, balance2, nonce2);
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);
        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
      }
    }

    @Test
    void emptyBalAccessList_producesCorrectRoot() {
      final BlockAccessList bal = new BlockAccessList(List.of());
      final Hash expectedRoot = chainHeadHeader.getStateRoot();
      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);
        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
      }
    }

    @Test
    void balRootMismatchThrowsException() {
      final Address address = testAddress("b2");
      final Wei balBalance = Wei.of(1_000_000L);
      final BlockAccessList bal = balanceOnlyBal(address, balBalance);
      final Hash wrongRoot =
          Hash.fromHexString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
      final BlockHeader blockHeader = childHeader(wrongRoot);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        applyBalanceAndNonce(worldState, address, balBalance, 0L);
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());

        assertThatThrownBy(() -> worldState.persist(blockHeader, committer))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("BAL-computed root does not match block header state root");
      }
    }

    @Test
    void cancel_preventsSubsequentCompute() {
      final Address address = testAddress("28");
      final Wei newBalance = Wei.of(9_999_999L);
      final BlockAccessList bal = balanceOnlyBal(address, newBalance);
      final Hash expectedRoot = computeRootFromAccumulator(address, newBalance, 0L);
      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        applyBalanceAndNonce(worldState, address, newBalance, 0L);
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        committer.cancel();

        assertThatThrownBy(
                () -> committer.compute(worldState, blockHeader, worldState.updater()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Background BAL state root computation was cancelled");
      }
    }
  }

  @Nested
  class BalOverlayMerge {

    @Test
    void balRootWithoutHeadUpdate_doesNotExposeMergedAccounts() {
      final Address address = testAddress("7d");
      final Wei balBalance = Wei.of(1_234_567L);
      final long balNonce = 42L;
      final BlockAccessList bal = balanceAndNonceBal(address, balBalance, balNonce);
      final Hash expectedRoot = computeRootFromAccumulator(address, balBalance, balNonce);
      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        assertThat(worldState.get(address)).isNull();
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);

        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
        assertThat(worldState.get(address)).isNull();
      }
    }

    @Test
    void frozenStorage_balComputesRootWithoutExposingAccounts() {
      final Address address = testAddress("7e");
      final Wei balBalance = Wei.of(2_345_678L);
      final long balNonce = 7L;
      final BlockAccessList bal = balanceAndNonceBal(address, balBalance, balNonce);
      final Hash expectedRoot = computeRootFromAccumulator(address, balBalance, balNonce);
      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        worldState.freezeStorage();
        assertThat(worldState.isStorageFrozen()).isTrue();
        assertThat(worldState.get(address)).isNull();

        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);

        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
        assertThat(worldState.get(address)).isNull();
      }
    }

    @Test
    void mergeBalStateChanges_balanceAndNonce() {
      final Address address = testAddress("7d");
      final Wei balBalance = Wei.of(1_234_567L);
      final long balNonce = 42L;
      final BlockAccessList bal = balanceAndNonceBal(address, balBalance, balNonce);
      final Hash expectedRoot = computeRootFromAccumulator(address, balBalance, balNonce);
      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(true)) {
        assertThat(worldState.get(address)).isNull();
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);

        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
        assertThat(worldState.get(address)).isNotNull();
        assertThat(worldState.get(address).getBalance()).isEqualTo(balBalance);
        assertThat(worldState.get(address).getNonce()).isEqualTo(balNonce);
      }
    }

    @Test
    void mergeBalStateChanges_withStorage() {
      final Address address = testAddress("a0");
      final Wei balance = Wei.of(5_000_000L);
      final StorageSlotKey slot1 = new StorageSlotKey(UInt256.valueOf(1));
      final StorageSlotKey slot2 = new StorageSlotKey(UInt256.valueOf(2));
      final UInt256 value1 = UInt256.valueOf(100);
      final UInt256 value2 = UInt256.valueOf(200);

      final BlockAccessList bal =
          new BlockAccessList(
              List.of(
                  new AccountChanges(
                      address,
                      List.of(
                          new SlotChanges(slot1, List.of(new StorageChange(0, value1))),
                          new SlotChanges(slot2, List.of(new StorageChange(0, value2)))),
                      List.of(),
                      List.of(new BalanceChange(0, balance)),
                      List.of(),
                      List.of())));

      final Hash expectedRoot;
      try (BonsaiWorldState expectedWorldState = getWorldState(false)) {
        final WorldUpdater updater = expectedWorldState.updater();
        final MutableAccount account = updater.getOrCreate(address);
        account.setBalance(balance);
        account.setStorageValue(slot1.getSlotKey().orElseThrow(), value1);
        account.setStorageValue(slot2.getSlotKey().orElseThrow(), value2);
        updater.commit();
        expectedRoot = expectedWorldState.rootHash();
      }

      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(true)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);

        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
        assertThat(worldState.get(address)).isNotNull();
        assertThat(worldState.get(address).getBalance()).isEqualTo(balance);
        assertThat(worldState.getStorageValue(address, slot1.getSlotKey().orElseThrow()))
            .isEqualTo(value1);
        assertThat(worldState.getStorageValue(address, slot2.getSlotKey().orElseThrow()))
            .isEqualTo(value2);
      }
    }

    @Test
    void mergeBalStateChanges_withCode() {
      final Address address = testAddress("b1");
      final Wei balance = Wei.of(3_000_000L);
      final Bytes code = Bytes.fromHexString("0x60806040");

      final BlockAccessList bal =
          new BlockAccessList(
              List.of(
                  new AccountChanges(
                      address,
                      List.of(),
                      List.of(),
                      List.of(new BalanceChange(0, balance)),
                      List.of(),
                      List.of(new CodeChange(0, code)))));

      final Hash expectedRoot;
      try (BonsaiWorldState expectedWorldState = getWorldState(false)) {
        final WorldUpdater updater = expectedWorldState.updater();
        final MutableAccount account = updater.getOrCreate(address);
        account.setBalance(balance);
        account.setCode(code);
        updater.commit();
        expectedRoot = expectedWorldState.rootHash();
      }

      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(true)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);

        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
        assertThat(worldState.get(address)).isNotNull();
        assertThat(worldState.get(address).getBalance()).isEqualTo(balance);
        assertThat(worldState.get(address).getCode()).isEqualTo(code);
      }
    }

    @Test
    void mergeBalStateChanges_complexScenario() {
      final Address contractAddress = testAddress("e4");
      final Address eoaAddress = testAddress("f5");
      final Wei contractBalance = Wei.of(5_000_000L);
      final Wei eoaBalance = Wei.of(10_000_000L);
      final long eoaNonce = 15L;
      final Bytes contractCode = Bytes.fromHexString("0x608060405234801561001057600080fd5b50");
      final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(5));
      final UInt256 slotValue = UInt256.valueOf(999);

      final BlockAccessList bal =
          new BlockAccessList(
              List.of(
                  new AccountChanges(
                      contractAddress,
                      List.of(new SlotChanges(slot, List.of(new StorageChange(0, slotValue)))),
                      List.of(),
                      List.of(new BalanceChange(0, contractBalance)),
                      List.of(),
                      List.of(new CodeChange(0, contractCode))),
                  new AccountChanges(
                      eoaAddress,
                      List.of(),
                      List.of(),
                      List.of(new BalanceChange(0, eoaBalance)),
                      List.of(new NonceChange(0, eoaNonce)),
                      List.of())));

      final Hash expectedRoot;
      try (BonsaiWorldState expectedWorldState = getWorldState(false)) {
        final WorldUpdater updater = expectedWorldState.updater();
        final MutableAccount contract = updater.getOrCreate(contractAddress);
        contract.setBalance(contractBalance);
        contract.setCode(contractCode);
        contract.setStorageValue(slot.getSlotKey().orElseThrow(), slotValue);
        final MutableAccount eoa = updater.getOrCreate(eoaAddress);
        eoa.setBalance(eoaBalance);
        eoa.setNonce(eoaNonce);
        updater.commit();
        expectedRoot = expectedWorldState.rootHash();
      }

      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(true)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);

        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
        assertThat(worldState.get(contractAddress)).isNotNull();
        assertThat(worldState.get(contractAddress).getBalance()).isEqualTo(contractBalance);
        assertThat(worldState.get(contractAddress).getCode()).isEqualTo(contractCode);
        assertThat(worldState.getStorageValue(contractAddress, slot.getSlotKey().orElseThrow()))
            .isEqualTo(slotValue);
        assertThat(worldState.get(eoaAddress)).isNotNull();
        assertThat(worldState.get(eoaAddress).getBalance()).isEqualTo(eoaBalance);
        assertThat(worldState.get(eoaAddress).getNonce()).isEqualTo(eoaNonce);
      }
    }
  }

  private static BalConfiguration balConfig() {
    return ImmutableBalConfiguration.builder().balStateRootTimeout(DEFAULT_TIMEOUT).build();
  }

  private BlockHeader childHeader(final Hash stateRoot) {
    return new BlockHeaderTestFixture()
        .parentHash(chainHeadHeader.getHash())
        .number(chainHeadHeader.getNumber() + 1L)
        .stateRoot(stateRoot)
        .buildHeader();
  }

  private static Address testAddress(final String suffix) {
    return Address.fromHexString("0x00000000000000000000000000000000000000" + suffix);
  }

  private static BlockAccessList balanceAndNonceBal(
      final Address address, final Wei balance, final long nonce) {
    return new BlockAccessList(
        List.of(
            new AccountChanges(
                address,
                List.of(),
                List.of(),
                List.of(new BalanceChange(0, balance)),
                List.of(new NonceChange(0, nonce)),
                List.of())));
  }

  private static BlockAccessList balanceOnlyBal(final Address address, final Wei balance) {
    return new BlockAccessList(
        List.of(
            new AccountChanges(
                address,
                List.of(),
                List.of(),
                List.of(new BalanceChange(0, balance)),
                List.of(),
                List.of())));
  }

  private static void applyBalanceAndNonce(
      final BonsaiWorldState worldState,
      final Address address,
      final Wei balance,
      final long nonce) {
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.getOrCreate(address);
    account.setBalance(balance);
    if (nonce > 0) {
      account.setNonce(nonce);
    }
    updater.commit();
  }

  private BonsaiWorldState getWorldState(final boolean shouldUpdateHead) {
    return (BonsaiWorldState)
        protocolContext
            .getWorldStateArchive()
            .getWorldState(
                WorldStateQueryParams.newBuilder()
                    .withBlockHeader(chainHeadHeader)
                    .withShouldWorldStateUpdateHead(shouldUpdateHead)
                    .build())
            .orElseThrow();
  }

  private Hash computeRootFromAccumulator(
      final Address address, final Wei balance, final long nonce) {
    try (BonsaiWorldState worldState = getWorldState(false)) {
      applyBalanceAndNonce(worldState, address, balance, nonce);
      return worldState.rootHash();
    }
  }
}
