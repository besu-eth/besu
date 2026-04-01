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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BalStateRootCommitterFactoryTest {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

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
  void trustedMode_balAndSyncCommitterProduceSameRoot() throws Exception {

    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000a1");
    final Wei newBalance = Wei.of(999_999L);
    final long newNonce = 5L;

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, newBalance)),
                    List.of(new NonceChange(0, newNonce)),
                    List.of())));

    // Calculate expected root using standard accumulator
    final Hash expectedRoot = computeRootFromAccumulator(address, newBalance, newNonce);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    // Create committer in trusted mode
    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    final BonsaiWorldState worldState = getWorldState(false);
    try {
      final PathBasedWorldStateUpdateAccumulator<?> updater = worldState.updater();
      final MutableAccount account = updater.getOrCreate(address);
      account.setBalance(newBalance);
      account.setNonce(newNonce);
      updater.commit();

      worldState.persist(blockHeader, committer);

      assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
    } finally {
      worldState.close();
    }
  }

  @Test
  void trustedMode_balRootMismatchThrowsException() throws Exception {

    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000b2");
    final Wei balBalance = Wei.of(1_000_000L);

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balBalance)),
                    List.of(),
                    List.of())));

    final Hash wrongRoot =
        Hash.fromHexString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(wrongRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    final BonsaiWorldState worldState = getWorldState(false);
    try {
      final PathBasedWorldStateUpdateAccumulator<?> updater = worldState.updater();
      final MutableAccount account = updater.getOrCreate(address);
      account.setBalance(balBalance);
      updater.commit();

      assertThatThrownBy(() -> worldState.persist(blockHeader, committer))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("BAL-computed root does not match block header state root");
    } finally {
      worldState.close();
    }
  }

  @Test
  void verificationMode_strictMode_matchingRootsSucceed() throws Exception {

    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000c3");
    final Wei newBalance = Wei.of(555_555L);

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, newBalance)),
                    List.of(),
                    List.of())));

    final Hash expectedRoot = computeRootFromAccumulator(address, newBalance, 0L);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(false)
            .isBalLenientOnStateRootMismatch(false)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    final BonsaiWorldState worldState = getWorldState(false);
    try {
      final PathBasedWorldStateUpdateAccumulator<?> updater = worldState.updater();
      final MutableAccount account = updater.getOrCreate(address);
      account.setBalance(newBalance);
      updater.commit();

      worldState.persist(blockHeader, committer);

      assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
    } finally {
      worldState.close();
    }
  }

  @Test
  void verificationMode_strictMode_mismatchThrowsException() throws Exception {

    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000d4");
    final Wei balBalance = Wei.of(1_111_111L);
    final Wei syncBalance = Wei.of(2_222_222L); // Different!

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balBalance)),
                    List.of(),
                    List.of())));

    // Calculate root with sync balance (different from BAL)
    final Hash syncRoot = computeRootFromAccumulator(address, syncBalance, 0L);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(syncRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(false)
            .isBalLenientOnStateRootMismatch(false)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    try (BonsaiWorldState worldState = getWorldState(false)) {
      final PathBasedWorldStateUpdateAccumulator<?> updater = worldState.updater();
      final MutableAccount account = updater.getOrCreate(address);
      account.setBalance(syncBalance); // Use different balance than BAL
      updater.commit();

      assertThatThrownBy(() -> worldState.persist(blockHeader, committer))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("BAL root mismatch");
    }
  }

  @Test
  void verificationMode_lenientMode_mismatchLogsButSucceeds() throws Exception {

    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000e5");
    final Wei balBalance = Wei.of(3_333_333L);
    final Wei syncBalance = Wei.of(4_444_444L); // Different!

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balBalance)),
                    List.of(),
                    List.of())));

    final Hash syncRoot = computeRootFromAccumulator(address, syncBalance, 0L);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(syncRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(false)
            .isBalLenientOnStateRootMismatch(true) // Lenient mode
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    try (BonsaiWorldState worldState = getWorldState(false)) {
      final PathBasedWorldStateUpdateAccumulator<?> updater = worldState.updater();
      final MutableAccount account = updater.getOrCreate(address);
      account.setBalance(syncBalance); // Use different balance than BAL
      updater.commit();

      worldState.persist(blockHeader, committer);

      assertThat(worldState.rootHash()).isEqualTo(syncRoot);
    }
  }

  @Test
  void cancel_cancelsBalFutureGracefully() throws Exception {

    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000028");
    final Wei newBalance = Wei.of(9_999_999L);

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, newBalance)),
                    List.of(),
                    List.of())));

    final Hash expectedRoot = computeRootFromAccumulator(address, newBalance, 0L);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    committer.cancel();
  }

  @Test
  void factoryReturnsSync_whenBalNotPresent() {

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);

    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.empty());

    assertThat(committer).isSameAs(StateRootCommitter.SYNCHRONOUS);
  }

  @Test
  void multipleAccountChanges_producesCorrectRoot() throws Exception {

    final Address address1 = Address.fromHexString("0x000000000000000000000000000000000000004a");
    final Address address2 = Address.fromHexString("0x000000000000000000000000000000000000005b");
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

    // Calculate expected root
    final BonsaiWorldState expectedWorldState = getWorldState(false);
    Hash expectedRoot;
    try {
      final PathBasedWorldStateUpdateAccumulator<?> updater = expectedWorldState.updater();
      final MutableAccount account1 = updater.getOrCreate(address1);
      account1.setBalance(balance1);
      account1.setNonce(nonce1);
      final MutableAccount account2 = updater.getOrCreate(address2);
      account2.setBalance(balance2);
      account2.setNonce(nonce2);
      updater.commit();
      expectedRoot = expectedWorldState.rootHash();
    } finally {
      expectedWorldState.close();
    }

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    final BonsaiWorldState worldState = getWorldState(false);
    try {
      final PathBasedWorldStateUpdateAccumulator<?> updater = worldState.updater();
      final MutableAccount account1 = updater.getOrCreate(address1);
      account1.setBalance(balance1);
      account1.setNonce(nonce1);
      final MutableAccount account2 = updater.getOrCreate(address2);
      account2.setBalance(balance2);
      account2.setNonce(nonce2);
      updater.commit();

      worldState.persist(blockHeader, committer);

      assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
    } finally {
      worldState.close();
    }
  }

  @Test
  void emptyBalAccessList_producesCorrectRoot() throws Exception {

    final BlockAccessList bal = new BlockAccessList(List.of());

    final Hash expectedRoot = chainHeadHeader.getStateRoot();

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    try (BonsaiWorldState worldState = getWorldState(false)) {
      // No changes to world state

      worldState.persist(blockHeader, committer);

      assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
    }
  }

  @Test
  void trustedMode_notMergeBalStateChangesForFrozenSate() throws Exception {

    final Address address = Address.fromHexString("0x000000000000000000000000000000000000007d");
    final Wei balBalance = Wei.of(1_234_567L);
    final long balNonce = 42L;

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balBalance)),
                    List.of(new NonceChange(0, balNonce)),
                    List.of())));

    final Hash expectedRoot = computeRootFromAccumulator(address, balBalance, balNonce);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    // DON'T make any changes to worldState, let BAL import them
    try (BonsaiWorldState verifyWorldState = getWorldState(false)) {
      // Verify account doesn't exist before
      assertThat(verifyWorldState.get(address)).isNull();

      verifyWorldState.persist(blockHeader, committer);

      // verify BAL state was imported
      assertThat(verifyWorldState.rootHash()).isEqualTo(expectedRoot);

      // Verify the account was not merged
      assertThat(verifyWorldState.get(address)).isNull();
    }
  }

  @Test
  void trustedMode_mergeBalStateChanges_balanceAndNonce() throws Exception {

    final Address address = Address.fromHexString("0x000000000000000000000000000000000000007d");
    final Wei balBalance = Wei.of(1_234_567L);
    final long balNonce = 42L;

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balBalance)),
                    List.of(new NonceChange(0, balNonce)),
                    List.of())));

    final Hash expectedRoot = computeRootFromAccumulator(address, balBalance, balNonce);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    // DON'T make any changes to worldState, let BAL import them
    final BonsaiWorldState verifyWorldState = getWorldState(true);
    try {
      // Verify account doesn't exist before
      assertThat(verifyWorldState.get(address)).isNull();

      verifyWorldState.persist(blockHeader, committer);

      // verify BAL state was imported
      assertThat(verifyWorldState.rootHash()).isEqualTo(expectedRoot);

      // Verify the account was created with correct values from BAL
      assertThat(verifyWorldState.get(address)).isNotNull();
      assertThat(verifyWorldState.get(address).getBalance()).isEqualTo(balBalance);
      assertThat(verifyWorldState.get(address).getNonce()).isEqualTo(balNonce);
    } finally {
      verifyWorldState.close();
    }
  }

  @Test
  void trustedMode_mergeBalStateChanges_withStorage() throws Exception {

    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000a0");
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

    // Calculate expected root
    final BonsaiWorldState expectedWorldState = getWorldState(false);
    Hash expectedRoot;
    try {
      final PathBasedWorldStateUpdateAccumulator<?> updater = expectedWorldState.updater();
      final MutableAccount account = updater.getOrCreate(address);
      account.setBalance(balance);
      account.setStorageValue(slot1.getSlotKey().orElseThrow(), value1);
      account.setStorageValue(slot2.getSlotKey().orElseThrow(), value2);
      updater.commit();
      expectedRoot = expectedWorldState.rootHash();
    } finally {
      expectedWorldState.close();
    }

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    final BonsaiWorldState verifyWorldState = getWorldState(true);
    try {
      verifyWorldState.persist(blockHeader, committer);

      assertThat(verifyWorldState.rootHash()).isEqualTo(expectedRoot);

      // Verify account and storage were imported
      assertThat(verifyWorldState.get(address)).isNotNull();
      assertThat(verifyWorldState.get(address).getBalance()).isEqualTo(balance);
      assertThat(verifyWorldState.getStorageValue(address, slot1.getSlotKey().orElseThrow()))
          .isEqualTo(value1);
      assertThat(verifyWorldState.getStorageValue(address, slot2.getSlotKey().orElseThrow()))
          .isEqualTo(value2);
    } finally {
      verifyWorldState.close();
    }
  }

  @Test
  void trustedMode_mergeBalStateChanges_withCode() throws Exception {

    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000b1");
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

    // Calculate expected root
    final BonsaiWorldState expectedWorldState = getWorldState(false);
    Hash expectedRoot;
    try {
      final PathBasedWorldStateUpdateAccumulator<?> updater = expectedWorldState.updater();
      final MutableAccount account = updater.getOrCreate(address);
      account.setBalance(balance);
      account.setCode(code);
      updater.commit();
      expectedRoot = expectedWorldState.rootHash();
    } finally {
      expectedWorldState.close();
    }

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    try (BonsaiWorldState verifyWorldState = getWorldState(true)) {
      verifyWorldState.persist(blockHeader, committer);

      assertThat(verifyWorldState.rootHash()).isEqualTo(expectedRoot);

      // Verify account and code were imported
      assertThat(verifyWorldState.get(address)).isNotNull();
      assertThat(verifyWorldState.get(address).getBalance()).isEqualTo(balance);
      assertThat(verifyWorldState.get(address).getCode()).isEqualTo(code);
    }
  }

  @Test
  void trustedMode_mergeBalStateChanges_complexScenario() throws Exception {
    // Complex scenario with multiple accounts, storage, and code
    final Address contractAddress = Address.fromHexString("0x00000000000000000000000000000000e4");
    final Address eoaAddress = Address.fromHexString("0x00000000000000000000000000000000000000f5");

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

    // Calculate expected root
    final BonsaiWorldState expectedWorldState = getWorldState(false);
    Hash expectedRoot;
    try {
      final PathBasedWorldStateUpdateAccumulator<?> updater = expectedWorldState.updater();

      final MutableAccount contract = updater.getOrCreate(contractAddress);
      contract.setBalance(contractBalance);
      contract.setCode(contractCode);
      contract.setStorageValue(slot.getSlotKey().orElseThrow(), slotValue);

      final MutableAccount eoa = updater.getOrCreate(eoaAddress);
      eoa.setBalance(eoaBalance);
      eoa.setNonce(eoaNonce);

      updater.commit();
      expectedRoot = expectedWorldState.rootHash();
    } finally {
      expectedWorldState.close();
    }

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(expectedRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(true)
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    // DON'T make any changes, let BAL import everything
    try (BonsaiWorldState verifyWorldState = getWorldState(true)) {
      verifyWorldState.persist(blockHeader, committer);

      // verify all state was imported correctly
      assertThat(verifyWorldState.rootHash()).isEqualTo(expectedRoot);

      // Verify contract account
      assertThat(verifyWorldState.get(contractAddress)).isNotNull();
      assertThat(verifyWorldState.get(contractAddress).getBalance()).isEqualTo(contractBalance);
      assertThat(verifyWorldState.get(contractAddress).getCode()).isEqualTo(contractCode);
      assertThat(verifyWorldState.getStorageValue(contractAddress, slot.getSlotKey().orElseThrow()))
          .isEqualTo(slotValue);

      // Verify EOA account
      assertThat(verifyWorldState.get(eoaAddress)).isNotNull();
      assertThat(verifyWorldState.get(eoaAddress).getBalance()).isEqualTo(eoaBalance);
      assertThat(verifyWorldState.get(eoaAddress).getNonce()).isEqualTo(eoaNonce);
    }
  }

  @Test
  void verificationMode_doesNotImportBalStateChanges() throws Exception {

    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000d3");
    final Wei balBalance = Wei.of(1_111_111L);
    final Wei syncBalance = Wei.of(2_222_222L);

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balBalance)),
                    List.of(),
                    List.of())));

    // Expected root based on sync balance (what we actually set)
    final Hash syncRoot = computeRootFromAccumulator(address, syncBalance, 0L);

    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .stateRoot(syncRoot)
            .buildHeader();

    final BalConfiguration balConfig =
        ImmutableBalConfiguration.builder()
            .isBalStateRootTrusted(false) // Verification mode
            .isBalLenientOnStateRootMismatch(true) // Lenient to allow mismatch
            .balStateRootTimeout(DEFAULT_TIMEOUT)
            .build();

    final StateRootCommitterFactory factory = new BalStateRootCommitterFactory(balConfig);
    final StateRootCommitter committer =
        factory.forBlock(protocolContext, blockHeader, Optional.of(bal));

    // Make sync changes (different from BAL)
    final BonsaiWorldState verifyWorldState = getWorldState(true);
    try {
      final PathBasedWorldStateUpdateAccumulator<?> updater = verifyWorldState.updater();
      final MutableAccount account = updater.getOrCreate(address);
      account.setBalance(syncBalance); // Use sync balance, not BAL balance
      updater.commit();

      // In lenient mode, this should succeed even though BAL root differs
      verifyWorldState.persist(blockHeader, committer);

      // verify sync root is correct (BAL was NOT imported)
      assertThat(verifyWorldState.rootHash()).isEqualTo(syncRoot);

      // Verify the persisted state uses sync balance, not BAL balance
      assertThat(verifyWorldState.get(address)).isNotNull();
      assertThat(verifyWorldState.get(address).getBalance())
          .isEqualTo(syncBalance) // Should be sync balance
          .isNotEqualTo(balBalance); // NOT BAL balance
    } finally {
      verifyWorldState.close();
    }
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
    final BonsaiWorldState worldState = getWorldState(false);
    try {
      final PathBasedWorldStateUpdateAccumulator<?> accumulator = worldState.updater();
      final MutableAccount account = accumulator.getOrCreate(address);
      account.setBalance(balance);
      if (nonce > 0) {
        account.setNonce(nonce);
      }
      accumulator.commit();
      return worldState.rootHash();
    } finally {
      worldState.close();
    }
  }
}
