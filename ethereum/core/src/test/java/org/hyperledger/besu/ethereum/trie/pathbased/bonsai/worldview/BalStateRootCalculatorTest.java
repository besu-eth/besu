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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalRootComputation;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BalStateRootCalculatorTest {

  private static final Duration FUTURE_TIMEOUT = Duration.ofSeconds(3);

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
  void balanceAndNonceUpdatesProduceMatchingRoots() throws Exception {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final Wei newBalance = Wei.of(1_234_567L);
    final long newNonce = 7L;

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

    final Hash accumulatorRoot =
        computeRootFromAccumulator(
            accumulator -> {
              final MutableAccount account = accumulator.getOrCreate(address);
              account.setBalance(newBalance);
              account.setNonce(newNonce);
            });

    final Hash balRoot =
        computeRootFromBalAsync(bal).get(FUTURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS).root();

    assertThat(balRoot).isEqualTo(accumulatorRoot);
  }

  @Test
  void codeAndStorageUpdatesProduceMatchingRoots() throws Exception {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(12));
    final UInt256 newValue = UInt256.valueOf(0xdeadbeefL);
    final Bytes newCode = Bytes.fromHexString("0x60016000");

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(new SlotChanges(slotKey, List.of(new StorageChange(0, newValue)))),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(new CodeChange(0, newCode)))));

    final Hash accumulatorRoot =
        computeRootFromAccumulator(
            accumulator -> {
              final MutableAccount account = accumulator.getOrCreate(address);
              account.setStorageValue(slotKey.getSlotKey().orElseThrow(), newValue);
              account.setCode(newCode);
            });

    final Hash balRoot =
        computeRootFromBalAsync(bal).get(FUTURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS).root();

    assertThat(balRoot).isEqualTo(accumulatorRoot);
  }

  @Test
  void multipleAccountsProduceMatchingRoots() throws Exception {
    final Address addressOne = Address.fromHexString("0x00000000000000000000000000000000000000cc");
    final Address addressTwo = Address.fromHexString("0x00000000000000000000000000000000000000dd");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(1));
    final Bytes newCode = Bytes.fromHexString("0x60016000");

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    addressOne,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, Wei.of(42))),
                    List.of(new NonceChange(0, 3L)),
                    List.of()),
                new AccountChanges(
                    addressTwo,
                    List.of(
                        new SlotChanges(
                            slotKey, List.of(new StorageChange(0, UInt256.valueOf(99))))),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(new CodeChange(0, newCode)))));

    final Hash accumulatorRoot =
        computeRootFromAccumulator(
            accumulator -> {
              final MutableAccount first = accumulator.getOrCreate(addressOne);
              first.setBalance(Wei.of(42));
              first.setNonce(3L);

              final MutableAccount second = accumulator.getOrCreate(addressTwo);
              second.setCode(newCode);
              second.setStorageValue(slotKey.getSlotKey().orElseThrow(), UInt256.valueOf(99));
            });

    final Hash balRoot =
        computeRootFromBalAsync(bal).get(FUTURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS).root();

    assertThat(balRoot).isEqualTo(accumulatorRoot);
  }

  @Test
  void mismatchedBalanceUpdateProducesDifferentRoots() throws Exception {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000ee");

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, Wei.of(50))),
                    List.of(),
                    List.of())));

    final Hash accumulatorRoot =
        computeRootFromAccumulator(
            accumulator -> {
              final MutableAccount account = accumulator.getOrCreate(address);
              account.setBalance(Wei.of(75));
            });

    final Hash balRoot =
        computeRootFromBalAsync(bal).get(FUTURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS).root();

    assertThat(balRoot).isNotEqualTo(accumulatorRoot);
  }

  @Test
  void mismatchedStorageUpdateProducesDifferentRoots() throws Exception {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000ff");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(2));

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(
                        new SlotChanges(
                            slotKey, List.of(new StorageChange(0, UInt256.valueOf(111))))),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of())));

    final Hash accumulatorRoot =
        computeRootFromAccumulator(
            accumulator -> {
              final MutableAccount account = accumulator.getOrCreate(address);
              account.setStorageValue(slotKey.getSlotKey().orElseThrow(), UInt256.valueOf(222));
            });

    final Hash balRoot =
        computeRootFromBalAsync(bal).get(FUTURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS).root();

    assertThat(balRoot).isNotEqualTo(accumulatorRoot);
  }

  @Test
  void accountPresentButNoChangesDoesNotAlterRoot() throws Exception {
    final Address readOnlyAddress =
        Address.fromHexString("0x0000000000000000000000000000000000000aaa");
    final Address updatedAddress =
        Address.fromHexString("0x0000000000000000000000000000000000000bbb");

    final Wei newBalance = Wei.of(12345);
    final long newNonce = 9L;

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    readOnlyAddress, List.of(), List.of(), List.of(), List.of(), List.of()),
                new AccountChanges(
                    updatedAddress,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, newBalance)),
                    List.of(new NonceChange(0, newNonce)),
                    List.of())));

    final Hash expectedRoot =
        computeRootFromAccumulator(
            accumulator -> {
              final MutableAccount account = accumulator.getOrCreate(updatedAddress);
              account.setBalance(newBalance);
              account.setNonce(newNonce);
            });

    final Hash balRoot =
        computeRootFromBalAsync(bal).get(FUTURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS).root();

    assertThat(balRoot).isEqualTo(expectedRoot);
  }

  private Hash computeRootFromAccumulator(
      final Consumer<BonsaiWorldStateUpdateAccumulator> accumulatorConsumer) {
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            protocolContext
                .getWorldStateArchive()
                .getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(chainHeadHeader))
                .orElseThrow();
    try {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.getAccumulator();
      accumulatorConsumer.accept(accumulator);
      accumulator.commit();
      return worldState.calculateRootHash(Optional.empty(), accumulator);
    } finally {
      worldState.close();
    }
  }

  private CompletableFuture<BalRootComputation> computeRootFromBalAsync(final BlockAccessList bal) {
    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber() + 1L)
            .buildHeader();

    return BalStateRootCalculator.computeAsync(protocolContext, blockHeader, bal);
  }
}
