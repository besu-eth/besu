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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BlockAccessListFlatDatabaseUpdaterTest {

  @Test
  void shouldApplyStoredBlockAccessListIntoFlatDbUsingTrieBackedAccountData() {
    final BonsaiWorldStateKeyValueStorage storage = createStorage();
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(storage);

    final Address accountAddress =
        Address.fromHexString("0x1000000000000000000000000000000000000001");
    final Hash accountHash = Hash.hash(accountAddress.getBytes());
    final StorageSlotKey updatedSlot = new StorageSlotKey(UInt256.ONE);
    final StorageSlotKey removedSlot = new StorageSlotKey(UInt256.valueOf(2));

    final Hash originalCodeHash = Hash.hash(Bytes.fromHexString("0x6000"));
    final Hash storageRootFromTrie = Hash.hash(Bytes.fromHexString("0x1234"));

    final PmtStateTrieAccountValue existingTrieAccount =
        new PmtStateTrieAccountValue(7L, Wei.of(5_000L), storageRootFromTrie, originalCodeHash);
    final Hash stateRoot =
        persistAccountTrieState(coordinator, storage, accountHash, existingTrieAccount);

    final MutableBlockchain blockchain = createBlockchainWithGenesis(stateRoot);
    final BlockHeader parent = blockchain.getChainHeadHeader();

    final Bytes updatedCode = Bytes.fromHexString("0x6001600055");
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    accountAddress,
                    List.of(
                        new BlockAccessList.SlotChanges(
                            updatedSlot,
                            List.of(
                                new BlockAccessList.StorageChange(0, UInt256.valueOf(11)),
                                new BlockAccessList.StorageChange(1, UInt256.valueOf(99)))),
                        new BlockAccessList.SlotChanges(
                            removedSlot,
                            List.of(
                                new BlockAccessList.StorageChange(0, UInt256.valueOf(22)),
                                new BlockAccessList.StorageChange(1, UInt256.ZERO)))),
                    List.of(),
                    List.of(),
                    List.of(
                        new BlockAccessList.NonceChange(0, 8L),
                        new BlockAccessList.NonceChange(1, 9L)),
                    List.of(
                        new BlockAccessList.CodeChange(0, Bytes.fromHexString("0x60016000")),
                        new BlockAccessList.CodeChange(1, updatedCode)))));

    appendBlock(blockchain, parent, stateRoot, Optional.of(blockAccessList));

    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    BlockAccessListFlatDatabaseUpdater.applyFromStoredBlockAccessLists(
        blockchain, coordinator, updater, 1L, 1L);

    final PmtStateTrieAccountValue updatedAccount =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(
                storage
                    .getAccount(accountHash)
                    .orElseThrow(() -> new AssertionError("missing account"))));

    assertThat(updatedAccount.getNonce()).isEqualTo(9L);
    assertThat(updatedAccount.getBalance()).isEqualTo(Wei.of(5_000L));
    assertThat(updatedAccount.getStorageRoot()).isEqualTo(storageRootFromTrie);
    assertThat(updatedAccount.getCodeHash()).isEqualTo(Hash.hash(updatedCode));

    assertThat(storage.getCode(Hash.hash(updatedCode), accountHash)).contains(updatedCode);
    assertThat(storage.getStorageValueByStorageSlotKey(accountHash, updatedSlot))
        .contains(Bytes32.leftPad(UInt256.valueOf(99).toMinimalBytes()));
    assertThat(storage.getStorageValueByStorageSlotKey(accountHash, removedSlot)).isEmpty();
  }

  @Test
  void shouldRemoveStaleFlatStateWhenTrieEntryIsMissingAndBalContainsNoChanges() {
    final BonsaiWorldStateKeyValueStorage storage = createStorage();
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(storage);

    final Hash stateRoot = Hash.EMPTY_TRIE_HASH;
    final MutableBlockchain blockchain = createBlockchainWithGenesis(stateRoot);

    final Address newAddress = Address.fromHexString("0x2000000000000000000000000000000000000002");
    final Hash newAccountHash = Hash.hash(newAddress.getBytes());
    final StorageSlotKey staleSlot = new StorageSlotKey(UInt256.valueOf(3));
    final BonsaiWorldStateKeyValueStorage.Updater staleDataUpdater = storage.updater();
    staleDataUpdater.putAccountInfoState(
        newAccountHash,
        RLP.encode(
            out ->
                new PmtStateTrieAccountValue(1L, Wei.of(2L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY)
                    .writeTo(out)));
    staleDataUpdater.putStorageValueBySlotHash(
        newAccountHash, staleSlot.getSlotHash(), Bytes32.leftPad(UInt256.ONE.toMinimalBytes()));
    staleDataUpdater.commit();

    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    newAddress, List.of(), List.of(), List.of(), List.of(), List.of())));

    appendBlock(
        blockchain, blockchain.getChainHeadHeader(), stateRoot, Optional.of(blockAccessList));

    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    BlockAccessListFlatDatabaseUpdater.applyFromStoredBlockAccessLists(
        blockchain, coordinator, updater, 1L, 1L);

    assertThat(storage.getAccount(newAccountHash)).isEmpty();
    assertThat(storage.getStorageValueByStorageSlotKey(newAccountHash, staleSlot)).isEmpty();
  }

  @Test
  void shouldContinueAcrossMissingHeadersAndMissingStoredAccessLists() {
    final BonsaiWorldStateKeyValueStorage storage = createStorage();
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(storage);

    final Address appliedAddress =
        Address.fromHexString("0x3000000000000000000000000000000000000003");
    final Hash appliedAccountHash = Hash.hash(appliedAddress.getBytes());
    final PmtStateTrieAccountValue existingTrieAccount =
        new PmtStateTrieAccountValue(0L, Wei.ZERO, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final Hash stateRoot =
        persistAccountTrieState(coordinator, storage, appliedAccountHash, existingTrieAccount);
    final MutableBlockchain blockchain = createBlockchainWithGenesis(stateRoot);

    appendBlock(blockchain, blockchain.getChainHeadHeader(), stateRoot, Optional.empty());

    final BlockAccessList balAtBlock2 =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    appliedAddress,
                    List.of(),
                    List.of(),
                    List.of(new BlockAccessList.BalanceChange(0, Wei.of(77L))),
                    List.of(),
                    List.of())));
    appendBlock(blockchain, blockchain.getChainHeadHeader(), stateRoot, Optional.of(balAtBlock2));

    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    BlockAccessListFlatDatabaseUpdater.applyFromStoredBlockAccessLists(
        blockchain, coordinator, updater, 1L, 3L);

    final PmtStateTrieAccountValue accountValue =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(
                storage
                    .getAccount(appliedAccountHash)
                    .orElseThrow(() -> new AssertionError("missing account"))));

    assertThat(accountValue.getBalance()).isEqualTo(Wei.of(77L));
    assertThat(accountValue.getNonce()).isZero();
  }

  private BonsaiWorldStateKeyValueStorage createStorage() {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_PARTIAL_DB_CONFIG);
    storage.upgradeToFullFlatDbMode();
    return storage;
  }

  private Hash persistAccountTrieState(
      final WorldStateStorageCoordinator coordinator,
      final BonsaiWorldStateKeyValueStorage storage,
      final Hash accountHash,
      final PmtStateTrieAccountValue accountValue) {
    final StoredMerklePatriciaTrie<Bytes, Bytes> accountTrie =
        new StoredMerklePatriciaTrie<>(
            coordinator::getAccountStateTrieNode, Function.identity(), Function.identity());
    accountTrie.put(accountHash.getBytes(), RLP.encode(accountValue::writeTo));

    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    accountTrie.commit(updater::putAccountStateTrieNode);
    updater.commit();

    return Hash.wrap(accountTrie.getRootHash());
  }

  private void appendBlock(
      final MutableBlockchain blockchain,
      final BlockHeader parent,
      final Hash stateRoot,
      final Optional<BlockAccessList> blockAccessList) {
    final BlockHeader childHeader =
        new BlockHeaderTestFixture()
            .number(parent.getNumber() + 1)
            .parentHash(parent.getHash())
            .stateRoot(stateRoot)
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildHeader();
    blockchain.appendBlock(new Block(childHeader, BlockBody.empty()), List.of(), blockAccessList);
  }

  private MutableBlockchain createBlockchainWithGenesis(final Hash stateRoot) {
    final Block genesis =
        new Block(
            new BlockHeaderTestFixture()
                .number(0)
                .stateRoot(stateRoot)
                .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
                .buildHeader(),
            BlockBody.empty());
    return InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesis);
  }
}
