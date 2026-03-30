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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.messages.snap.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.ImmutableSnapSyncConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapServerBlockAccessListsTest {

  private static final int SNAP_MAX_RESPONSE_SIZE = 2 * 1024 * 1024;
  private static final int SNAP_MAX_ENTRIES_PER_REQUEST = 100_000;

  private final BlockDataGenerator dataGenerator = new BlockDataGenerator();

  private MutableBlockchain blockchain;
  private SnapServer snapServer;

  @BeforeEach
  void setup() {
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        mock(WorldStateStorageCoordinator.class);
    final WorldStateKeyValueStorage worldStateStorage = mock(WorldStateKeyValueStorage.class);
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    final BonsaiWorldStateProvider worldStateArchive = mock(BonsaiWorldStateProvider.class);
    blockchain = mock(MutableBlockchain.class);

    when(worldStateStorageCoordinator.worldStateKeyValueStorage()).thenReturn(worldStateStorage);
    when(worldStateStorage.getDataStorageFormat()).thenReturn(DataStorageFormat.BONSAI);
    when(worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.FULL)).thenReturn(true);
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);

    snapServer =
        new SnapServer(
                ImmutableSnapSyncConfiguration.builder().isSnapServerEnabled(true).build(),
                new EthMessages(),
                worldStateStorageCoordinator,
                protocolContext,
                mock(Synchronizer.class))
            .start();
  }

  @Test
  void shouldIncludeEmptyEntryForUnavailableBlockAccessList() {
    final Hash availableHash = dataGenerator.hash();
    final Hash unavailableHash = dataGenerator.hash();
    final BlockAccessList available = dataGenerator.blockAccessList();

    when(blockchain.getBlockAccessList(availableHash)).thenReturn(Optional.of(available));
    when(blockchain.getBlockAccessList(unavailableHash)).thenReturn(Optional.empty());

    final GetBlockAccessListsMessage request =
        GetBlockAccessListsMessage.create(List.of(availableHash, unavailableHash));

    final BlockAccessListsMessage response =
        (BlockAccessListsMessage)
            snapServer.constructGetBlockAccessListsResponse(
                request.wrapMessageData(BigInteger.ONE));

    assertThat(response.blockAccessLists(false))
        .containsExactly(available, new BlockAccessList(List.of()));
  }

  @Test
  void shouldLimitBlockAccessListsByMessageSize() {
    final Hash firstHash = dataGenerator.hash();
    final Hash secondHash = dataGenerator.hash();

    final BlockAccessList hugeBlockAccessList =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    Address.ZERO,
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(new CodeChange(0, Bytes.wrap(new byte[SNAP_MAX_RESPONSE_SIZE]))))));
    final BlockAccessList secondBlockAccessList = dataGenerator.blockAccessList();

    when(blockchain.getBlockAccessList(firstHash)).thenReturn(Optional.of(hugeBlockAccessList));
    when(blockchain.getBlockAccessList(secondHash)).thenReturn(Optional.of(secondBlockAccessList));

    final GetBlockAccessListsMessage request =
        GetBlockAccessListsMessage.create(List.of(firstHash, secondHash));

    final BlockAccessListsMessage response =
        (BlockAccessListsMessage)
            snapServer.constructGetBlockAccessListsResponse(
                request.wrapMessageData(BigInteger.ONE));

    assertThat(response.blockAccessLists(false)).isEmpty();
    verify(blockchain, never()).getBlockAccessList(secondHash);
  }

  @Test
  void shouldLimitBlockAccessListsByEntryCount() {
    when(blockchain.getBlockAccessList(any())).thenReturn(Optional.empty());

    final List<Hash> hashes = new ArrayList<>(SNAP_MAX_ENTRIES_PER_REQUEST + 1);
    for (int i = 0; i < SNAP_MAX_ENTRIES_PER_REQUEST + 1; i++) {
      hashes.add(dataGenerator.hash());
    }

    final GetBlockAccessListsMessage request = GetBlockAccessListsMessage.create(hashes);
    final Hash hashPastLimit = hashes.get(SNAP_MAX_ENTRIES_PER_REQUEST);

    final BlockAccessListsMessage response =
        (BlockAccessListsMessage)
            snapServer.constructGetBlockAccessListsResponse(
                request.wrapMessageData(BigInteger.ONE));

    assertThat(response.blockAccessLists(false)).hasSize(SNAP_MAX_ENTRIES_PER_REQUEST);
    verify(blockchain, never()).getBlockAccessList(hashPastLimit);
  }

  @Test
  void shouldIncludeBlockAccessListsUpToMessageSizeLimit() {
    final int codeSize = 450_000;
    final BlockAccessList largeBlockAccessList = createBlockAccessListWithCodeSize(codeSize);
    final int encodedSize = calculateRlpEncodedSize(largeBlockAccessList);

    assertThat(SNAP_MAX_RESPONSE_SIZE).isGreaterThanOrEqualTo(4 * encodedSize);
    assertThat(SNAP_MAX_RESPONSE_SIZE).isLessThan(5 * encodedSize);

    final List<Hash> hashes = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      final Hash hash = dataGenerator.hash();
      hashes.add(hash);
      when(blockchain.getBlockAccessList(hash)).thenReturn(Optional.of(largeBlockAccessList));
    }

    final GetBlockAccessListsMessage request = GetBlockAccessListsMessage.create(hashes);

    final BlockAccessListsMessage response =
        (BlockAccessListsMessage)
            snapServer.constructGetBlockAccessListsResponse(
                request.wrapMessageData(BigInteger.ONE));

    assertThat(response.blockAccessLists(false))
        .containsExactly(
            largeBlockAccessList, largeBlockAccessList, largeBlockAccessList, largeBlockAccessList);
  }

  @Test
  void shouldCountUnavailableBlockAccessListsTowardEntryLimit() {
    when(blockchain.getBlockAccessList(any())).thenReturn(Optional.empty());

    final Hash firstAvailableHash = dataGenerator.hash();
    final Hash hashPastLimit = dataGenerator.hash();
    final BlockAccessList firstAvailable = dataGenerator.blockAccessList();

    when(blockchain.getBlockAccessList(firstAvailableHash)).thenReturn(Optional.of(firstAvailable));

    final List<Hash> hashes = new ArrayList<>(SNAP_MAX_ENTRIES_PER_REQUEST + 1);
    hashes.add(firstAvailableHash);

    for (int i = 1; i < SNAP_MAX_ENTRIES_PER_REQUEST; i++) {
      hashes.add(dataGenerator.hash());
    }

    hashes.add(hashPastLimit);

    final GetBlockAccessListsMessage request = GetBlockAccessListsMessage.create(hashes);

    final BlockAccessListsMessage response =
        (BlockAccessListsMessage)
            snapServer.constructGetBlockAccessListsResponse(
                request.wrapMessageData(BigInteger.ONE));

    assertThat(response.blockAccessLists(false)).hasSize(SNAP_MAX_ENTRIES_PER_REQUEST);
    assertThat(response.blockAccessLists(false).getFirst()).isEqualTo(firstAvailable);
    verify(blockchain, never()).getBlockAccessList(hashPastLimit);
  }

  @Test
  void shouldReturnEmptyResponseWhenServerIsStopped() {
    snapServer.stop();

    final Hash hash = dataGenerator.hash();
    final GetBlockAccessListsMessage request = GetBlockAccessListsMessage.create(List.of(hash));

    final BlockAccessListsMessage response =
        (BlockAccessListsMessage)
            snapServer.constructGetBlockAccessListsResponse(
                request.wrapMessageData(BigInteger.ONE));

    assertThat(response.blockAccessLists(false)).isEmpty();
    verify(blockchain, never()).getBlockAccessList(any());
  }

  private BlockAccessList createBlockAccessListWithCodeSize(final int codeSize) {
    return new BlockAccessList(
        List.of(
            new AccountChanges(
                Address.ZERO,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(new CodeChange(0, Bytes.wrap(new byte[codeSize]))))));
  }

  private int calculateRlpEncodedSize(final BlockAccessList blockAccessList) {
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    BlockAccessListEncoder.encode(blockAccessList, rlp);
    return rlp.encodedSize();
  }
}
