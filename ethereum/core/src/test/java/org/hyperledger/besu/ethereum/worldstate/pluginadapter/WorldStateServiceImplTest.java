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
package org.hyperledger.besu.ethereum.worldstate.pluginadapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class WorldStateServiceImplTest {

  private final Blockchain blockchain = mock(Blockchain.class);
  private final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
  private final MutableWorldState worldState = mock(MutableWorldState.class);
  private final BlockHeader header = new BlockHeaderTestFixture().number(7).buildHeader();
  private final Hash blockHash = header.getHash();
  private WorldStateServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new WorldStateServiceImpl(worldStateArchive, blockchain);
  }

  @Test
  void mapperIsAppliedToTheWorldStateOfTheBlockWhichIsThenClosed() throws Exception {
    final Address address = Address.fromHexString("0x01");
    final Account account = mock(Account.class);
    when(account.getBalance()).thenReturn(Wei.of(42));
    when(worldState.get(address)).thenReturn(account);
    when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)))
        .thenReturn(Optional.of(worldState));

    final Optional<Wei> balance =
        service.mapWorldView(
            blockHash,
            worldView -> Optional.ofNullable(worldView.get(address)).map(a -> a.getBalance()));

    assertThat(balance).contains(Wei.of(42));
    final InOrder inOrder = inOrder(worldState);
    inOrder.verify(worldState).get(address);
    inOrder.verify(worldState).close();
  }

  @Test
  void unknownBlockIsEmptyWithoutOpeningWorldState() {
    when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.empty());
    final Function<WorldView, Optional<Integer>> mapper = worldView -> Optional.of(1);

    assertThat(service.mapWorldView(blockHash, mapper)).isEmpty();
    verify(worldStateArchive, never()).getWorldState(any());
  }

  @Test
  void unavailableWorldStateIsEmpty() {
    when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)))
        .thenReturn(Optional.empty());
    final AtomicBoolean mapperCalled = new AtomicBoolean(false);

    assertThat(
            service.mapWorldView(
                blockHash,
                worldView -> {
                  mapperCalled.set(true);
                  return Optional.of(1);
                }))
        .isEmpty();
    assertThat(mapperCalled).isFalse();
  }

  @Test
  void throwingMapperPropagatesAndWorldStateIsClosed() throws Exception {
    when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)))
        .thenReturn(Optional.of(worldState));

    assertThatThrownBy(
            () ->
                service.mapWorldView(
                    blockHash,
                    worldView -> {
                      throw new IllegalStateException("boom");
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("boom");
    verify(worldState).close();
  }

  @Test
  void nullMapperResultIsEmpty() throws Exception {
    when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)))
        .thenReturn(Optional.of(worldState));

    assertThat(service.mapWorldView(blockHash, worldView -> null)).isEmpty();
    verify(worldState).close();
  }

  @Test
  void failureToCloseWorldStateKeepsTheResult() throws Exception {
    when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)))
        .thenReturn(Optional.of(worldState));
    doThrow(new IllegalStateException("close failed")).when(worldState).close();

    assertThat(service.mapWorldView(blockHash, worldView -> Optional.of(1))).contains(1);
  }

  @Test
  void failureToOpenWorldStateIsEmpty() {
    when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)))
        .thenThrow(new IllegalStateException("pruned"));

    assertThat(service.mapWorldView(blockHash, worldView -> Optional.of(1))).isEmpty();
  }

  @Test
  void emptyMapperResultIsEmpty() throws Exception {
    when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)))
        .thenReturn(Optional.of(worldState));

    assertThat(service.mapWorldView(blockHash, worldView -> Optional.empty())).isEmpty();
    verify(worldState).close();
  }
}
