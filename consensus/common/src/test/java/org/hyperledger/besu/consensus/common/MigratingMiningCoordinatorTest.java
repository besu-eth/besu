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
package org.hyperledger.besu.consensus.common;

import static java.util.Collections.emptyList;
import static org.hyperledger.besu.ethereum.core.BlockHeader.GENESIS_BLOCK_NUMBER;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftEventQueue;
import org.hyperledger.besu.consensus.common.bft.BftExecutors;
import org.hyperledger.besu.consensus.common.bft.BftProcessor;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftBlockCreatorFactory;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftMiningCoordinator;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftEventHandler;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MigratingMiningCoordinatorTest {

  @Mock private BftMiningCoordinator coordinator1;
  @Mock private BftMiningCoordinator coordinator2;
  @Mock private Blockchain blockchain;
  @Mock private BlockHeader blockHeader;
  @Mock private BlockBody blockBody;
  private BlockAddedEvent blockEvent;
  private ForksSchedule<MiningCoordinator> coordinatorSchedule;
  private static final long MIGRATION_BLOCK_NUMBER = 5L;

  @BeforeEach
  public void setup() {
    coordinatorSchedule = createCoordinatorSchedule(coordinator1, coordinator2);
    final Block block = new Block(blockHeader, blockBody);
    blockEvent = BlockAddedEvent.createForHeadAdvancement(block, emptyList(), emptyList());
  }

  private ForksSchedule<MiningCoordinator> createCoordinatorSchedule(
      final MiningCoordinator genesisCoordinator, final MiningCoordinator migrateToCoordinator) {
    final ForkSpec<MiningCoordinator> genesisFork =
        new ForkSpec<>(GENESIS_BLOCK_NUMBER, genesisCoordinator);
    final ForkSpec<MiningCoordinator> migrationFork =
        new ForkSpec<>(MIGRATION_BLOCK_NUMBER, migrateToCoordinator);
    return new ForksSchedule<>(List.of(genesisFork, migrationFork));
  }

  @Test
  public void startShouldRegisterThisCoordinatorAsObserver() {
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);
    final MigratingMiningCoordinator coordinator =
        new MigratingMiningCoordinator(coordinatorSchedule, blockchain);

    coordinator.start();

    verify(blockchain).observeBlockAdded(coordinator);
  }

  @Test
  public void startShouldUnregisterDelegateCoordinatorAsObserver() {
    final BftMiningCoordinator delegateCoordinator = createDelegateCoordinator();
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);
    lenient().when(blockchain.observeBlockAdded(delegateCoordinator)).thenReturn(1L);
    final MigratingMiningCoordinator coordinator =
        new MigratingMiningCoordinator(
            createCoordinatorSchedule(delegateCoordinator, coordinator2), blockchain);

    coordinator.start();
    verify(blockchain).observeBlockAdded(coordinator);
    verify(blockchain).observeBlockAdded(delegateCoordinator);
    verify(blockchain).removeObserver(1L);
  }

  private BftMiningCoordinator createDelegateCoordinator() {
    return new BftMiningCoordinator(
        mock(BftExecutors.class),
        mock(BftEventHandler.class),
        mock(BftProcessor.class),
        mock(BftBlockCreatorFactory.class),
        blockchain,
        mock(BftEventQueue.class));
  }

  @Test
  public void stopShouldUnregisterThisCoordinatorAsObserver() {
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);
    final MigratingMiningCoordinator coordinator =
        new MigratingMiningCoordinator(coordinatorSchedule, blockchain);
    when(blockchain.observeBlockAdded(coordinator)).thenReturn(1L);

    coordinator.start();
    coordinator.stop();

    verify(blockchain).removeObserver(1L);
  }

  @Test
  public void onBlockAddedShouldNotDelegateWhenDelegateIsNoop() {
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);
    MiningCoordinator mockMiningCoordinator = mock(MiningCoordinator.class);
    coordinatorSchedule = createCoordinatorSchedule(mockMiningCoordinator, coordinator2);
    when(blockHeader.getNumber()).thenReturn(GENESIS_BLOCK_NUMBER);

    new MigratingMiningCoordinator(coordinatorSchedule, blockchain).onBlockAdded(blockEvent);

    verifyNoInteractions(mockMiningCoordinator);
  }

  @Test
  public void delegatesToActiveMiningCoordinator() {
    verifyDelegation(MiningCoordinator::start, GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegation(MiningCoordinator::start, MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);

    verifyDelegation(MiningCoordinator::stop, GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegation(MiningCoordinator::stop, MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);

    verifyDelegation(MiningCoordinator::enable, GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegation(MiningCoordinator::enable, MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);

    verifyDelegation(MiningCoordinator::disable, GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegation(
        MiningCoordinator::disable, MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);

    verifyDelegation(MiningCoordinator::isMining, GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegation(
        MiningCoordinator::isMining, MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);

    verifyDelegation(
        MiningCoordinator::getMinTransactionGasPrice,
        GENESIS_BLOCK_NUMBER,
        coordinator1,
        coordinator2);
    verifyDelegation(
        MiningCoordinator::getMinTransactionGasPrice,
        MIGRATION_BLOCK_NUMBER,
        coordinator2,
        coordinator1);

    verifyDelegation(
        MiningCoordinator::getCoinbase, GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegation(
        MiningCoordinator::getCoinbase, MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);

    verifyDelegation(
        c -> c.createBlock(blockHeader, emptyList(), emptyList()),
        GENESIS_BLOCK_NUMBER,
        coordinator1,
        coordinator2);
    verifyDelegation(
        c -> c.createBlock(blockHeader, emptyList(), emptyList()),
        MIGRATION_BLOCK_NUMBER,
        coordinator2,
        coordinator1);

    verifyDelegation(
        c -> c.changeTargetGasLimit(1L), GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegation(
        c -> c.changeTargetGasLimit(1L), MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);

    verifyDelegation(
        c -> c.changeTargetGasLimit(1L), GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegation(
        c -> c.changeTargetGasLimit(1L), MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);
  }

  private void verifyDelegation(
      final Consumer<MiningCoordinator> methodUnderTest,
      final long blockHeight,
      final MiningCoordinator expectedActiveCoordinator,
      final MiningCoordinator expectedInactiveCoordinator) {
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);
    when(blockHeader.getNumber()).thenReturn(blockHeight);

    methodUnderTest.accept(new MigratingMiningCoordinator(coordinatorSchedule, blockchain));

    methodUnderTest.accept(verify(expectedActiveCoordinator));
    verifyNoInteractions(expectedInactiveCoordinator);
    reset(coordinator1, coordinator2);
  }

  @Test
  public void verifyDelegationForAwaitStop() throws InterruptedException {
    verifyDelegationForAwaitStop(GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegationForAwaitStop(MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);
  }

  private void verifyDelegationForAwaitStop(
      final long blockHeight,
      final MiningCoordinator expectedActiveCoordinator,
      final MiningCoordinator expectedInactiveCoordinator)
      throws InterruptedException {
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);
    when(blockHeader.getNumber()).thenReturn(blockHeight);

    new MigratingMiningCoordinator(coordinatorSchedule, blockchain).awaitStop();

    verify(expectedActiveCoordinator).awaitStop();
    verifyNoInteractions(expectedInactiveCoordinator);
    reset(coordinator1, coordinator2);
  }

  @Test
  public void verifyDelegationForOnBlockAdded() {
    verifyDelegationForOnBlockAdded(GENESIS_BLOCK_NUMBER, coordinator1, coordinator2);
    verifyDelegationForOnBlockAdded(MIGRATION_BLOCK_NUMBER, coordinator2, coordinator1);
  }

  private void verifyDelegationForOnBlockAdded(
      final long blockHeight,
      final BftMiningCoordinator expectedActiveCoordinator,
      final BftMiningCoordinator expectedInactiveCoordinator) {
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);
    when(blockHeader.getNumber()).thenReturn(blockHeight);

    new MigratingMiningCoordinator(coordinatorSchedule, blockchain).onBlockAdded(blockEvent);

    verify(expectedActiveCoordinator).onBlockAdded(blockEvent);
    verifyNoInteractions(expectedInactiveCoordinator);
    reset(coordinator1, coordinator2);
  }
}
