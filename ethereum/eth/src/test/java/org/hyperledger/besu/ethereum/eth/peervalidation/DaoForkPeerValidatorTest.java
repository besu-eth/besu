/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.peervalidation;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderValidator;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class DaoForkPeerValidatorTest extends AbstractPeerBlockValidatorTest {

  @Override
  AbstractPeerBlockValidator createValidator(
      final PeerTaskExecutor peerTaskExecutor, final long blockNumber, final long buffer) {
    return new DaoForkPeerValidator(
        ProtocolScheduleFixture.TESTING_NETWORK, peerTaskExecutor, blockNumber, buffer);
  }

  @Test
  public void validatePeer_responsivePeerOnRightSideOfFork() {
    final EthProtocolManager ethProtocolManager = EthProtocolManagerTestBuilder.builder().build();
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final long daoBlockNumber = 500;
    final Block daoBlock =
        gen.block(
            BlockOptions.create()
                .setBlockNumber(daoBlockNumber)
                .setExtraData(MainnetBlockHeaderValidator.DAO_EXTRA_DATA));

    final PeerValidator validator =
        new DaoForkPeerValidator(
            ProtocolScheduleFixture.TESTING_NETWORK, peerTaskExecutor, daoBlockNumber, 0);

    final EthPeer peer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, daoBlockNumber).getEthPeer();

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peer)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(daoBlock.getHeader())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer)));

    final CompletableFuture<Boolean> result =
        validator.validatePeer(ethProtocolManager.ethContext(), peer);

    ArgumentCaptor<GetHeadersFromPeerTask> getHeadersTaskCaptor =
        ArgumentCaptor.forClass(GetHeadersFromPeerTask.class);
    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(getHeadersTaskCaptor.capture(), Mockito.eq(peer));
    assertThat(getHeadersTaskCaptor.getValue().getBlockNumber()).isEqualTo(daoBlockNumber);
    assertThat(result).isDone();
    assertThat(result).isCompletedWithValue(true);
  }

  @Test
  public void validatePeer_responsivePeerOnWrongSideOfFork() {
    final EthProtocolManager ethProtocolManager = EthProtocolManagerTestBuilder.builder().build();
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final long daoBlockNumber = 500;
    final Block daoBlock =
        gen.block(BlockOptions.create().setBlockNumber(daoBlockNumber).setExtraData(Bytes.EMPTY));

    final PeerValidator validator =
        new DaoForkPeerValidator(
            ProtocolScheduleFixture.TESTING_NETWORK, peerTaskExecutor, daoBlockNumber, 0);

    final EthPeer peer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, daoBlockNumber).getEthPeer();

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peer)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(daoBlock.getHeader())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer)));

    final CompletableFuture<Boolean> result =
        validator.validatePeer(ethProtocolManager.ethContext(), peer);

    ArgumentCaptor<GetHeadersFromPeerTask> getHeadersTaskCaptor =
        ArgumentCaptor.forClass(GetHeadersFromPeerTask.class);
    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(getHeadersTaskCaptor.capture(), Mockito.eq(peer));
    assertThat(getHeadersTaskCaptor.getValue().getBlockNumber()).isEqualTo(daoBlockNumber);
    assertThat(result).isDone();
    assertThat(result).isCompletedWithValue(false);
  }

  @Test
  public void validatePeer_responsivePeerDoesNotHaveBlockWhenPastForkHeight() {
    final EthProtocolManager ethProtocolManager = EthProtocolManagerTestBuilder.builder().build();
    final long daoBlockNumber = 500;

    final PeerValidator validator =
        new DaoForkPeerValidator(
            ProtocolScheduleFixture.TESTING_NETWORK, peerTaskExecutor, daoBlockNumber, 0);

    final EthPeer peer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, daoBlockNumber).getEthPeer();

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peer)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(Collections.emptyList()),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peer)));

    final CompletableFuture<Boolean> result =
        validator.validatePeer(ethProtocolManager.ethContext(), peer);

    assertThat(result).isDone();
    assertThat(result).isCompletedWithValue(true);
  }
}
