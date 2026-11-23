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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockWithAccessList;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.eth.core.Utils;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DownloadBlockDataStepTest {

  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final List<BlockWithAccessList> blocks = new ArrayList<>();
  private List<SyncBlock> syncBlocks;

  @Mock private EthContext ethContext;
  @Mock private PeerTaskExecutor peerTaskExecutor;

  @BeforeEach
  void setUp() {
    when(ethContext.getScheduler()).thenReturn(new DeterministicEthScheduler());
    when(ethContext.getPeerTaskExecutor()).thenReturn(peerTaskExecutor);
    Block parent = generator.genesisBlock();
    for (int number = 1; number <= 3; number++) {
      final BlockWithAccessList block =
          generator.blockWithAccessList(
              new BlockOptions()
                  .setBlockNumber(number)
                  .setParentHash(parent.getHash())
                  .withGeneratedBlockAccessList());
      blocks.add(block);
      parent = block.getBlock();
    }
    syncBlocks =
        Utils.blocksToSyncBlocks(blocks.stream().map(BlockWithAccessList::getBlock).toList());
  }

  @Test
  void downloadsTheBalsAndDecodesTheBlocks() {
    balsAnswered(List.of(bal(0), bal(1), bal(2)));

    final BlockBatch batch = step().apply(syncBlocks).join();

    assertThat(batch.blockAccessLists())
        .containsOnlyKeys(hash(0), hash(1), hash(2))
        .containsEntry(hash(2), bal(2).orElseThrow());
    assertThat(batch.blocks())
        .extracting(Block::getHash)
        .containsExactly(hash(0), hash(1), hash(2));
    assertThat(batch.blocks().get(2).getBody().getTransactions())
        .isEqualTo(blocks.get(2).getBlock().getBody().getTransactions());
  }

  @Test
  void asksAgainForTheBalsAResponseLeftOut() {
    balsAnswered(List.of(bal(0)), List.of(bal(1), bal(2)));

    final BlockBatch batch = step().apply(syncBlocks).join();

    assertThat(batch.blockAccessLists()).containsOnlyKeys(hash(0), hash(1), hash(2));
    verify(peerTaskExecutor, times(2)).execute(any(GetBlockAccessListsFromPeerTask.class));
  }

  @Test
  void continuesWithoutTheBalsThatFailedToDownload() {
    when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, List.of()));

    final BlockBatch batch = step().apply(syncBlocks).join();

    assertThat(batch.blockAccessLists()).isEmpty();
    assertThat(batch.blocks()).hasSize(3);
  }

  private DownloadBlockDataStep step() {
    return new DownloadBlockDataStep(ethContext);
  }

  @SafeVarargs
  private void balsAnswered(final List<Optional<BlockAccessList>>... responses) {
    var stubbing = when(peerTaskExecutor.execute(any(GetBlockAccessListsFromPeerTask.class)));
    for (final List<Optional<BlockAccessList>> response : responses) {
      stubbing =
          stubbing.thenReturn(
              new PeerTaskExecutorResult<>(
                  Optional.of(response), PeerTaskExecutorResponseCode.SUCCESS, List.of()));
    }
  }

  private Optional<BlockAccessList> bal(final int index) {
    return blocks.get(index).getBlockAccessList();
  }

  private Hash hash(final int index) {
    return blocks.get(index).getBlock().getHash();
  }
}
