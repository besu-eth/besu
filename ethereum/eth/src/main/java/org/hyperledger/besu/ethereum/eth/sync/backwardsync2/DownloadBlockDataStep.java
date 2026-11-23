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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads, next to the bodies of a batch, the block access lists of the blocks that have one,
 * which their execution uses, and decodes the bodies. Best effort, a block without one is executed
 * all the same.
 */
class DownloadBlockDataStep implements Function<List<SyncBlock>, CompletableFuture<BlockBatch>> {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadBlockDataStep.class);

  private final EthContext ethContext;

  DownloadBlockDataStep(final EthContext ethContext) {
    this.ethContext = ethContext;
  }

  @Override
  public CompletableFuture<BlockBatch> apply(final List<SyncBlock> syncBlocks) {
    final CompletableFuture<Map<Hash, BlockAccessList>> bals =
        ethContext
            .getScheduler()
            .scheduleServiceTask(
                () -> CompletableFuture.completedFuture(downloadBals(headers(syncBlocks))));
    return bals.exceptionally(error -> failed("block access lists", error))
        .thenApply(balsByHash -> new BlockBatch(decodeBlocks(syncBlocks), balsByHash));
  }

  private Map<Hash, BlockAccessList> downloadBals(final List<BlockHeader> headers) {
    final List<BlockHeader> remaining =
        headers.stream()
            .filter(header -> header.getBalHash().isPresent())
            .collect(Collectors.toCollection(ArrayList::new));
    final Map<Hash, BlockAccessList> bals = HashMap.newHashMap(remaining.size());
    // a response may stop early, the rest is asked again until a request brings nothing
    boolean progress = true;
    while (!remaining.isEmpty() && progress) {
      final PeerTaskExecutorResult<List<Optional<BlockAccessList>>> result =
          ethContext
              .getPeerTaskExecutor()
              .execute(new GetBlockAccessListsFromPeerTask(List.copyOf(remaining)));
      final List<Optional<BlockAccessList>> response =
          result.responseCode() == PeerTaskExecutorResponseCode.SUCCESS
              ? result.result().orElse(List.of())
              : List.of();
      final int answered = Math.min(response.size(), remaining.size());
      for (int i = 0; i < answered; i++) {
        final Hash hash = remaining.get(i).getHash();
        response.get(i).ifPresent(bal -> bals.put(hash, bal));
      }
      remaining.subList(0, answered).clear();
      progress = answered > 0;
    }
    return bals;
  }

  private static <T> Map<Hash, T> failed(final String what, final Throwable error) {
    LOG.atDebug()
        .setMessage("Downloading {} failed ({}), the blocks without are executed")
        .addArgument(what)
        .addArgument(error::toString)
        .log();
    return Map.of();
  }

  private static List<BlockHeader> headers(final List<SyncBlock> syncBlocks) {
    return syncBlocks.stream().map(SyncBlock::getHeader).toList();
  }

  private static List<Block> decodeBlocks(final List<SyncBlock> syncBlocks) {
    return syncBlocks.stream()
        .map(block -> new Block(block.getHeader(), block.getBody().getBodySupplier().get()))
        .toList();
  }
}
