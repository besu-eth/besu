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

import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadAndPersistBlockAccessListsStep
    implements Function<
        List<SyncBlockWithReceipts>, CompletableFuture<List<SyncBlockWithReceipts>>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DownloadAndPersistBlockAccessListsStep.class);

  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;
  private final DefaultBlockchain blockchain;
  private final Duration timeoutDuration;

  public DownloadAndPersistBlockAccessListsStep(
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final DefaultBlockchain blockchain,
      final Duration timeoutDuration) {
    this.ethContext = ethContext;
    this.metricsSystem = metricsSystem;
    this.blockchain = blockchain;
    this.timeoutDuration = timeoutDuration;
  }

  @Override
  public CompletableFuture<List<SyncBlockWithReceipts>> apply(
      final List<SyncBlockWithReceipts> syncBlocksWithReceipts) {
    final List<BlockHeader> balEnabledHeaders =
        syncBlocksWithReceipts.stream()
            .map(SyncBlockWithReceipts::getHeader)
            .filter(header -> header.getBalHash().isPresent())
            .toList();

    if (balEnabledHeaders.isEmpty()) {
      return CompletableFuture.completedFuture(syncBlocksWithReceipts);
    }

    return RetryingGetBlockAccessListsFromPeerTask.forBlockAccessLists(
            ethContext, balEnabledHeaders, metricsSystem)
        .run()
        .orTimeout(timeoutDuration.toMillis(), TimeUnit.MILLISECONDS)
        .handle(
            (blockAccessLists, error) -> {
              if (error != null) {
                LOG.warn(
                    "Failed to download block access lists for {} headers; proceeding without them",
                    balEnabledHeaders.size(),
                    error);
                return syncBlocksWithReceipts;
              }
              if (blockAccessLists == null) {
                LOG.warn(
                    "Received null block access list response for {} headers; proceeding without them",
                    balEnabledHeaders.size());
                return syncBlocksWithReceipts;
              }
              persistBlockAccessLists(balEnabledHeaders, blockAccessLists);
              return syncBlocksWithReceipts;
            });
  }

  private void persistBlockAccessLists(
      final List<BlockHeader> balEnabledHeaders, final List<BlockAccessList> blockAccessLists) {
    final int persistedCount = Math.min(balEnabledHeaders.size(), blockAccessLists.size());
    if (persistedCount != balEnabledHeaders.size()) {
      LOG.warn(
          "Downloaded {} block access lists for {} BAL-enabled headers; persisting available subset",
          blockAccessLists.size(),
          balEnabledHeaders.size());
    }
    final BlockchainStorage.Updater updater = blockchain.getBlockchainStorage().updater();
    for (int i = 0; i < persistedCount; i++) {
      final BlockHeader header = balEnabledHeaders.get(i);
      try {
        updater.putBlockAccessList(header.getHash(), blockAccessLists.get(i));
      } catch (final Exception exception) {
        LOG.warn(
            "Failed to persist block access list for block {} ({}); continuing with remaining BALs",
            header.getNumber(),
            header.getHash(),
            exception);
      }
    }
    updater.commit();
  }
}
