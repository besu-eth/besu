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
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

public class DownloadAndPersistBlockAccessListsStep
    implements Function<
        List<SyncBlockWithReceipts>, CompletableFuture<List<SyncBlockWithReceipts>>> {

  private final DefaultBlockchain blockchain;
  private final Duration timeoutDuration;
  private final Function<List<BlockHeader>, CompletableFuture<List<SyncBlockAccessList>>>
      blockAccessListDownloader;

  public DownloadAndPersistBlockAccessListsStep(
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final DefaultBlockchain blockchain,
      final Duration timeoutDuration) {
    this(
        blockchain,
        timeoutDuration,
        balEnabledHeaders ->
            new RetryingGetBlockAccessListsFromPeerTask(
                    ethContext, balEnabledHeaders, metricsSystem)
                .run());
  }

  @VisibleForTesting
  DownloadAndPersistBlockAccessListsStep(
      final DefaultBlockchain blockchain,
      final Duration timeoutDuration,
      final Function<List<BlockHeader>, CompletableFuture<List<SyncBlockAccessList>>>
          blockAccessListDownloader) {
    this.blockchain = blockchain;
    this.timeoutDuration = timeoutDuration;
    this.blockAccessListDownloader = blockAccessListDownloader;
  }

  @Override
  public CompletableFuture<List<SyncBlockWithReceipts>> apply(
      final List<SyncBlockWithReceipts> syncBlocksWithReceipts) {
    final List<BlockHeader> headers =
        syncBlocksWithReceipts.stream().map(SyncBlockWithReceipts::getHeader).toList();
    validateAllHeadersBalEnabled(headers);

    if (headers.isEmpty()) {
      return CompletableFuture.completedFuture(syncBlocksWithReceipts);
    }

    final CompletableFuture<List<SyncBlockAccessList>> downloadFuture =
        blockAccessListDownloader
            .apply(headers)
            .orTimeout(timeoutDuration.toMillis(), TimeUnit.MILLISECONDS);

    return downloadFuture.thenApply(
        blockAccessLists -> {
          validateRequiredBlockAccessLists(headers, blockAccessLists);
          persistBlockAccessLists(headers, blockAccessLists);
          return syncBlocksWithReceipts;
        });
  }

  private void validateAllHeadersBalEnabled(final List<BlockHeader> headers) {
    final long missingBalHashCount =
        headers.stream().filter(header -> header.getBalHash().isEmpty()).count();
    if (missingBalHashCount == 0) {
      return;
    }

    final BlockHeader firstMissingBalHashHeader =
        headers.stream().filter(header -> header.getBalHash().isEmpty()).findFirst().orElseThrow();
    throw new IllegalArgumentException(
        "Expected all supplied headers to be BAL-enabled, but "
            + missingBalHashCount
            + " of "
            + headers.size()
            + " headers are missing BAL hashes. First missing header: block "
            + firstMissingBalHashHeader.getNumber()
            + " ("
            + firstMissingBalHashHeader.getHash()
            + ")");
  }

  private void validateRequiredBlockAccessLists(
      final List<BlockHeader> balEnabledHeaders,
      final List<SyncBlockAccessList> syncBlockAccessLists) {
    if (syncBlockAccessLists == null) {
      throw new IllegalStateException(
          "Missing block access lists for " + balEnabledHeaders.size() + " BAL-enabled headers");
    }
    if (syncBlockAccessLists.size() < balEnabledHeaders.size()) {
      throw new IllegalStateException(
          "Downloaded "
              + syncBlockAccessLists.size()
              + " block access lists for "
              + balEnabledHeaders.size()
              + " BAL-enabled headers");
    }
    for (int i = 0; i < balEnabledHeaders.size(); i++) {
      final SyncBlockAccessList syncBlockAccessList = syncBlockAccessLists.get(i);
      if (syncBlockAccessList == null || syncBlockAccessList.isUnavailable()) {
        throw new IllegalStateException(
            "Missing required block access list for block "
                + balEnabledHeaders.get(i).getNumber()
                + " ("
                + balEnabledHeaders.get(i).getHash()
                + ")");
      }
    }
  }

  private void persistBlockAccessLists(
      final List<BlockHeader> balEnabledHeaders,
      final List<SyncBlockAccessList> syncBlockAccessLists) {
    final BlockchainStorage.Updater updater = blockchain.getBlockchainStorage().updater();
    for (int i = 0; i < balEnabledHeaders.size(); i++) {
      final BlockHeader header = balEnabledHeaders.get(i);
      final SyncBlockAccessList syncBlockAccessList = syncBlockAccessLists.get(i);
      updater.putSyncBlockAccessList(header.getHash(), syncBlockAccessList);
    }
    updater.commit();
  }
}
