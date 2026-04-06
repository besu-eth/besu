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
package org.hyperledger.besu.ethereum.eth.sync.common;

import static java.util.Collections.emptyList;
import static org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode.SUCCESS;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask.Request;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask.Response;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadSyncReceiptsStep
    implements Function<List<SyncBlock>, CompletableFuture<List<SyncBlockWithReceipts>>> {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadSyncReceiptsStep.class);
  private static final Duration RETRY_DELAY = Duration.ofSeconds(1);
  private static final AtomicInteger taskSequence = new AtomicInteger(0);

  private final ProtocolSchedule protocolSchedule;
  private final EthScheduler ethScheduler;
  private final PeerTaskExecutor peerTaskExecutor;
  private final SyncTransactionReceiptEncoder syncTransactionReceiptEncoder;
  private final Duration timeoutDuration;

  public DownloadSyncReceiptsStep(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final SyncTransactionReceiptEncoder syncTransactionReceiptEncoder,
      final Duration timeoutDuration) {
    this.protocolSchedule = protocolSchedule;
    this.ethScheduler = ethContext.getScheduler();
    this.peerTaskExecutor = ethContext.getPeerTaskExecutor();
    this.syncTransactionReceiptEncoder = syncTransactionReceiptEncoder;
    this.timeoutDuration = timeoutDuration;
  }

  @Override
  public CompletableFuture<List<SyncBlockWithReceipts>> apply(final List<SyncBlock> blocks) {
    final int currTaskId = taskSequence.incrementAndGet();
    final List<SyncBlock> blocksToRequest = prepareRequest(blocks);
    final List<SyncTransactionReceipt> firstBlockPartialReceipts = new ArrayList<>();
    final Map<Hash, List<SyncTransactionReceipt>> receiptsByRootHash =
        HashMap.newHashMap(blocksToRequest.size());
    final AtomicBoolean cancelled = new AtomicBoolean(false);

    return ethScheduler
        .scheduleServiceTask(
            () ->
                downloadReceipts(
                    currTaskId,
                    0,
                    blocksToRequest,
                    firstBlockPartialReceipts,
                    receiptsByRootHash,
                    cancelled))
        .thenApply(receipts -> combineBlocksAndReceipts(blocks, receipts))
        .orTimeout(timeoutDuration.toMillis(), TimeUnit.MILLISECONDS)
        .whenComplete(
            (unused, throwable) -> {
              if (throwable instanceof TimeoutException) {
                cancelled.set(true);
                LOG.trace(
                    "[{}] Timed out after {} ms while downloading receipts for {} blocks",
                    currTaskId,
                    timeoutDuration.toMillis(),
                    blocks.size());
              }
            });
  }

  private List<SyncBlock> prepareRequest(final List<SyncBlock> blocks) {
    // filter any headers with an empty receipts root, for which we do not need to request anything
    final List<SyncBlock> blocksWithTxs =
        blocks.stream()
            .filter(
                sb -> {
                  if (sb.getHeader()
                      .getReceiptsRoot()
                      .getBytes()
                      .equals(Hash.EMPTY_TRIE_HASH.getBytes())) {
                    LOG.trace(
                        "Skipping receipts retrieval for empty block {}",
                        sb.getHeader().getNumber());
                    return false;
                  }
                  return true;
                })
            .collect(Collectors.toCollection(ArrayList::new));

    // dedup by grouping blocks by same receipt root hash
    final List<SyncBlock> blocksToRequest =
        blocksWithTxs.stream()
            .collect(
                Collectors.groupingBy(
                    syncBlock -> syncBlock.getHeader().getReceiptsRoot(),
                    LinkedHashMap::new,
                    Collectors.toList()))
            .values()
            .stream()
            .map(List::getFirst)
            .collect(Collectors.toCollection(ArrayList::new));

    LOG.trace("Saving by dedup {}", blocksWithTxs.size() - blocksToRequest.size());
    return blocksToRequest;
  }

  private CompletableFuture<Map<Hash, List<SyncTransactionReceipt>>> downloadReceipts(
      final int currTaskId,
      final int prevIterations,
      final List<SyncBlock> blocksToRequest,
      final List<SyncTransactionReceipt> firstBlockPartialReceipts,
      final Map<Hash, List<SyncTransactionReceipt>> receiptsByRootHash,
      final AtomicBoolean cancelled) {

    final int initialBlockCount = blocksToRequest.size();
    int iteration = prevIterations;
    while (!cancelled.get() && !blocksToRequest.isEmpty()) {
      ++iteration;

      LOG.atTrace()
          .setMessage(
              "[{}:{}] Requesting receipts for {} blocks, partial receipts fetched for first block {} (initial {}): {}")
          .addArgument(currTaskId)
          .addArgument(iteration)
          .addArgument(blocksToRequest::size)
          .addArgument(firstBlockPartialReceipts::size)
          .addArgument(initialBlockCount)
          .addArgument(() -> formatBlockDetails(blocksToRequest))
          .log();

      final GetSyncReceiptsFromPeerTask task =
          new GetSyncReceiptsFromPeerTask(
              new Request(blocksToRequest, firstBlockPartialReceipts),
              protocolSchedule,
              syncTransactionReceiptEncoder);

      final PeerTaskExecutorResult<Response> getReceiptsResult = peerTaskExecutor.execute(task);

      final PeerTaskExecutorResponseCode responseCode = getReceiptsResult.responseCode();

      if (responseCode == SUCCESS) {
        final Response response =
            getReceiptsResult
                .result()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Task validation failure, it must flag empty result as failure"));

        final Map<SyncBlock, List<SyncTransactionReceipt>> receiptsByBlock =
            response.completeReceiptsByBlock();
        final List<SyncTransactionReceipt> lastBlockPartialReceipts =
            response.lastBlockPartialReceipts();

        firstBlockPartialReceipts.clear();
        if (!lastBlockPartialReceipts.isEmpty()) {
          firstBlockPartialReceipts.addAll(lastBlockPartialReceipts);
        }

        LOG.atTrace()
            .setMessage(
                "[{}:{}] Received complete response for {} blocks, last block partial receipts {}, completed blocks {} (requested {}, initial {}): {}")
            .addArgument(currTaskId)
            .addArgument(iteration)
            .addArgument(response::completeCount)
            .addArgument(lastBlockPartialReceipts::size)
            .addArgument(receiptsByBlock::size)
            .addArgument(blocksToRequest::size)
            .addArgument(initialBlockCount)
            .addArgument(() -> formatBlockDetails(receiptsByBlock.keySet()))
            .log();

        receiptsByBlock.forEach(
            (block, receipts) -> {
              receiptsByRootHash.put(block.getHeader().getReceiptsRoot(), receipts);
              blocksToRequest.remove(block);
            });
      } else {
        LOG.atTrace()
            .setMessage(
                "[{}:{}] Failed with {} to retrieve receipts for {} blocks (initial {}): {}")
            .addArgument(currTaskId)
            .addArgument(iteration)
            .addArgument(responseCode)
            .addArgument(blocksToRequest::size)
            .addArgument(initialBlockCount)
            .addArgument(() -> formatBlockDetails(blocksToRequest))
            .log();

        LOG.trace("[{}:{}] Waiting for {} before retrying", currTaskId, iteration, RETRY_DELAY);
        final int passIterations = iteration;
        return ethScheduler.scheduleFutureTask(
            () ->
                ethScheduler.scheduleServiceTask(
                    () ->
                        downloadReceipts(
                            currTaskId,
                            passIterations,
                            blocksToRequest,
                            firstBlockPartialReceipts,
                            receiptsByRootHash,
                            cancelled)),
            RETRY_DELAY);
      }
    }
    return CompletableFuture.completedFuture(receiptsByRootHash);
  }

  List<SyncBlockWithReceipts> combineBlocksAndReceipts(
      final List<SyncBlock> blocks,
      final Map<Hash, List<SyncTransactionReceipt>> receiptsByRootHash) {
    return blocks.stream()
        .map(
            block -> {
              final List<SyncTransactionReceipt> receipts =
                  receiptsByRootHash.getOrDefault(block.getHeader().getReceiptsRoot(), emptyList());
              if (block.getBody().getTransactionCount() != receipts.size()) {
                throw new IllegalStateException(
                    "PeerTask response code was success, but incorrect number of receipts returned. Block hash: "
                        + block.getHeader().getHash()
                        + ", transactions: "
                        + block.getBody().getTransactionCount()
                        + ", receipts: "
                        + receipts.size());
              }
              return new SyncBlockWithReceipts(block, receipts);
            })
        .toList();
  }

  private String formatBlockDetails(final Collection<SyncBlock> blocks) {
    return blocks.stream()
        .map(sb -> sb.getHeader().getNumber() + "(" + sb.getBody().getTransactionCount() + ")")
        .collect(Collectors.joining(","));
  }
}
