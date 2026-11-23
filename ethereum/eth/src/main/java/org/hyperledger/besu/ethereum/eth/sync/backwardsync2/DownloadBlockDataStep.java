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
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads, next to the bodies of a batch, what its import uses: the block access lists of the
 * blocks that have one, and the receipts of the finalized ones, which are imported from their block
 * access list instead of being executed, then decodes the bodies. Both are best effort: the batch
 * waits for them at most {@link #OPTIONAL_DOWNLOAD_TIMEOUT}, a block missing either being executed.
 */
class DownloadBlockDataStep implements Function<List<SyncBlock>, CompletableFuture<BlockBatch>> {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadBlockDataStep.class);

  /** How long a batch waits for what it can be imported without. */
  static final Duration OPTIONAL_DOWNLOAD_TIMEOUT = Duration.ofSeconds(10);

  private final EthContext ethContext;
  private final Function<List<SyncBlock>, CompletableFuture<List<SyncBlockWithReceipts>>>
      receiptsDownload;
  private final long finalizedNumber;

  DownloadBlockDataStep(
      final EthContext ethContext,
      final Function<List<SyncBlock>, CompletableFuture<List<SyncBlockWithReceipts>>>
          receiptsDownload,
      final long finalizedNumber) {
    this.ethContext = ethContext;
    this.receiptsDownload = receiptsDownload;
    this.finalizedNumber = finalizedNumber;
  }

  @Override
  public CompletableFuture<BlockBatch> apply(final List<SyncBlock> syncBlocks) {
    final CompletableFuture<Map<Hash, BlockAccessList>> bals = downloadBals(headers(syncBlocks));
    final CompletableFuture<Map<Hash, List<TransactionReceipt>>> receipts =
        downloadReceipts(syncBlocks);
    return bals.exceptionally(error -> failed("block access lists", error))
        .thenCombine(
            receipts.exceptionally(error -> failed("receipts", error)),
            (balsByHash, receiptsByHash) ->
                new BlockBatch(decodeBlocks(syncBlocks), balsByHash, receiptsByHash));
  }

  private CompletableFuture<Map<Hash, BlockAccessList>> downloadBals(
      final List<BlockHeader> headers) {
    final CompletableFuture<Map<Hash, BlockAccessList>> bals = new CompletableFuture<>();
    final EthScheduler scheduler = ethContext.getScheduler();
    scheduler.scheduleServiceTask(
        () -> {
          try {
            bals.complete(requestBals(headers, bals::isDone));
          } catch (final RuntimeException e) {
            bals.completeExceptionally(e);
          }
        });
    scheduler.failAfterTimeout(bals, OPTIONAL_DOWNLOAD_TIMEOUT);
    return bals;
  }

  /**
   * Asks for the BALs of {@code headers}, then again for the rest of a short answer, until a
   * request brings nothing or the batch stops waiting.
   */
  private Map<Hash, BlockAccessList> requestBals(
      final List<BlockHeader> headers, final BooleanSupplier abandoned) {
    final List<BlockHeader> remaining =
        headers.stream()
            .filter(header -> header.getBalHash().isPresent())
            .collect(Collectors.toCollection(ArrayList::new));
    final Map<Hash, BlockAccessList> bals = HashMap.newHashMap(remaining.size());
    boolean progress = true;
    while (!remaining.isEmpty() && progress && !abandoned.getAsBoolean()) {
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

  private CompletableFuture<Map<Hash, List<TransactionReceipt>>> downloadReceipts(
      final List<SyncBlock> syncBlocks) {
    final List<SyncBlock> finalized =
        syncBlocks.stream()
            .filter(block -> block.getHeader().getNumber() <= finalizedNumber)
            .filter(block -> block.getHeader().getBalHash().isPresent())
            .toList();
    if (finalized.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }
    return receiptsDownload
        .apply(finalized)
        .thenApply(
            blocks ->
                blocks.stream()
                    .collect(
                        Collectors.toMap(
                            SyncBlockWithReceipts::getHash,
                            block -> decodeReceipts(block.getReceipts()))));
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

  // the same decoding as the receipts of a sync read back from storage
  private static List<TransactionReceipt> decodeReceipts(
      final List<SyncTransactionReceipt> receipts) {
    return receipts.stream()
        .map(
            receipt -> {
              final Bytes rlp = receipt.getRlpBytes();
              // a typed receipt is not an RLP item until it is wrapped in one
              final Bytes item = Byte.toUnsignedInt(rlp.get(0)) < 0x80 ? RLP.encodeOne(rlp) : rlp;
              return TransactionReceiptDecoder.readFrom(RLP.input(item), true);
            })
        .toList();
  }
}
