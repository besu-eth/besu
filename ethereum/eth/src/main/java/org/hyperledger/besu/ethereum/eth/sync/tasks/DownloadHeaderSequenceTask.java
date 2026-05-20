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
package org.hyperledger.besu.ethereum.eth.sync.tasks;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.NoAvailablePeersException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractRetryingPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.ValidationPolicy;
import org.hyperledger.besu.ethereum.eth.sync.tasks.exceptions.InvalidBlockException;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves a sequence of headers, sending out requests repeatedly until all headers are fulfilled.
 * Validates headers as they are received.
 */
public class DownloadHeaderSequenceTask extends AbstractRetryingPeerTask<List<BlockHeader>> {
  private static final Logger LOG = LoggerFactory.getLogger(DownloadHeaderSequenceTask.class);
  private static final int DEFAULT_RETRIES = 5;

  private final EthContext ethContext;
  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;

  private final BlockHeader[] headers;
  private final BlockHeader referenceHeader;
  private final int segmentLength;
  private final long startingBlockNumber;
  private final ValidationPolicy validationPolicy;

  private int lastFilledHeaderIndex;

  private DownloadHeaderSequenceTask(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final BlockHeader referenceHeader,
      final int segmentLength,
      final int maxRetries,
      final ValidationPolicy validationPolicy,
      final MetricsSystem metricsSystem) {
    super(ethContext, maxRetries, Collection::isEmpty, metricsSystem);
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.referenceHeader = referenceHeader;
    this.segmentLength = segmentLength;
    this.validationPolicy = validationPolicy;

    checkArgument(segmentLength > 0, "Segment length must not be 0");
    startingBlockNumber = referenceHeader.getNumber() - segmentLength;
    headers = new BlockHeader[segmentLength];
    lastFilledHeaderIndex = segmentLength;
  }

  public static DownloadHeaderSequenceTask endingAtHeader(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final BlockHeader referenceHeader,
      final int segmentLength,
      final ValidationPolicy validationPolicy,
      final MetricsSystem metricsSystem) {
    return new DownloadHeaderSequenceTask(
        protocolSchedule,
        protocolContext,
        ethContext,
        referenceHeader,
        segmentLength,
        DEFAULT_RETRIES,
        validationPolicy,
        metricsSystem);
  }

  @Override
  protected CompletableFuture<List<BlockHeader>> executePeerTask(
      final Optional<EthPeer> assignedPeer) {
    LOG.debug(
        "Downloading headers from {} to {}.", startingBlockNumber, referenceHeader.getNumber());
    return downloadHeadersUsingPeerTaskSystem(assignedPeer)
        .thenCompose(this::processHeadersUsingPeerTask)
        .whenComplete(
            (r, t) -> {
              // We're done if we've filled all requested headers
              if (lastFilledHeaderIndex == 0) {
                LOG.debug(
                    "Finished downloading headers from {} to {}.",
                    headers[0].getNumber(),
                    headers[segmentLength - 1].getNumber());
                result.complete(Arrays.asList(headers));
              }
            });
  }

  private CompletableFuture<PeerTaskExecutorResult<List<BlockHeader>>>
      downloadHeadersUsingPeerTaskSystem(final Optional<EthPeer> ethPeer) {
    return ethContext
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              // Figure out parameters for our headers request
              final boolean partiallyFilled = lastFilledHeaderIndex < segmentLength;
              final BlockHeader referenceHeaderForNextRequest =
                  partiallyFilled ? headers[lastFilledHeaderIndex] : referenceHeader;
              final Hash referenceHash = referenceHeaderForNextRequest.getHash();
              final int count = partiallyFilled ? lastFilledHeaderIndex : segmentLength;

              GetHeadersFromPeerTask task =
                  new GetHeadersFromPeerTask(
                      referenceHash,
                      referenceHeaderForNextRequest.getNumber(),
                      count + 1,
                      0,
                      GetHeadersFromPeerTask.Direction.REVERSE,
                      protocolSchedule);
              PeerTaskExecutorResult<List<BlockHeader>> taskResult;
              if (ethPeer.isPresent()) {
                taskResult =
                    ethContext.getPeerTaskExecutor().executeAgainstPeer(task, ethPeer.get());
              } else {
                taskResult = ethContext.getPeerTaskExecutor().execute(task);
              }

              if (taskResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
                  || taskResult.result().isEmpty()) {
                if (taskResult.responseCode() == PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE) {
                  return CompletableFuture.failedFuture(
                      NoAvailablePeersException.WITHOUT_STACKTRACE);
                } else {
                  return CompletableFuture.failedFuture(
                      new RuntimeException(
                          "Failed to download headers. Response code was "
                              + taskResult.responseCode()));
                }
              }
              return CompletableFuture.completedFuture(taskResult);
            });
  }

  private CompletableFuture<List<BlockHeader>> processHeadersUsingPeerTask(
      final PeerTaskExecutorResult<List<BlockHeader>> headersResult) {
    final List<BlockHeader> blockHeaders =
        headersResult
            .result()
            .orElseThrow(
                () -> new RuntimeException("Expected blockHeaders in PeerTaskExecutorResult"));
    final EthPeer ethPeer =
        headersResult.ethPeers().reversed().stream()
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Expected a peer in PeerTaskExecutorResult"));
    return processHeaders(blockHeaders, ethPeer);
  }

  private CompletableFuture<List<BlockHeader>> processHeaders(
      final List<BlockHeader> blockHeaders, final EthPeer ethPeer) {
    final CompletableFuture<List<BlockHeader>> future = new CompletableFuture<>();
    BlockHeader child = null;
    boolean firstSkipped = false;
    final int previousHeaderIndex = lastFilledHeaderIndex;
    for (final BlockHeader header : blockHeaders) {
      final int headerIndex =
          Ints.checkedCast(segmentLength - (referenceHeader.getNumber() - header.getNumber()));
      if (!firstSkipped) {
        // Skip over reference header
        firstSkipped = true;
        continue;
      }
      if (child == null) {
        child = (headerIndex == segmentLength - 1) ? referenceHeader : headers[headerIndex + 1];
      }

      final boolean foundChild = child != null;
      final boolean headerInRange = checkHeaderInRange(header);
      final boolean headerInvalid = foundChild && !validateHeader(child, header);
      if (!headerInRange || !foundChild || headerInvalid) {
        final BlockHeader invalidHeader = child;
        final CompletableFuture<?> badBlockHandled =
            headerInvalid
                ? markBadBlock(invalidHeader, ethPeer)
                : CompletableFuture.completedFuture(null);
        badBlockHandled.whenComplete(
            (res, err) -> {
              LOG.debug(
                  "Received invalid headers from peer (BREACH_OF_PROTOCOL), disconnecting from: {}",
                  ethPeer);
              ethPeer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL_INVALID_HEADERS);
              final InvalidBlockException exception;
              if (invalidHeader == null) {
                final String msg =
                    String.format(
                        "Received misordered blocks. Missing child of %s", header.toLogString());
                exception = InvalidBlockException.create(msg);
              } else {
                final String errorMsg =
                    headerInvalid
                        ? "Header failed validation"
                        : "Out-of-range header received from peer";
                exception = InvalidBlockException.fromInvalidBlock(errorMsg, invalidHeader);
              }
              future.completeExceptionally(exception);
            });

        return future;
      }
      headers[headerIndex] = header;
      lastFilledHeaderIndex = headerIndex;
      child = header;
    }
    future.complete(asList(headers).subList(lastFilledHeaderIndex, previousHeaderIndex));
    return future;
  }

  private CompletableFuture<?> markBadBlock(final BlockHeader badHeader, final EthPeer badPeer) {
    // even though the header is known bad we are downloading the block body for the debug_badBlocks
    // RPC
    final BadBlockManager badBlockManager = protocolContext.getBadBlockManager();
    return ethContext
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              GetBodiesFromPeerTask task =
                  new GetBodiesFromPeerTask(List.of(badHeader), protocolSchedule);
              PeerTaskExecutorResult<List<Block>> taskResult =
                  ethContext.getPeerTaskExecutor().executeAgainstPeer(task, badPeer);
              if (taskResult.responseCode() == PeerTaskExecutorResponseCode.SUCCESS) {
                return CompletableFuture.completedFuture(
                    taskResult.result().map(List::getFirst).orElse(null));
              } else {
                return CompletableFuture.failedFuture(new RuntimeException());
              }
            })
        .whenComplete(
            (blockResult, error) -> {
              final HeaderValidationMode validationMode =
                  validationPolicy.getValidationModeForNextBlock();
              final String description =
                  String.format("Failed header validation (%s)", validationMode);
              final BadBlockCause cause = BadBlockCause.fromValidationFailure(description);
              if (blockResult != null) {
                badBlockManager.addBadBlock(blockResult, cause);
              } else {
                badBlockManager.addBadHeader(badHeader, cause);
              }
            });
  }

  private boolean checkHeaderInRange(final BlockHeader header) {
    final long finalBlockNumber = startingBlockNumber + segmentLength;
    return header.getNumber() >= startingBlockNumber && header.getNumber() < finalBlockNumber;
  }

  private boolean validateHeader(final BlockHeader header, final BlockHeader parent) {
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(header);
    final BlockHeaderValidator blockHeaderValidator = protocolSpec.getBlockHeaderValidator();
    return blockHeaderValidator.validateHeader(
        header, parent, protocolContext, validationPolicy.getValidationModeForNextBlock());
  }
}
