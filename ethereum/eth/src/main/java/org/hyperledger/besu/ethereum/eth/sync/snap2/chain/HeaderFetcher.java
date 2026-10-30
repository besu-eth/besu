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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.PeerTaskRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender.Response;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.util.FutureUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Downloads block headers trusted through hash links: a header known by hash, then its ancestors,
 * each one the parent of the previous.
 */
public final class HeaderFetcher {

  // peers announce their range at most once per epoch, so one at the tip can look 32 blocks behind
  private static final long HEAD_TOLERANCE = 32;

  private final RequestSender sender;
  private final ProtocolSchedule protocolSchedule;
  private final ChainHeadSource heads;
  private final int headersPerRequest;

  public HeaderFetcher(
      final RequestSender sender,
      final ProtocolSchedule protocolSchedule,
      final ChainHeadSource heads,
      final int headersPerRequest) {
    checkArgument(headersPerRequest > 0, "headersPerRequest must be positive");
    this.sender = sender;
    this.protocolSchedule = protocolSchedule;
    this.heads = heads;
    this.headersPerRequest = headersPerRequest;
  }

  /** Returns the header with the given hash. */
  public CompletableFuture<BlockHeader> byHash(final Hash hash, final Priority priority) {
    return heads
        .knownHeader(hash)
        .map(CompletableFuture::completedFuture)
        .orElseGet(
            () -> {
              // a head just announced is only known by the peers closest to the tip
              final long recent =
                  priority == Priority.PIVOT ? announced(sender.bestPeerHeight()) : 0;
              final CompletableFuture<Response<List<BlockHeader>>> request =
                  request(hash, 1, priority, recent);
              final CompletableFuture<BlockHeader> header =
                  request.thenApply(answer -> answer.value().getFirst());
              // the scheduler retries the request until it is cancelled
              FutureUtils.propagateCancellation(header, request);
              return header;
            });
  }

  /**
   * Returns the headers from {@code lowest} up to {@code top} included, in ascending order, each
   * linked to the next by its hash; none when {@code lowest} is above {@code top}.
   */
  public CompletableFuture<List<BlockHeader>> chainDown(
      final BlockHeader top, final long lowest, final Priority priority) {
    if (lowest > top.getNumber()) {
      return CompletableFuture.completedFuture(List.of());
    }
    final List<BlockHeader> descending = new ArrayList<>();
    descending.add(top);
    return walkDown(descending, lowest, priority)
        .thenApply(
            headers -> {
              Collections.reverse(headers);
              return headers;
            });
  }

  private CompletableFuture<List<BlockHeader>> walkDown(
      final List<BlockHeader> descending, final long lowest, final Priority priority) {
    final BlockHeader cursor = descending.getLast();
    if (cursor.getNumber() <= lowest) {
      return CompletableFuture.completedFuture(descending);
    }
    final int count = (int) Math.min(headersPerRequest, cursor.getNumber() - lowest);
    return fetch(cursor.getParentHash(), count, priority, announced(cursor.getNumber() - 1))
        .thenCompose(
            headers -> {
              final int walked = descending.size();
              for (final BlockHeader header : headers) {
                if (header.getNumber() < lowest) {
                  break;
                }
                descending.add(header);
              }
              // no header added: the top does not follow its parent, asking again would loop
              if (descending.size() == walked) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException(
                        "block "
                            + cursor.getNumber()
                            + " has parent "
                            + headers.getFirst().getNumber()
                            + ", below "
                            + lowest));
              }
              return walkDown(descending, lowest, priority);
            });
  }

  // headers from the given hash going down, the first one checked against the hash
  private CompletableFuture<List<BlockHeader>> fetch(
      final Hash hash, final int count, final Priority priority, final long minimumPeerHeight) {
    return request(hash, count, priority, minimumPeerHeight).thenApply(Response::value);
  }

  private CompletableFuture<Response<List<BlockHeader>>> request(
      final Hash hash, final int count, final Priority priority, final long minimumPeerHeight) {
    return sender.submitWithRetries(
        new PeerTaskRequest<>(
            () ->
                new GetHeadersFromPeerTask(hash, count, 0, Direction.REVERSE, 0, protocolSchedule),
            priority,
            // peers are rated per name: short pivot lookups apart from full batches
            priority == Priority.PIVOT ? "pivot_headers" : "headers",
            minimumPeerHeight));
  }

  // a peer that holds this block may have announced a lower height, as announcements lag
  private static long announced(final long height) {
    return Math.max(0, height - HEAD_TOLERANCE);
  }
}
