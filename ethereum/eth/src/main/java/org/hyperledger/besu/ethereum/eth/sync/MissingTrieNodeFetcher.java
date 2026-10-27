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
package org.hyperledger.besu.ethereum.eth.sync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.SnapProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseStream;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.TrieNodeRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fetches one hash-verified trie node directly from available SNAP1 peers. */
public class MissingTrieNodeFetcher
    implements org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.MissingTrieNodeFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(MissingTrieNodeFetcher.class);
  private static final Duration MAX_TIMEOUT = Duration.ofSeconds(3);
  private static final int MAX_PEERS = 2;
  private static final int MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
  private static final long CANCELLATION_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

  private final EthContext ethContext;
  private final Duration timeout;
  private final AtomicLong peerCursor = new AtomicLong();
  private final AtomicLong snapServerCursor = new AtomicLong();

  public MissingTrieNodeFetcher(final EthContext ethContext) {
    this(ethContext, MAX_TIMEOUT);
  }

  @VisibleForTesting
  MissingTrieNodeFetcher(final EthContext ethContext, final Duration timeout) {
    this.ethContext = ethContext;
    this.timeout = timeout;
  }

  @Override
  public Optional<Bytes> fetch(final TrieNodeRequest request, final WorldStateRepairBudget budget) {
    final long startedAt = System.nanoTime();
    final long allowance =
        timeout.compareTo(MAX_TIMEOUT) < 0 ? boundedNanos(timeout) : MAX_TIMEOUT.toNanos();
    final long totalNanos = Math.min(allowance, budget.remainingTime().toNanos());
    if (totalNanos <= 0 || Thread.currentThread().isInterrupted()) {
      return Optional.empty();
    }
    try {
      final Bytes encodedLocation = CompactEncoding.encode(request.location());
      final List<List<Bytes>> paths =
          List.of(
              request
                  .accountHash()
                  .map(hash -> List.of(hash.getBytes(), encodedLocation))
                  .orElseGet(() -> List.of(encodedLocation)));
      final List<EthPeer> peers;
      try (var availablePeers = ethContext.getEthPeers().streamAvailablePeers()) {
        final List<EthPeerImmutableAttributes> candidates =
            availablePeers
                .filter(EthPeerImmutableAttributes::isFullyValidated)
                .filter(EthPeerImmutableAttributes::hasAvailableRequestCapacity)
                .filter(
                    a ->
                        a.ethPeer()
                            .getAgreedCapabilities()
                            .contains(SnapProtocolVersion.V1.getCapability()))
                .sorted(Comparator.comparing(a -> a.ethPeer().getId()))
                .toList();
        peers = selectPeers(candidates);
      }
      for (int i = 0; i < peers.size(); i++) {
        final long remaining = remainingNanos(startedAt, totalNanos, budget);
        if (remaining <= 0 || Thread.currentThread().isInterrupted()) {
          return Optional.empty();
        }
        // Reserve time for the second peer when the first peer stays silent.
        final long peerAllowance = remaining / (peers.size() - i);
        final Optional<Bytes> node =
            fetchFrom(
                peers.get(i),
                request.stateRoot(),
                paths,
                request.expectedNodeHash(),
                startedAt,
                Math.min(totalNanos, System.nanoTime() - startedAt + peerAllowance),
                budget);
        if (node.isPresent() && remainingNanos(startedAt, totalNanos, budget) > 0) {
          return node;
        }
      }
    } catch (final RuntimeException e) {
      LOG.debug("Unable to fetch trie node {}", request.expectedNodeHash(), e);
    }
    return Optional.empty();
  }

  private List<EthPeer> selectPeers(final List<EthPeerImmutableAttributes> candidates) {
    if (candidates.isEmpty()) {
      return List.of();
    }
    final List<EthPeerImmutableAttributes> servers =
        candidates.stream().filter(EthPeerImmutableAttributes::isServingSnap).toList();
    final List<EthPeerImmutableAttributes> selected = new ArrayList<>(MAX_PEERS);
    if (!servers.isEmpty()) {
      selected.add(servers.get(Math.floorMod(snapServerCursor.getAndIncrement(), servers.size())));
      final List<EthPeerImmutableAttributes> others =
          candidates.stream().filter(candidate -> !candidate.equals(selected.get(0))).toList();
      if (!others.isEmpty()) {
        selected.add(others.get(Math.floorMod(peerCursor.getAndIncrement(), others.size())));
      }
    } else {
      final int start = Math.floorMod(peerCursor.getAndIncrement(), candidates.size());
      selected.add(candidates.get(start));
      if (candidates.size() > 1) {
        selected.add(candidates.get((start + 1) % candidates.size()));
      }
    }
    return selected.stream().map(EthPeerImmutableAttributes::ethPeer).toList();
  }

  private Optional<Bytes> fetchFrom(
      final EthPeer peer,
      final Hash stateRoot,
      final List<List<Bytes>> paths,
      final Hash expectedNodeHash,
      final long startedAt,
      final long timeoutNanos,
      final WorldStateRepairBudget budget) {
    final PendingNodeResponse response = new PendingNodeResponse(expectedNodeHash, budget);
    ResponseStream stream = null;
    try {
      stream = peer.getSnapTrieNode(stateRoot, paths);
      stream.then(response::accept);
      while (!Thread.currentThread().isInterrupted()) {
        final long remaining = remainingNanos(startedAt, timeoutNanos, budget);
        if (remaining <= 0) {
          return Optional.empty();
        }
        try {
          return response.result.get(
              Math.min(remaining, CANCELLATION_POLL_NANOS), TimeUnit.NANOSECONDS);
        } catch (final TimeoutException e) {
          // Recheck cancellation and the cumulative deadline while a peer is silent.
        }
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final Exception e) {
      LOG.debug("Unable to fetch trie node {} from {}", expectedNodeHash, peer.getLoggableId(), e);
    } finally {
      // Stop admitting callbacks and wait for any current decoder before releasing ownership.
      // Closing ResponseStream alone deregisters the request but does not drain its callback.
      response.close();
      if (stream != null) {
        stream.close();
      }
    }
    return Optional.empty();
  }

  private static Optional<Bytes> nodeOf(final MessageData message, final Hash expectedNodeHash) {
    if (message.getCode() != SnapV1.TRIE_NODES) {
      return Optional.empty();
    }
    final RLPInput input = new BytesValueRLPInput(message.getData(), false);
    if (input.enterList(2) != 2) {
      return Optional.empty();
    }
    input.skipNext();
    if (input.enterList(1) != 1) {
      return Optional.empty();
    }
    final Bytes node = input.readBytes();
    input.leaveList();
    input.leaveList();
    return !node.isEmpty() && Hash.hash(node).equals(expectedNodeHash)
        ? Optional.of(node)
        : Optional.empty();
  }

  private static long remainingNanos(
      final long startedAt, final long timeoutNanos, final WorldStateRepairBudget budget) {
    return Math.min(
        timeoutNanos - (System.nanoTime() - startedAt), budget.remainingTime().toNanos());
  }

  private static long boundedNanos(final Duration timeout) {
    return timeout.isNegative() ? 0 : timeout.toNanos();
  }

  private static final class PendingNodeResponse {
    private final Hash expectedNodeHash;
    private final WorldStateRepairBudget budget;
    private final CompletableFuture<Optional<Bytes>> result = new CompletableFuture<>();
    private boolean accepting = true;

    private PendingNodeResponse(final Hash expectedNodeHash, final WorldStateRepairBudget budget) {
      this.expectedNodeHash = expectedNodeHash;
      this.budget = budget;
    }

    private synchronized void accept(
        final boolean closed, final MessageData message, final EthPeer sender) {
      if (!accepting) {
        return;
      }
      try {
        if (closed) {
          result.complete(Optional.empty());
        } else {
          // Charge every admitted message, including malformed or surplus responses.
          final int bytes = message.getSize();
          if (!budget.consumeReceivedBytes(bytes) || bytes > MAX_RESPONSE_BYTES) {
            result.complete(Optional.empty());
          } else if (!result.isDone()) {
            result.complete(nodeOf(message, expectedNodeHash));
          }
        }
      } catch (final RuntimeException malformedResponse) {
        result.complete(Optional.empty());
      }
    }

    private synchronized void close() {
      accepting = false;
    }
  }
}
