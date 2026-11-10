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
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.eth.SnapProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseStream;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.CodeFetcher;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches the missing code of a stored account by its hash, asking the peers agreeing on snap one
 * at a time. Peers are only flagged as serving snap in snap sync mode, so the request is sent
 * directly rather than through the snap sync tasks.
 */
public class MissingCodeFetcher implements CodeFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(MissingCodeFetcher.class);
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final int MAX_PEERS = 4;

  private final EthContext ethContext;
  private final Blockchain blockchain;
  private final Duration timeout;

  public MissingCodeFetcher(final EthContext ethContext, final Blockchain blockchain) {
    this(ethContext, blockchain, TIMEOUT);
  }

  @VisibleForTesting
  MissingCodeFetcher(
      final EthContext ethContext, final Blockchain blockchain, final Duration timeout) {
    this.ethContext = ethContext;
    this.blockchain = blockchain;
    this.timeout = timeout;
  }

  @Override
  public Optional<Bytes> fetch(final Hash codeHash) {
    return ethContext
        .getEthPeers()
        .streamAvailablePeers()
        .map(EthPeerImmutableAttributes::ethPeer)
        .filter(MissingCodeFetcher::agreedOnSnap)
        .limit(MAX_PEERS)
        // an interrupted caller no longer waits, so a request would only get an unsolicited reply
        .takeWhile(peer -> !Thread.currentThread().isInterrupted())
        .map(peer -> fetchFrom(peer, codeHash))
        .flatMap(Optional::stream)
        .findFirst();
  }

  private static boolean agreedOnSnap(final EthPeer peer) {
    return peer.getAgreedCapabilities().contains(SnapProtocolVersion.V1.getCapability())
        || peer.getAgreedCapabilities().contains(SnapProtocolVersion.V2.getCapability());
  }

  private Optional<Bytes> fetchFrom(final EthPeer peer, final Hash codeHash) {
    final CompletableFuture<Optional<Bytes>> response = new CompletableFuture<>();
    ResponseStream stream = null;
    try {
      stream =
          peer.getSnapBytecode(
              blockchain.getChainHeadHeader().getStateRoot(),
              List.of(Bytes32.wrap(codeHash.getBytes())));
      stream.then(
          (streamClosed, message, sender) -> {
            try {
              response.complete(streamClosed ? Optional.empty() : codeOf(message, codeHash));
            } catch (final RuntimeException e) {
              // a malformed response would otherwise leave the request waiting for the timeout
              response.completeExceptionally(e);
            }
          });
      return response.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return Optional.empty();
    } catch (final Exception e) {
      LOG.debug("Unable to fetch the code {} from {}", codeHash, peer.getLoggableId(), e);
      return Optional.empty();
    } finally {
      // releases the peer's request capacity, which only a response or a close gives back
      if (stream != null) {
        stream.close();
      }
    }
  }

  private static Optional<Bytes> codeOf(final MessageData message, final Hash codeHash) {
    return ByteCodesMessage.readFrom(message).bytecodes(true).codes().stream()
        .filter(code -> Hash.hash(code).equals(codeHash))
        .findFirst();
  }
}
