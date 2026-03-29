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
package org.hyperledger.besu.ethereum.eth.manager.task;

import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.PeerRequest;
import org.hyperledger.besu.ethereum.eth.manager.PendingPeerRequest;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.PeerBreachedProtocolException;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.FramingException;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.ExceptionUtils;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPeerRequestTask<R> extends AbstractPeerTask<R> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractPeerRequestTask.class);
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  private Duration timeout = DEFAULT_TIMEOUT;
  private final String protocolName;
  private final int requestCode;
  private volatile PendingPeerRequest responseStream;

  protected AbstractPeerRequestTask(
      final EthContext ethContext,
      final String protocolName,
      final int requestCode,
      final MetricsSystem metricsSystem) {
    super(ethContext, metricsSystem);
    this.protocolName = protocolName;
    this.requestCode = requestCode;
  }

  public AbstractPeerRequestTask<R> setTimeout(final Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  @Override
  protected final void executeTask() {
    final CompletableFuture<R> promise = new CompletableFuture<>();
    responseStream = sendRequest();
    responseStream.then(
        stream -> {
          // Start the timeout now that the request has actually been sent
          ethContext.getScheduler().failAfterTimeout(promise, timeout);

          stream.then(
              (streamClosed, message, peer1) ->
                  handleMessage(promise, streamClosed, message, peer1));
        },
        promise::completeExceptionally);

    promise.whenComplete(
        (r, t) -> {
          final Optional<RequestManager.ResponseStream> responseStream =
              this.responseStream.abort();
          if (t != null) {
            t = ExceptionUtils.rootCause(t);
            if (t instanceof TimeoutException && responseStream.isPresent()) {
              responseStream.get().getPeer().recordRequestTimeout(protocolName, requestCode);
            }
            result.completeExceptionally(t);
          } else if (r != null) {
            // If we got a response we must have had a response stream...
            result.complete(new PeerTaskResult<>(responseStream.get().getPeer(), r));
          }
        });
  }

  public PendingPeerRequest sendRequestToPeer(
      final PeerRequest request, final long minimumBlockNumber) {
    return ethContext.getEthPeers().executePeerRequest(request, minimumBlockNumber, assignedPeer);
  }

  private void handleMessage(
      final CompletableFuture<R> promise,
      final boolean streamClosed,
      final MessageData message,
      final EthPeer peer) {
    if (promise.isDone()) {
      // We've already got our response, don't pass on the stream closed event.
      return;
    }
    try {
      final Optional<R> result = processResponse(streamClosed, message, peer);
      result.ifPresent(
          r -> {
            promise.complete(r);
            peer.recordUsefulResponse();
          });
    } catch (final FramingException e) {
      // Peer sent us data that failed to decompress - disconnect
      LOG.atDebug()
          .setMessage("Disconnecting peer {} due to decompression failure for message code {}")
          .addArgument(peer::getLoggableId)
          .addArgument(message.getCode())
          .setCause(e)
          .log();
      peer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
      promise.completeExceptionally(new PeerBreachedProtocolException());
    } catch (final RLPException e) {
      // Peer sent us malformed data - disconnect
      LOG.debug(
          "Disconnecting with BREACH_OF_PROTOCOL due to malformed message: {}",
          peer.getLoggableId(),
          e);
      LOG.trace("Peer {} Malformed message data: {}", peer, message.getData());
      peer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
      promise.completeExceptionally(new PeerBreachedProtocolException());
    }
  }

  @Override
  protected void cleanup() {
    super.cleanup();
    responseStream.abort().ifPresent(RequestManager.ResponseStream::close);
  }

  protected abstract PendingPeerRequest sendRequest();

  protected abstract Optional<R> processResponse(
      boolean streamClosed, MessageData message, EthPeer peer);
}
