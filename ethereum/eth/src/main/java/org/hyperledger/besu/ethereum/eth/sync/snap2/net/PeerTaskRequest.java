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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.MalformedRlpFromPeerException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Sends an eth {@link PeerTask} through the {@link RequestScheduler}, reusing its encoding and
 * validation. A fresh task is created per attempt since tasks may keep state.
 */
public final class PeerTaskRequest<T> implements SyncRequest<T> {

  private final Supplier<PeerTask<T>> tasks;
  private final Priority priority;
  private final String name;
  private final long minimumPeerHeight;
  private final PeerTask<T> template;
  private volatile PeerTask<T> current;

  public PeerTaskRequest(
      final Supplier<PeerTask<T>> tasks, final Priority priority, final String name) {
    this(tasks, priority, name, 0);
  }

  /** Creates a request only sent to peers that announced at least the given chain height. */
  public PeerTaskRequest(
      final Supplier<PeerTask<T>> tasks,
      final Priority priority,
      final String name,
      final long minimumPeerHeight) {
    this.tasks = tasks;
    this.priority = priority;
    this.name = name;
    this.minimumPeerHeight = minimumPeerHeight;
    this.template = tasks.get();
    // sent on eth, as every peer task
    checkArgument(
        EthProtocol.NAME.equals(template.getSubProtocol().getName()),
        "%s is not an eth task",
        name);
  }

  @Override
  public Protocol protocol() {
    return Protocol.ETH;
  }

  @Override
  public Priority priority() {
    return priority;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public long minimumPeerHeight() {
    return minimumPeerHeight;
  }

  @Override
  public boolean canServe(final EthPeer peer) {
    return template.getPeerRequirementFilter().test(EthPeerImmutableAttributes.from(peer));
  }

  @Override
  public MessageData message(final EthPeer peer) {
    final PeerTask<T> task = tasks.get();
    current = task;
    return task.getRequestMessage(peer.getAgreedCapabilities());
  }

  @Override
  public T decode(final MessageData response, final EthPeer peer) {
    // an empty list is how a peer says it has none of it, as for bodies it pruned
    if (RLP.EMPTY_LIST.equals(response.getData())) {
      throw new EmptyResponseException(name + " returned nothing");
    }
    final PeerTask<T> task = current;
    final T result;
    try {
      result = task.processResponse(response, peer.getAgreedCapabilities());
    } catch (final InvalidPeerTaskResponseException e) {
      throw new InvalidResponseException(e.getMessage(), false);
    } catch (final MalformedRlpFromPeerException e) {
      throw new InvalidResponseException(e.getMessage(), true);
    }
    final PeerTaskValidationResponse validation = task.validateResult(result);
    if (validation == PeerTaskValidationResponse.NO_RESULTS_RETURNED) {
      throw new EmptyResponseException(name + " returned nothing");
    }
    if (validation != PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD) {
      throw new InvalidResponseException(
          name + ": " + validation, validation.getDisconnectReason().orElse(null));
    }
    // as the peer task executor does, e.g. to learn the chain height of the peer
    task.postProcessResult(
        new PeerTaskExecutorResult<>(
            Optional.ofNullable(result), PeerTaskExecutorResponseCode.SUCCESS, List.of(peer)));
    return result;
  }
}
