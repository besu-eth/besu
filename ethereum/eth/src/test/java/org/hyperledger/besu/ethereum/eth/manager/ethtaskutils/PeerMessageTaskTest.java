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
package org.hyperledger.besu.ethereum.eth.manager.ethtaskutils;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.MockPeerConnection;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.EthTaskException;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.ExceptionUtils;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Tests ethTasks that interact with a single peer to retrieve data from the network.
 *
 * @param <T> The type of data being retrieved.
 */
public abstract class PeerMessageTaskTest<T>
    extends AbstractMessageTaskTest<T, AbstractPeerTask.PeerTaskResult<T>> {
  @Test
  public void completesWhenPeerReturnsPartialResult() {
    // Setup a partially responsive peer
    final RespondingEthPeer.Responder responder =
        RespondingEthPeer.partialResponder(
            blockchain,
            protocolContext.getWorldStateArchive(),
            transactionPool,
            protocolSchedule,
            0.5f);
    final RespondingEthPeer respondingEthPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 32);

    // Execute task and wait for response
    final AtomicReference<T> actualResult = new AtomicReference<>();
    final AtomicReference<EthPeer> actualPeer = new AtomicReference<>();
    final AtomicBoolean done = new AtomicBoolean(false);
    final T requestedData = generateDataToBeRequested();
    final EthTask<AbstractPeerTask.PeerTaskResult<T>> task = createTask(requestedData);
    final CompletableFuture<AbstractPeerTask.PeerTaskResult<T>> future = task.run();
    respondingEthPeer.respondWhile(responder, () -> !future.isDone());
    future.whenComplete(
        (response, error) -> {
          actualResult.set(response.getResult());
          actualPeer.set(response.getPeer());
          done.compareAndSet(false, true);
        });

    assertThat(done).isTrue();
    assertPartialResultMatchesExpectation(requestedData, actualResult.get());
    assertThat(actualPeer.get()).isEqualTo(respondingEthPeer.getEthPeer());
  }

  @Test
  public void failsWhenNoPeersAreAvailable() {
    // Setup data to be requested
    final T requestedData = generateDataToBeRequested();

    // Execute task
    final EthTask<AbstractPeerTask.PeerTaskResult<T>> task = createTask(requestedData);
    final CompletableFuture<AbstractPeerTask.PeerTaskResult<T>> future = task.run();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    future.whenComplete((r, t) -> failure.set(t));

    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThat(failure.get()).isNotNull();
    // Check wrapped failure
    final Throwable error = ExceptionUtils.rootCause(failure.get());
    assertThat(error).isInstanceOf(EthTaskException.class);
    final EthTaskException ethException = (EthTaskException) error;
    assertThat(ethException.reason()).isEqualTo(EthTaskException.FailureReason.NO_AVAILABLE_PEERS);

    assertThat(task.run().isCompletedExceptionally()).isTrue();
    task.cancel();
    assertThat(task.run().isCompletedExceptionally()).isTrue();
  }

  @Test
  public void completesWhenPeersSendEmptyResponses() {
    // Setup a unresponsive peer
    final RespondingEthPeer.Responder responder = RespondingEthPeer.emptyResponder();
    final RespondingEthPeer respondingEthPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 32);

    // Setup data to be requested
    final T requestedData = generateDataToBeRequested();

    // Execute task and wait for response
    final AtomicBoolean done = new AtomicBoolean(false);
    final EthTask<AbstractPeerTask.PeerTaskResult<T>> task = createTask(requestedData);
    final CompletableFuture<AbstractPeerTask.PeerTaskResult<T>> future = task.run();
    respondingEthPeer.respondWhile(responder, () -> !future.isDone());
    future.whenComplete((response, error) -> done.compareAndSet(false, true));
    assertThat(future.isDone()).isTrue();
    assertThat(future.isCompletedExceptionally()).isFalse();
  }

  @Test
  public void recordsTimeoutAgainstPeerWhenTaskTimesOut() {
    peersDoTimeout.set(true);
    // Setup a unresponsive peer
    final RespondingEthPeer respondingEthPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 32);

    // Setup data to be requested
    final T requestedData = generateDataToBeRequested();

    // Execute task and wait for response
    final EthTask<AbstractPeerTask.PeerTaskResult<T>> task = createTask(requestedData);
    final CompletableFuture<AbstractPeerTask.PeerTaskResult<T>> future = task.run();

    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThat(
            respondingEthPeer.getEthPeer().timeoutCounts().values().stream()
                .mapToInt(AtomicInteger::get)
                .sum())
        .isEqualTo(1);
  }

  @Override
  protected void assertResultMatchesExpectation(
      final T requestedData,
      final AbstractPeerTask.PeerTaskResult<T> response,
      final EthPeer respondingPeer) {
    assertThat(response.getResult()).isEqualTo(requestedData);
    assertThat(response.getPeer()).isEqualTo(respondingPeer);
  }

  protected abstract void assertPartialResultMatchesExpectation(T requestedData, T partialResponse);

  protected EthPeer createPeer() {
    final PeerConnection peerConnection = new MockPeerConnection(Set.of(EthProtocol.LATEST));
    final Consumer<EthPeer> onPeerReady = (peer) -> {};
    return new EthPeer(
        peerConnection,
        onPeerReady,
        Collections.emptyList(),
        EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
        TestClock.fixed(),
        Collections.emptyList(),
        Bytes.random(64));
  }
}
