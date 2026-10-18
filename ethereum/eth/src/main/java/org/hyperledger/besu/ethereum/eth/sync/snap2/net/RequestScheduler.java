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

import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.DISCONNECTED;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.EMPTY;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.INVALID;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.NO_PEER;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.STOPPED;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.TIMEOUT;

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseStream;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.EmptyResponseException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.InvalidResponseException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection.PeerNotConnected;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends the sync requests to the peers, most urgent first, each to the fastest peer with capacity
 * left. A peer has one lane per protocol whose capacity grows with answers and halves on timeouts;
 * a timed out request keeps its slot until its late answer or one more timeout.
 */
public final class RequestScheduler implements RequestSender {

  private static final Logger LOG = LoggerFactory.getLogger(RequestScheduler.class);
  private static final double INITIAL_THROUGHPUT = 64; // bytes per millisecond
  private static final int INITIAL_LANE_LIMIT = 2;
  private static final int MAX_BACKOFF_SECONDS = 30;
  private static final int CONSECUTIVE_TIMEOUTS_BEFORE_DISCONNECT = 12;
  // announced heights lag, so a peer that looks behind is only picked when no other peer can be
  private static final double BEHIND_PENALTY = 1e-3;

  private enum AttemptState {
    PENDING,
    DONE,
    TIMED_OUT,
    RELEASED
  }

  /** How a request ended, also its metric label. */
  private enum Outcome {
    SUCCESS,
    EMPTY,
    INVALID,
    ERROR,
    TIMEOUT,
    DISCONNECTED,
    NO_PEER;

    private final String label = name().toLowerCase(Locale.ROOT);
  }

  private final EthPeers ethPeers;
  private final Duration timeout;
  private final Duration queueTimeout;
  private final int maxSnapRequestsPerPeer;
  private final int maxEthRequestsPerPeer;
  private final ScheduledExecutorService loop;
  private final ExecutorService workers;
  private final LabelledMetric<Counter> requestCounter;
  private final LabelledMetric<Counter> responseBytes;
  private final LabelledMetric<Counter> responseMillis;

  private final PriorityQueue<Pending<?>> queue =
      new PriorityQueue<>(
          Comparator.<Pending<?>>comparingInt(p -> p.request.priority().ordinal())
              .thenComparingLong(p -> p.sequence));
  private final Map<EthPeer, PeerState> peers = new HashMap<>();
  // every request not yet answered, failed when the scheduler stops
  private final Set<CompletableFuture<?>> outstanding = ConcurrentHashMap.newKeySet();
  private long sequence;
  private final long connectSubscription;
  private volatile int snapCapacity;
  private volatile int ethCapacity;
  private volatile int snapInFlight;
  private volatile int ethInFlight;
  private volatile int queued;
  private volatile boolean stopped;
  // a dispatch is already queued on the loop, loop thread only
  private boolean dispatchQueued;

  public RequestScheduler(
      final EthPeers ethPeers,
      final MetricsSystem metricsSystem,
      final Duration timeout,
      final int maxSnapRequestsPerPeer,
      final int maxEthRequestsPerPeer,
      final int workerThreads) {
    this.ethPeers = ethPeers;
    this.timeout = timeout;
    this.queueTimeout = timeout.multipliedBy(6);
    this.maxSnapRequestsPerPeer = maxSnapRequestsPerPeer;
    this.maxEthRequestsPerPeer = maxEthRequestsPerPeer;
    this.loop =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("snap2-scheduler").daemon().factory());
    this.workers =
        Executors.newFixedThreadPool(
            workerThreads, Thread.ofPlatform().name("snap2-worker-", 0).daemon().factory());
    this.requestCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_requests_total",
            "Requests sent by the SNAP2 sync, by type and outcome",
            "request",
            "outcome");
    this.responseBytes =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_response_bytes_total",
            "Bytes of the responses received by the SNAP2 sync, by request type",
            "request");
    this.responseMillis =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_response_milliseconds_total",
            "Time peers took to answer SNAP2 requests, by request type",
            "request");
    final var inFlight =
        metricsSystem.createLabelledSuppliedGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_requests_in_flight",
            "Requests of the SNAP2 sync waiting for an answer, by protocol",
            "protocol");
    inFlight.labels(() -> snapInFlight, "snap");
    inFlight.labels(() -> ethInFlight, "eth");
    final var capacity =
        metricsSystem.createLabelledSuppliedGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_request_capacity",
            "Requests the peers currently accept from the SNAP2 sync, by protocol",
            "protocol");
    capacity.labels(() -> snapCapacity, "snap");
    capacity.labels(() -> ethCapacity, "eth");
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap2_requests_queued",
        "Requests of the SNAP2 sync waiting for a peer",
        () -> queued);
    ethPeers.subscribeDisconnect(peer -> execute(() -> peers.remove(peer)));
    this.connectSubscription = ethPeers.subscribeConnect(peer -> execute(this::dispatchSoon));
    loop.scheduleWithFixedDelay(this::dispatchSafely, 1, 1, TimeUnit.SECONDS);
  }

  @Override
  public ExecutorService workers() {
    return workers;
  }

  @Override
  public <T> CompletableFuture<RequestSender.Response<T>> submit(
      final SyncRequest<T> request, final Set<EthPeer> excluded) {
    final Pending<T> pending = new Pending<>(request, new HashSet<>(excluded));
    if (stopped) {
      pending.future.completeExceptionally(stoppedFailure());
      return pending.future;
    }
    outstanding.add(pending.future);
    pending.future.whenComplete((response, error) -> outstanding.remove(pending.future));
    execute(
        () -> {
          pending.sequence = sequence++;
          pending.queuedAt = System.nanoTime();
          queue.add(pending);
          dispatchSoon();
        });
    return pending.future;
  }

  @Override
  public <T> CompletableFuture<RequestSender.Response<T>> submitWithRetries(
      final SyncRequest<T> request, final int attempts) {
    final CompletableFuture<RequestSender.Response<T>> result = new CompletableFuture<>();
    attempt(request, new HashSet<>(), attempts, result);
    return result;
  }

  private <T> void attempt(
      final SyncRequest<T> request,
      final Set<EthPeer> excluded,
      final int attemptsLeft,
      final CompletableFuture<RequestSender.Response<T>> result) {
    submit(request, excluded)
        .whenComplete(
            (response, error) -> {
              if (error == null) {
                result.complete(response);
                return;
              }
              final RequestFailedException failure =
                  RequestFailedException.from(error)
                      .orElseGet(() -> failure(INVALID, null, error.toString()));
              if (attemptsLeft <= 1 || failure.reason() == STOPPED || result.isDone()) {
                result.completeExceptionally(failure);
                return;
              }
              if (failure.reason() == NO_PEER) {
                // every peer may have been tried, start over after a pause
                excluded.clear();
                try {
                  loop.schedule(
                      () -> attempt(request, excluded, attemptsLeft - 1, result),
                      1,
                      TimeUnit.SECONDS);
                } catch (final RejectedExecutionException stopping) {
                  result.completeExceptionally(stoppedFailure());
                }
              } else {
                failure.peer().ifPresent(excluded::add);
                attempt(request, excluded, attemptsLeft - 1, result);
              }
            });
  }

  @Override
  public int peerCount(final Protocol protocol) {
    return (int)
        ethPeers
            .streamAvailablePeers()
            .map(EthPeerImmutableAttributes::ethPeer)
            .filter(peer -> speaks(peer, protocol))
            .count();
  }

  @Override
  public long bestPeerHeight() {
    return ethPeers
        .streamAvailablePeers()
        .mapToLong(peer -> peer.ethPeer().chainState().getEstimatedHeight())
        .max()
        .orElse(0);
  }

  @Override
  public int capacity(final Protocol protocol) {
    final int adapted = protocol == Protocol.SNAP ? snapCapacity : ethCapacity;
    return adapted > 0 ? adapted : peerCount(protocol) * INITIAL_LANE_LIMIT;
  }

  /** Stops dispatching and fails every request not yet answered. */
  @Override
  public void stop() {
    stopped = true;
    ethPeers.unsubscribeConnect(connectSubscription);
    // timeouts still scheduled would fire on failed requests
    loop.shutdownNow();
    workers.shutdownNow();
    final RequestFailedException failure = stoppedFailure();
    outstanding.forEach(future -> future.completeExceptionally(failure));
  }

  private void execute(final Runnable task) {
    if (!loop.isShutdown()) {
      try {
        loop.execute(() -> runLogged(task));
      } catch (final RuntimeException e) {
        LOG.trace("Scheduler loop rejected a task", e);
      }
    }
  }

  private void dispatchSafely() {
    runLogged(this::dispatch);
  }

  // events coming in a burst share one dispatch, which scans every queued request
  private void dispatchSoon() {
    if (!dispatchQueued) {
      dispatchQueued = true;
      execute(
          () -> {
            dispatchQueued = false;
            dispatch();
          });
    }
  }

  // a scheduled executor keeps the exceptions of its tasks to itself
  private static void runLogged(final Runnable task) {
    try {
      task.run();
    } catch (final RuntimeException e) {
      LOG.warn("SNAP2 scheduler task failed", e);
    }
  }

  private void dispatch() {
    if (stopped) {
      return;
    }
    final long now = System.nanoTime();
    final List<PeerState> candidates = new ArrayList<>();
    ethPeers
        .streamAvailablePeers()
        .map(EthPeerImmutableAttributes::ethPeer)
        .filter(EthPeer::isFullyValidated)
        .forEach(peer -> candidates.add(peers.computeIfAbsent(peer, this::newPeerState)));
    updateCapacities(candidates);
    if (queue.isEmpty()) {
      return;
    }

    final List<Pending<?>> deferred = new ArrayList<>();
    try {
      Pending<?> pending;
      while ((pending = queue.poll()) != null) {
        if (pending.future.isDone()) {
          continue;
        }
        final PeerState peer = select(pending, candidates, now);
        if (peer != null) {
          send(pending, peer);
        } else if (now - pending.queuedAt > queueTimeout.toNanos()) {
          countRequest(pending.request, Outcome.NO_PEER);
          pending.future.completeExceptionally(
              failure(NO_PEER, null, "no peer could serve " + pending.request.name()));
        } else {
          deferred.add(pending);
        }
      }
    } finally {
      queue.addAll(deferred);
    }
  }

  private PeerState newPeerState(final EthPeer peer) {
    return new PeerState(peer, maxSnapRequestsPerPeer, maxEthRequestsPerPeer);
  }

  // requests the state and chain downloads may keep in flight, from the adapted lane limits
  private void updateCapacities(final List<PeerState> candidates) {
    int snap = 0;
    int eth = 0;
    int snapBusy = 0;
    int ethBusy = 0;
    for (final PeerState state : candidates) {
      eth += state.eth.slots();
      ethBusy += state.eth.inFlight;
      snapBusy += state.snap.inFlight;
      if (speaks(state.peer, Protocol.SNAP)) {
        snap += state.snap.slots();
      }
    }
    snapCapacity = snap;
    ethCapacity = eth;
    snapInFlight = snapBusy;
    ethInFlight = ethBusy;
    queued = queue.size();
  }

  private PeerState select(
      final Pending<?> pending, final List<PeerState> candidates, final long now) {
    PeerState best = null;
    double bestScore = -1;
    final long minimumHeight = pending.request.minimumPeerHeight();
    for (final PeerState state : candidates) {
      final Protocol protocol = pending.request.protocolFor(state.peer);
      final Lane lane = state.lane(protocol);
      if (!canTake(state, lane, protocol, pending, now)) {
        continue;
      }
      final double score =
          lane.throughput
              / (lane.inFlight + 1)
              * (behind(state.peer, minimumHeight) ? BEHIND_PENALTY : 1);
      if (score > bestScore) {
        best = state;
        bestScore = score;
      }
    }
    return best;
  }

  private static boolean canTake(
      final PeerState state,
      final Lane lane,
      final Protocol protocol,
      final Pending<?> pending,
      final long now) {
    return !state.peer.isDisconnected()
        && lane.isOpen(now)
        && !pending.excluded.contains(state.peer)
        && speaks(state.peer, protocol)
        && pending.request.canServe(state.peer);
  }

  private <T> void send(final Pending<T> pending, final PeerState state) {
    final SyncRequest<T> request = pending.request;
    final EthPeer peer = state.peer;
    final Protocol protocol = request.protocolFor(peer);
    ResponseStream stream = null;
    try {
      stream = peer.send(request.message(peer), protocolName(protocol));
    } catch (final PeerNotConnected e) {
      LOG.trace("Peer {} disconnected before sending {}", peer.getLoggableId(), request.name());
    } catch (final RuntimeException e) {
      LOG.debug("Failed to send {} to {}", request.name(), peer.getLoggableId(), e);
    }
    if (stream == null) {
      state.lane(protocol).backoff(System.nanoTime());
      // the loop thread owns the excluded peers once submitted
      pending.excluded.add(peer);
      queue.add(pending);
      return;
    }
    final Lane lane = state.lane(protocol);
    lane.inFlight++;
    final Attempt<T> attempt = new Attempt<>(pending, state, lane, stream, System.nanoTime());
    attempt.timeoutTask =
        loop.schedule(() -> onTimeout(attempt), timeout.toMillis(), TimeUnit.MILLISECONDS);
    stream.then(
        (closed, message, sender) -> {
          if (message != null) {
            onMessage(attempt, message);
          } else if (closed) {
            onClosed(attempt);
          }
        });
  }

  private <T> void onMessage(final Attempt<T> attempt, final MessageData message) {
    final boolean answered = attempt.state.compareAndSet(AttemptState.PENDING, AttemptState.DONE);
    if (!answered && attempt.state.compareAndSet(AttemptState.TIMED_OUT, AttemptState.RELEASED)) {
      execute(
          () -> {
            attempt.lane.abandoned--;
            dispatchSoon();
          });
    }
    // only once the state is settled: closing calls back synchronously with a closed stream
    attempt.stream.close();
    if (!answered) {
      return;
    }
    final ScheduledFuture<?> timeoutTask = attempt.timeoutTask;
    if (timeoutTask != null) {
      timeoutTask.cancel(false);
    }
    final long elapsedMillis =
        Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - attempt.startedAt));
    final int size = message.getSize();
    try {
      workers.execute(
          () -> {
            final Outcome outcome = decode(attempt, message, size);
            execute(() -> recordAnswer(attempt, outcome, size, elapsedMillis));
          });
    } catch (final RuntimeException rejected) {
      attempt.pending.future.completeExceptionally(stoppedFailure());
    }
  }

  // completes the request with the decoded answer, or fails it
  private <T> Outcome decode(final Attempt<T> attempt, final MessageData message, final int size) {
    final SyncRequest<T> request = attempt.pending.request;
    final CompletableFuture<RequestSender.Response<T>> future = attempt.pending.future;
    final EthPeer peer = attempt.peer.peer;
    try {
      final T value = request.decode(message, peer);
      peer.recordUsefulResponse();
      future.complete(new RequestSender.Response<>(value, peer));
      return Outcome.SUCCESS;
    } catch (final EmptyResponseException e) {
      LOG.debug(
          "Empty {} ({} bytes) from {} {} at height {}: {}",
          request.name(),
          size,
          peer.getLoggableId(),
          peer.getConnection().getPeerInfo().getClientId(),
          peer.chainState().getEstimatedHeight(),
          e.getMessage());
      future.completeExceptionally(failure(EMPTY, peer, e.getMessage()));
      return Outcome.EMPTY;
    } catch (final InvalidResponseException e) {
      punish(peer, request, e.getMessage(), e.disconnectReason());
      future.completeExceptionally(failure(INVALID, peer, e.getMessage()));
      return Outcome.INVALID;
    } catch (final RLPException | IllegalArgumentException e) {
      punish(
          peer,
          request,
          e.getMessage(),
          Optional.of(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED));
      future.completeExceptionally(failure(INVALID, peer, e.toString()));
      return Outcome.INVALID;
    } catch (final RuntimeException e) {
      LOG.warn("Failed to process {} from {}", request.name(), peer.getLoggableId(), e);
      future.completeExceptionally(failure(INVALID, peer, e.toString()));
      return Outcome.ERROR;
    }
  }

  private void recordAnswer(
      final Attempt<?> attempt, final Outcome outcome, final int size, final long elapsedMillis) {
    attempt.lane.inFlight--;
    attempt.lane.record(outcome, size, elapsedMillis, System.nanoTime());
    if (outcome == Outcome.SUCCESS) {
      attempt.peer.consecutiveTimeouts = 0;
    }
    final SyncRequest<?> request = attempt.pending.request;
    countRequest(request, outcome);
    responseBytes.labels(request.name()).inc(size);
    responseMillis.labels(request.name()).inc(elapsedMillis);
    dispatchSoon();
  }

  private void onClosed(final Attempt<?> attempt) {
    if (attempt.state.compareAndSet(AttemptState.PENDING, AttemptState.DONE)) {
      final ScheduledFuture<?> timeoutTask = attempt.timeoutTask;
      if (timeoutTask != null) {
        timeoutTask.cancel(false);
      }
      attempt.pending.future.completeExceptionally(
          failure(DISCONNECTED, attempt.peer.peer, "stream closed"));
      execute(
          () -> {
            attempt.lane.inFlight--;
            countRequest(attempt.pending.request, Outcome.DISCONNECTED);
            dispatchSoon();
          });
    } else if (attempt.state.compareAndSet(AttemptState.TIMED_OUT, AttemptState.RELEASED)) {
      execute(() -> attempt.lane.abandoned--);
    }
  }

  private void onTimeout(final Attempt<?> attempt) {
    if (!attempt.state.compareAndSet(AttemptState.PENDING, AttemptState.TIMED_OUT)) {
      return;
    }
    final PeerState state = attempt.peer;
    attempt.lane.inFlight--;
    attempt.lane.abandoned++;
    attempt.lane.shrink();
    attempt.lane.record(Outcome.TIMEOUT, 0, timeout.toMillis(), System.nanoTime());
    state.consecutiveTimeouts++;
    countRequest(attempt.pending.request, Outcome.TIMEOUT);
    attempt.pending.future.completeExceptionally(
        failure(TIMEOUT, state.peer, attempt.pending.request.name() + " timed out"));
    loop.schedule(() -> abandon(attempt), timeout.toMillis(), TimeUnit.MILLISECONDS);
    if (state.consecutiveTimeouts >= CONSECUTIVE_TIMEOUTS_BEFORE_DISCONNECT) {
      LOG.debug("Disconnecting {} after repeated timeouts", state.peer.getLoggableId());
      state.peer.disconnect(DisconnectReason.TIMEOUT);
    }
    dispatchSoon();
  }

  // a request still unanswered long after its timeout gives its slot back
  private void abandon(final Attempt<?> attempt) {
    if (attempt.state.compareAndSet(AttemptState.TIMED_OUT, AttemptState.RELEASED)) {
      attempt.lane.abandoned--;
      attempt.stream.close();
      dispatchSoon();
    }
  }

  private void punish(
      final EthPeer peer,
      final SyncRequest<?> request,
      final String reason,
      final Optional<DisconnectReason> disconnect) {
    LOG.debug("Invalid {} from {}: {}", request.name(), peer.getLoggableId(), reason);
    disconnect.ifPresentOrElse(peer::disconnect, () -> peer.recordUselessResponse(request.name()));
  }

  private void countRequest(final SyncRequest<?> request, final Outcome outcome) {
    requestCounter.labels(request.name(), outcome.label).inc();
  }

  // a peer that announced a lower head may be syncing, or may just not have announced more
  private static boolean behind(final EthPeer peer, final long minimumHeight) {
    if (minimumHeight <= 0) {
      return false;
    }
    final long height = peer.chainState().getEstimatedHeight();
    return height > 0 && height < minimumHeight;
  }

  // range requests are the same in snap/1 and snap/2, so any snap peer serves them
  private static boolean speaks(final EthPeer peer, final Protocol protocol) {
    return protocol == Protocol.ETH || SnapProtocol.isAgreed(peer.getAgreedCapabilities());
  }

  private static String protocolName(final Protocol protocol) {
    return protocol == Protocol.SNAP ? SnapProtocol.NAME : EthProtocol.NAME;
  }

  private static RequestFailedException failure(
      final RequestFailedException.Reason reason, final EthPeer peer, final String message) {
    return new RequestFailedException(reason, peer, message);
  }

  private static RequestFailedException stoppedFailure() {
    return failure(STOPPED, null, "scheduler stopped");
  }

  private static final class Pending<T> {
    private final SyncRequest<T> request;
    private final Set<EthPeer> excluded;
    private final CompletableFuture<RequestSender.Response<T>> future = new CompletableFuture<>();
    private long sequence;
    private long queuedAt;

    private Pending(final SyncRequest<T> request, final Set<EthPeer> excluded) {
      this.request = request;
      this.excluded = excluded;
    }
  }

  private static final class Attempt<T> {
    private final Pending<T> pending;
    private final PeerState peer;
    private final Lane lane;
    private final ResponseStream stream;
    private final long startedAt;
    private final AtomicReference<AttemptState> state = new AtomicReference<>(AttemptState.PENDING);
    private volatile ScheduledFuture<?> timeoutTask;

    private Attempt(
        final Pending<T> pending,
        final PeerState peer,
        final Lane lane,
        final ResponseStream stream,
        final long startedAt) {
      this.pending = pending;
      this.peer = peer;
      this.lane = lane;
      this.stream = stream;
      this.startedAt = startedAt;
    }
  }

  /**
   * Requests of one protocol to one peer, with an adaptive limit. A peer serving one protocol badly
   * keeps serving the other.
   */
  private static final class Lane {
    private final int max;
    private double limit;
    private int inFlight;
    private int abandoned;
    private double throughput = INITIAL_THROUGHPUT;
    private int consecutiveFailures;
    private long backoffUntil;

    private Lane(final int max) {
      this.max = Math.max(1, max);
      this.limit = Math.min(INITIAL_LANE_LIMIT, this.max);
    }

    private int slots() {
      return (int) limit;
    }

    // not backing off, with a free slot
    private boolean isOpen(final long now) {
      return backoffUntil <= now && inFlight + abandoned < slots();
    }

    private void record(
        final Outcome outcome, final int size, final long elapsedMillis, final long now) {
      switch (outcome) {
        case SUCCESS -> {
          consecutiveFailures = 0;
          throughput = 0.8 * throughput + 0.2 * ((double) size / elapsedMillis);
          grow();
        }
        // a peer missing some data serves the rest: the request alone moves to another peer
        case EMPTY -> throughput *= 0.9;
        default -> {
          throughput *= 0.7;
          backoff(now);
        }
      }
    }

    private void backoff(final long now) {
      consecutiveFailures++;
      final long seconds =
          Math.min(MAX_BACKOFF_SECONDS, 1L << Math.min(consecutiveFailures - 1, 5));
      backoffUntil = now + TimeUnit.SECONDS.toNanos(seconds);
    }

    // additive increase: about one more slot per window of answered requests
    private void grow() {
      limit = Math.min(max, limit + 1.0 / slots());
    }

    // multiplicative decrease on a timeout
    private void shrink() {
      limit = Math.max(1, limit / 2);
    }
  }

  /** Bookkeeping of one peer, owned by the loop thread. */
  private static final class PeerState {
    private final EthPeer peer;
    private final Lane snap;
    private final Lane eth;
    private int consecutiveTimeouts;

    private PeerState(final EthPeer peer, final int maxSnap, final int maxEth) {
      this.peer = peer;
      this.snap = new Lane(maxSnap);
      this.eth = new Lane(maxEth);
    }

    private Lane lane(final Protocol protocol) {
      return protocol == Protocol.SNAP ? snap : eth;
    }
  }
}
