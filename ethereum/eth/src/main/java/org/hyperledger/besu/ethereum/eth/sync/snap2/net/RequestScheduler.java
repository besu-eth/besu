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
import org.hyperledger.besu.ethereum.eth.SnapProtocolVersion;
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
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends the sync requests to the peers, most urgent first, each to the fastest peer with capacity
 * left. A peer has one lane per protocol whose capacity grows with answers and halves on timeouts;
 * a timed out request keeps its slot until its late answer or one more timeout. One request of each
 * peer is always left to the other downloads.
 *
 * <p>The SNAP2 downloads go through it rather than the {@code PeerTaskExecutor}, and a merge of the
 * two must keep what it does that the executor does not:
 *
 * <ul>
 *   <li>an empty answer means the peer lacks the data, as pruned bodies, so the peer is not
 *       punished, where the executor counts a useless response and five in a minute disconnect it;
 *   <li>a request that failed on a peer goes to another peer, not to the best one again;
 *   <li>a request waits for a peer with capacity instead of failing at once;
 *   <li>peers are rated per kind of request, and the requests of a sync are served by priority.
 * </ul>
 */
public final class RequestScheduler implements RequestSender {

  private static final Logger LOG = LoggerFactory.getLogger(RequestScheduler.class);
  // the rate guessed for a request type no peer answered yet: 64 bytes per millisecond
  private static final double INITIAL_BYTES = 64_000;
  private static final double INITIAL_MILLIS = 1_000;
  private static final int INITIAL_LANE_LIMIT = 2;
  // the other downloads only pick a peer below this many outstanding requests
  private static final int PEER_REQUEST_LIMIT = EthPeer.MAX_OUTSTANDING_REQUESTS - 1;
  private static final int MAX_BACKOFF_SECONDS = 30;
  private static final int CONSECUTIVE_TIMEOUTS_BEFORE_DISCONNECT = 12;
  private static final long RETRY_PAUSE_MILLIS = 1_000;
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

  /** Which connected peers serve a request, busy or not. */
  private enum Servers {
    NONE,
    EXCLUDED,
    AVAILABLE
  }

  private final EthPeers ethPeers;
  private final Duration timeout;
  private final Duration queueTimeout;
  private final int maxSnapRequestsPerPeer;
  private final int maxEthRequestsPerPeer;
  private final ScheduledExecutorService loop;
  private final ExecutorService workers;
  private final LongSupplier nanoTime;
  private final LabelledMetric<Counter> requestCounter;
  private final LabelledMetric<Counter> responseBytes;
  private final LabelledMetric<Counter> responseMillis;

  private final PriorityQueue<Pending<?>> queue =
      new PriorityQueue<>(
          Comparator.<Pending<?>>comparingInt(p -> p.request.priority().rank())
              .thenComparingLong(p -> p.sequence));
  private final Map<EthPeer, PeerState> peers = new HashMap<>();
  // what the peers answered lately per request type, the guess for a peer not asked yet
  private final Map<String, Rate> typicalRates = new HashMap<>();
  // every request not yet answered, failed when the scheduler stops
  private final Set<CompletableFuture<?>> outstanding = ConcurrentHashMap.newKeySet();
  // every attempt whose stream is open, closed when the scheduler stops
  private final Set<Attempt<?>> open = ConcurrentHashMap.newKeySet();
  private long sequence;
  private final long connectSubscription;
  private volatile Map<Protocol, Integer> capacities = Map.of();
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
    this(
        ethPeers,
        metricsSystem,
        timeout,
        maxSnapRequestsPerPeer,
        maxEthRequestsPerPeer,
        workerThreads,
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("snap2-scheduler").daemon().factory()),
        System::nanoTime);
  }

  @VisibleForTesting
  RequestScheduler(
      final EthPeers ethPeers,
      final MetricsSystem metricsSystem,
      final Duration timeout,
      final int maxSnapRequestsPerPeer,
      final int maxEthRequestsPerPeer,
      final int workerThreads,
      final ScheduledExecutorService loop,
      final LongSupplier nanoTime) {
    this.ethPeers = ethPeers;
    this.timeout = timeout;
    this.queueTimeout = timeout.multipliedBy(6);
    this.maxSnapRequestsPerPeer = maxSnapRequestsPerPeer;
    this.maxEthRequestsPerPeer = maxEthRequestsPerPeer;
    this.loop = loop;
    this.nanoTime = nanoTime;
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
    capacity.labels(() -> capacities.getOrDefault(Protocol.SNAP, 0), "snap");
    capacity.labels(() -> capacities.getOrDefault(Protocol.ETH, 0), "eth");
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap2_requests_queued",
        "Requests of the SNAP2 sync waiting for a peer",
        () -> queued);
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
    return enqueue(request, excluded);
  }

  private <T> CompletableFuture<RequestSender.Response<T>> enqueue(
      final SyncRequest<T> request, final Set<EthPeer> excluded) {
    final Pending<T> pending = new Pending<>(request, new HashSet<>(excluded));
    track(pending.future);

    // tracked first, so that a concurrent stop either fails it or is seen here
    final boolean accepted =
        !stopped
            && execute(
                () -> {
                  pending.sequence = sequence++;
                  pending.waitingSince = nanoTime.getAsLong();
                  queue.add(pending);
                  dispatchSoon();
                });
    if (!accepted) {
      pending.future.completeExceptionally(stoppedFailure());
    }
    return pending.future;
  }

  private void track(final CompletableFuture<?> future) {
    outstanding.add(future);
    future.whenComplete((response, error) -> outstanding.remove(future));
  }

  @Override
  public <T> CompletableFuture<RequestSender.Response<T>> submitWithRetries(
      final SyncRequest<T> request, final int attempts) {
    final CompletableFuture<RequestSender.Response<T>> result = new CompletableFuture<>();
    // also failed by a stop during the pause between two attempts
    track(result);
    attempt(request, new HashSet<>(), attempts, result);
    return result;
  }

  private <T> void attempt(
      final SyncRequest<T> request,
      final Set<EthPeer> excluded,
      final int attemptsLeft,
      final CompletableFuture<RequestSender.Response<T>> result) {
    enqueue(request, excluded)
        .whenComplete(
            (response, error) -> {
              if (error == null) {
                result.complete(response);
                return;
              }

              final RequestFailedException failure =
                  RequestFailedException.from(error).orElse(null);
              // a retry that only found the peers it left out sent nothing: no attempt used
              final boolean leftOut =
                  failure != null && failure.reason() == NO_PEER && !excluded.isEmpty();
              // an error of the request itself would fail again on any peer
              if (failure == null
                  || (attemptsLeft <= 1 && !leftOut)
                  || failure.reason() == STOPPED
                  || result.isDone()) {
                result.completeExceptionally(failure != null ? failure : error);
                return;
              }

              if (failure.reason() == NO_PEER) {
                // every peer that serves it failed it, start over after a pause
                excluded.clear();
                final int left = leftOut ? attemptsLeft : attemptsLeft - 1;
                if (schedule(() -> attempt(request, excluded, left, result), RETRY_PAUSE_MILLIS)
                    == null) {
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
    return (int) validatedPeers().filter(peer -> speaks(peer, protocol)).count();
  }

  @Override
  public long bestPeerHeight() {
    final long[] heights =
        validatedPeers()
            .mapToLong(peer -> peer.chainState().getEstimatedHeight())
            .filter(height -> height > 0)
            .sorted()
            .toArray();
    // the second highest, so that a single peer cannot raise it
    return heights.length == 0 ? 0 : heights[Math.max(0, heights.length - 2)];
  }

  @Override
  public int capacity(final Protocol protocol) {
    final Integer adapted = capacities.get(protocol);
    // before the first dispatch, what new lanes take
    return adapted != null ? adapted : peerCount(protocol) * INITIAL_LANE_LIMIT;
  }

  /** Stops dispatching, fails every request not yet answered and closes their streams. */
  @Override
  public void stop() {
    stopped = true;
    ethPeers.unsubscribeConnect(connectSubscription);
    // timeouts still scheduled would fire on failed requests
    loop.shutdownNow();
    // work of the callers still queued there runs, so that their futures complete
    workers.shutdown();
    final RequestFailedException failure = stoppedFailure();
    outstanding.forEach(future -> future.completeExceptionally(failure));
    // a peer counts a request as outstanding until its stream is closed
    open.forEach(attempt -> attempt.stream.close());
  }

  // false once the loop is stopped
  private boolean execute(final Runnable task) {
    try {
      loop.execute(() -> runLogged(task));
      return true;
    } catch (final RejectedExecutionException stopping) {
      LOG.trace("Scheduler loop rejected a task", stopping);
      return false;
    }
  }

  // null once the loop is stopped
  private ScheduledFuture<?> schedule(final Runnable task, final long delayMillis) {
    try {
      return loop.schedule(() -> runLogged(task), delayMillis, TimeUnit.MILLISECONDS);
    } catch (final RejectedExecutionException stopping) {
      LOG.trace("Scheduler loop rejected a task", stopping);
      return null;
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

  // an executor keeps the exceptions of its tasks to itself
  private static void runLogged(final Runnable task) {
    try {
      task.run();
    } catch (final RuntimeException e) {
      LOG.warn("SNAP2 scheduler task failed", e);
    }
  }

  // the peers a dispatch may use
  private Stream<EthPeer> validatedPeers() {
    return ethPeers
        .streamAvailablePeers()
        .filter(EthPeerImmutableAttributes::isFullyValidated)
        .map(EthPeerImmutableAttributes::ethPeer);
  }

  private void dispatch() {
    if (stopped) {
      return;
    }

    final long now = nanoTime.getAsLong();
    peers.values().removeIf(state -> state.peer.isDisconnected());
    final List<PeerState> candidates = new ArrayList<>();
    validatedPeers()
        .forEach(
            peer -> {
              final PeerState state = peers.computeIfAbsent(peer, this::newPeerState);
              state.room = PEER_REQUEST_LIMIT - peer.outstandingRequests();
              candidates.add(state);
            });

    final List<Pending<?>> deferred = new ArrayList<>();
    try {
      Pending<?> pending;
      while ((pending = queue.poll()) != null) {
        if (pending.future.isDone()) {
          continue;
        }
        try {
          dispatch(pending, candidates, now, deferred);
        } catch (final RuntimeException e) {
          // the code of the request failed, it would on any peer
          LOG.warn("Failed to dispatch a SNAP2 request", e);
          pending.future.completeExceptionally(e);
          countRequest(pending.request.name(), Outcome.ERROR);
        }
      }
    } finally {
      queue.addAll(deferred);
      updateCapacities(candidates, now);
    }
  }

  // sends the request to the best peer that takes it now, or keeps it until one does
  private void dispatch(
      final Pending<?> pending,
      final List<PeerState> candidates,
      final long now,
      final List<Pending<?>> deferred) {
    final PeerState peer = select(pending, candidates, now);
    if (peer != null) {
      send(pending, peer);
      return;
    }

    final boolean expired = now - pending.waitingSince > queueTimeout.toNanos();
    final boolean excluding = !pending.excluded.isEmpty();
    if (expired || excluding) {
      final Servers servers = servers(pending, candidates);
      if (servers == Servers.AVAILABLE) {
        // busy peers serve it: it waits for them
        pending.waitingSince = now;
      } else if (expired || servers == Servers.EXCLUDED) {
        // the caller hears at once when only the peers it left out serve it
        countRequest(pending.request.name(), Outcome.NO_PEER);
        pending.future.completeExceptionally(
            failure(NO_PEER, null, "no peer could serve " + pending.request.name()));
        return;
      }
    }
    deferred.add(pending);
  }

  private PeerState newPeerState(final EthPeer peer) {
    return new PeerState(peer, maxSnapRequestsPerPeer, maxEthRequestsPerPeer);
  }

  // requests the downloads may keep in flight: those in flight and those the lanes still take
  private void updateCapacities(final List<PeerState> candidates, final long now) {
    int eth = 0;
    int snap = 0;
    int snap2 = 0;
    int ethBusy = 0;
    int snapBusy = 0;
    for (final PeerState state : candidates) {
      final int snapLane = state.snap.capacity(now, state.room);
      eth += state.eth.capacity(now, state.room);
      if (speaks(state.peer, Protocol.SNAP)) {
        snap += snapLane;
      }
      if (speaks(state.peer, Protocol.SNAP2)) {
        snap2 += snapLane;
      }
      ethBusy += state.eth.inFlight;
      snapBusy += state.snap.inFlight;
    }
    capacities = Map.of(Protocol.ETH, eth, Protocol.SNAP, snap, Protocol.SNAP2, snap2);
    ethInFlight = ethBusy;
    snapInFlight = snapBusy;
    queued = queue.size();
  }

  private PeerState select(
      final Pending<?> pending, final List<PeerState> candidates, final long now) {
    PeerState best = null;
    double bestScore = -1;
    final String name = pending.request.name();
    final long minimumHeight = pending.request.minimumPeerHeight();
    for (final PeerState state : candidates) {
      final Protocol protocol = pending.request.protocolFor(state.peer);
      final Lane lane = state.lane(protocol);
      if (!canTake(state, lane, protocol, pending, now)) {
        continue;
      }
      final double score =
          rate(lane, name).perMilli()
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
    return state.room > 0
        && !state.peer.isDisconnected()
        && lane.isOpen(now)
        && !pending.excluded.contains(state.peer)
        && speaks(state.peer, protocol)
        && pending.request.canServe(state.peer);
  }

  private static Servers servers(final Pending<?> pending, final List<PeerState> candidates) {
    boolean excluded = false;
    for (final PeerState state : candidates) {
      final EthPeer peer = state.peer;
      if (peer.isDisconnected()
          || !speaks(peer, pending.request.protocolFor(peer))
          || !pending.request.canServe(peer)) {
        continue;
      }
      if (!pending.excluded.contains(peer)) {
        return Servers.AVAILABLE;
      }
      excluded = true;
    }
    return excluded ? Servers.EXCLUDED : Servers.NONE;
  }

  private <T> void send(final Pending<T> pending, final PeerState state) {
    final SyncRequest<T> request = pending.request;
    final EthPeer peer = state.peer;
    final String name = request.name();
    final Protocol protocol = request.protocolFor(peer);
    final MessageData message = request.message(peer);

    final long sentAt = nanoTime.getAsLong();
    ResponseStream stream = null;
    try {
      stream = peer.send(message, protocolName(protocol));
    } catch (final PeerNotConnected e) {
      LOG.trace("Peer {} disconnected before sending {}", peer.getLoggableId(), name);
    } catch (final RuntimeException e) {
      LOG.debug("Failed to send {} to {}", name, peer.getLoggableId(), e);
    }
    if (stream == null) {
      // this peer did not take it, another one may: no lane is to blame
      pending.excluded.add(peer);
      queue.add(pending);
      return;
    }

    final Lane lane = state.lane(protocol);
    lane.inFlight++;
    state.room--;
    final Attempt<T> attempt = new Attempt<>(pending, state, lane, name, stream, sentAt);
    open.add(attempt);
    // tracked first, so that a concurrent stop either closes the stream or is seen here
    attempt.timeoutTask = stopped ? null : schedule(() -> onTimeout(attempt), timeout.toMillis());
    if (attempt.timeoutTask == null) {
      stream.close();
      return;
    }

    stream.then(
        (closed, response, sender) -> {
          if (response != null) {
            onMessage(attempt, response);
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
        Math.max(1, TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - attempt.startedAt));
    final int size = message.getSize();

    try {
      workers.execute(() -> runLogged(() -> answer(attempt, message, size, elapsedMillis)));
    } catch (final RejectedExecutionException stopping) {
      attempt.pending.future.completeExceptionally(stoppedFailure());
    }
  }

  private <T> void answer(
      final Attempt<T> attempt,
      final MessageData message,
      final int size,
      final long elapsedMillis) {
    if (stopped) {
      // the stop failed the request
      return;
    }

    final Answer<T> answer = decode(attempt, message, size);
    // queued first: the continuations of the caller run on this thread and may block it
    execute(() -> recordAnswer(attempt, answer.outcome(), size, elapsedMillis));
    answer.complete(attempt.pending.future);
  }

  private <T> Answer<T> decode(
      final Attempt<T> attempt, final MessageData message, final int size) {
    final SyncRequest<T> request = attempt.pending.request;
    final EthPeer peer = attempt.peer.peer;
    try {
      final T value = request.decode(message, peer);
      peer.recordUsefulResponse();
      return new Answer<>(Outcome.SUCCESS, new RequestSender.Response<>(value, peer), null);

    } catch (final EmptyResponseException e) {
      LOG.debug(
          "Empty {} ({} bytes) from {} {} at height {}: {}",
          request.name(),
          size,
          peer.getLoggableId(),
          peer.getConnection().getPeerInfo().getClientId(),
          peer.chainState().getEstimatedHeight(),
          e.getMessage());
      return new Answer<>(Outcome.EMPTY, null, failure(EMPTY, peer, e.getMessage()));

    } catch (final InvalidResponseException e) {
      punish(peer, request, e.getMessage(), e.disconnectReason());
      return new Answer<>(Outcome.INVALID, null, failure(INVALID, peer, e.getMessage()));

    } catch (final RLPException e) {
      // only the content of a response can be malformed: it reaches the stream of its request
      punish(
          peer,
          request,
          e.getMessage(),
          Optional.of(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED));
      return new Answer<>(Outcome.INVALID, null, failure(INVALID, peer, e.toString()));

    } catch (final RuntimeException e) {
      // as likely a bug of ours: the request moves on, the peer is not punished
      LOG.warn("Failed to process {} from {}", request.name(), peer.getLoggableId(), e);
      return new Answer<>(Outcome.ERROR, null, failure(INVALID, peer, e.toString()));
    }
  }

  private void recordAnswer(
      final Attempt<?> attempt, final Outcome outcome, final int size, final long elapsedMillis) {
    final Lane lane = attempt.lane;
    lane.inFlight--;
    lane.record(outcome, attempt.startedAt, nanoTime.getAsLong());
    measure(lane, attempt.name, outcome, size, elapsedMillis);
    if (outcome == Outcome.SUCCESS) {
      attempt.peer.consecutiveTimeouts = 0;
    }

    countRequest(attempt.name, outcome);
    responseBytes.labels(attempt.name).inc(size);
    responseMillis.labels(attempt.name).inc(elapsedMillis);
    dispatchSoon();
  }

  private void onClosed(final Attempt<?> attempt) {
    open.remove(attempt);
    if (attempt.state.compareAndSet(AttemptState.PENDING, AttemptState.DONE)) {
      final ScheduledFuture<?> timeoutTask = attempt.timeoutTask;
      if (timeoutTask != null) {
        timeoutTask.cancel(false);
      }
      execute(
          () -> {
            attempt.lane.inFlight--;
            countRequest(attempt.name, Outcome.DISCONNECTED);
            dispatchSoon();
          });
      attempt.pending.future.completeExceptionally(
          failure(DISCONNECTED, attempt.peer.peer, "stream closed"));
    } else if (attempt.state.compareAndSet(AttemptState.TIMED_OUT, AttemptState.RELEASED)) {
      execute(() -> attempt.lane.abandoned--);
    }
  }

  private void onTimeout(final Attempt<?> attempt) {
    if (!attempt.state.compareAndSet(AttemptState.PENDING, AttemptState.TIMED_OUT)) {
      return;
    }

    final PeerState state = attempt.peer;
    final Lane lane = attempt.lane;
    lane.inFlight--;
    lane.abandoned++;
    lane.record(Outcome.TIMEOUT, attempt.startedAt, nanoTime.getAsLong());
    measure(lane, attempt.name, Outcome.TIMEOUT, 0, timeout.toMillis());
    state.consecutiveTimeouts++;
    countRequest(attempt.name, Outcome.TIMEOUT);

    schedule(() -> abandon(attempt), timeout.toMillis());
    if (state.consecutiveTimeouts >= CONSECUTIVE_TIMEOUTS_BEFORE_DISCONNECT) {
      LOG.debug("Disconnecting {} after repeated timeouts", state.peer.getLoggableId());
      state.peer.disconnect(DisconnectReason.TIMEOUT);
    }
    dispatchSoon();

    attempt.pending.future.completeExceptionally(
        failure(TIMEOUT, state.peer, attempt.name + " timed out"));
  }

  // a request still unanswered long after its timeout gives its slot back
  private void abandon(final Attempt<?> attempt) {
    if (attempt.state.compareAndSet(AttemptState.TIMED_OUT, AttemptState.RELEASED)) {
      attempt.lane.abandoned--;
      attempt.stream.close();
      dispatchSoon();
    }
  }

  // every answer counts with its time, only a useful one with its bytes; an error may be ours
  private void measure(
      final Lane lane,
      final String request,
      final Outcome outcome,
      final int size,
      final long elapsedMillis) {
    if (outcome == Outcome.ERROR) {
      return;
    }
    final int bytes = outcome == Outcome.SUCCESS ? size : 0;
    lane.rates.computeIfAbsent(request, r -> typicalRate(r).copy()).add(bytes, elapsedMillis);
    if (outcome == Outcome.SUCCESS) {
      typicalRate(request).add(size, elapsedMillis);
    }
  }

  // the rate of a peer for a request type, what the peers answered lately if it was never asked
  private Rate rate(final Lane lane, final String request) {
    final Rate rate = lane.rates.get(request);
    return rate != null ? rate : typicalRate(request);
  }

  private Rate typicalRate(final String request) {
    return typicalRates.computeIfAbsent(request, r -> new Rate(INITIAL_BYTES, INITIAL_MILLIS));
  }

  private void punish(
      final EthPeer peer,
      final SyncRequest<?> request,
      final String reason,
      final Optional<DisconnectReason> disconnect) {
    LOG.debug("Invalid {} from {}: {}", request.name(), peer.getLoggableId(), reason);
    disconnect.ifPresentOrElse(peer::disconnect, () -> peer.recordUselessResponse(request.name()));
  }

  private void countRequest(final String request, final Outcome outcome) {
    requestCounter.labels(request, outcome.label).inc();
  }

  // a peer that announced a lower head may be syncing, or may just not have announced more
  private static boolean behind(final EthPeer peer, final long minimumHeight) {
    if (minimumHeight <= 0) {
      return false;
    }
    final long height = peer.chainState().getEstimatedHeight();
    return height > 0 && height < minimumHeight;
  }

  private static boolean speaks(final EthPeer peer, final Protocol protocol) {
    return switch (protocol) {
      case ETH -> true;
      // range requests are the same in snap/1 and snap/2, so any snap peer serves them
      case SNAP -> SnapProtocol.isAgreed(peer.getAgreedCapabilities());
      case SNAP2 -> peer.getAgreedCapabilities().contains(SnapProtocolVersion.V2.getCapability());
    };
  }

  private static String protocolName(final Protocol protocol) {
    return protocol == Protocol.ETH ? EthProtocol.NAME : SnapProtocol.NAME;
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
    // when it was queued, or last found waiting for a busy peer
    private long waitingSince;

    private Pending(final SyncRequest<T> request, final Set<EthPeer> excluded) {
      this.request = request;
      this.excluded = excluded;
    }
  }

  private static final class Attempt<T> {
    private final Pending<T> pending;
    private final PeerState peer;
    private final Lane lane;
    private final String name;
    private final ResponseStream stream;
    private final long startedAt;
    private final AtomicReference<AttemptState> state = new AtomicReference<>(AttemptState.PENDING);
    private volatile ScheduledFuture<?> timeoutTask;

    private Attempt(
        final Pending<T> pending,
        final PeerState peer,
        final Lane lane,
        final String name,
        final ResponseStream stream,
        final long startedAt) {
      this.pending = pending;
      this.peer = peer;
      this.lane = lane;
      this.name = name;
      this.stream = stream;
      this.startedAt = startedAt;
    }
  }

  /** A decoded response, or why there is none. */
  private record Answer<T>(
      Outcome outcome, RequestSender.Response<T> response, RequestFailedException failure) {

    private void complete(final CompletableFuture<RequestSender.Response<T>> future) {
      if (failure == null) {
        future.complete(response);
      } else {
        future.completeExceptionally(failure);
      }
    }
  }

  /**
   * Bytes and milliseconds of the recent answers of one request type, decayed alike: a timeout or
   * an empty answer weighs as the time it took.
   */
  private static final class Rate {
    private double bytes;
    private double millis;

    private Rate(final double bytes, final double millis) {
      this.bytes = bytes;
      this.millis = millis;
    }

    private Rate copy() {
      return new Rate(bytes, millis);
    }

    private double perMilli() {
      return bytes / millis;
    }

    private void add(final int size, final long elapsedMillis) {
      bytes = 0.8 * bytes + 0.2 * size;
      millis = 0.8 * millis + 0.2 * elapsedMillis;
    }
  }

  /**
   * Requests of one protocol to one peer, with an adaptive limit. A peer serving one protocol badly
   * keeps serving the other.
   */
  private static final class Lane {
    private final int max;
    // per request type, so that small answers compare with small answers
    private final Map<String, Rate> rates = new HashMap<>();
    private double limit;
    private int inFlight;
    private int abandoned;
    private int consecutiveFailures;
    private long backoffUntil;
    // when a failure last backed off the lane
    private long failedAt = Long.MIN_VALUE;

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

    // the requests in flight, and those it takes now within the room the peer leaves
    private int capacity(final long now, final int room) {
      final int free = backoffUntil > now ? 0 : Math.max(0, slots() - inFlight - abandoned);
      return inFlight + Math.clamp(room, 0, free);
    }

    private void record(final Outcome outcome, final long sentAt, final long now) {
      switch (outcome) {
        case SUCCESS -> {
          consecutiveFailures = 0;
          grow();
        }
        // a peer missing some data serves the rest, and an error may be ours: the request alone
        // moves to another peer
        case EMPTY, ERROR -> {}
        default -> fail(outcome, sentAt, now);
      }
    }

    // requests in flight together fail together: their burst backs off and shrinks the lane once
    private void fail(final Outcome outcome, final long sentAt, final long now) {
      if (sentAt < failedAt) {
        return;
      }
      failedAt = now;
      if (outcome == Outcome.TIMEOUT) {
        shrink();
      }
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
    // requests it takes before the other downloads would find it busy, set at each dispatch
    private int room;

    private PeerState(final EthPeer peer, final int maxSnap, final int maxEth) {
      this.peer = peer;
      this.snap = new Lane(maxSnap);
      this.eth = new Lane(maxEth);
    }

    private Lane lane(final Protocol protocol) {
      return protocol == Protocol.ETH ? eth : snap;
    }
  }
}
