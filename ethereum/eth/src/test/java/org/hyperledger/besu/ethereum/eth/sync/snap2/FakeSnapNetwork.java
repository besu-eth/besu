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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.EMPTY;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.INVALID;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.NO_PEER;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetStorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.RangeProofs;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Stream;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Serves the snap requests of the state download from in-memory worlds, with real range proofs, in
 * place of the request scheduler. A request waits until the test answers it; one that no peer takes
 * fails at once, as with the scheduler when only peers left out serve it.
 */
public final class FakeSnapNetwork implements RequestSender {

  // requests a peer takes at once
  private static final int PEER_CAPACITY = 4;

  private final List<EthPeer> peers = Stream.generate(() -> mock(EthPeer.class)).limit(3).toList();
  private final Map<Bytes32, ServedState> states = new HashMap<>();
  private final Map<Bytes32, Bytes> codes = new HashMap<>();
  private final Set<EthPeer> withoutState = new HashSet<>();
  private final List<Sent<?>> unanswered = new ArrayList<>();
  private final List<Bytes32> accountRangeOrigins = new ArrayList<>();
  private final Map<EthPeer, Integer> stateRequests = new HashMap<>();
  private final Map<EthPeer, Integer> codeRequests = new HashMap<>();
  private final Set<EthPeer> disconnected = new HashSet<>();
  // fails every request at once, if set
  private Throwable failure;
  private int requests;
  private int nextPeer;
  private int maxResponseBytes = Integer.MAX_VALUE;

  private record Sent<T>(
      SyncRequest<T> request,
      EthPeer peer,
      MessageData message,
      CompletableFuture<Response<T>> future) {}

  /** Starts serving a world state. */
  public void serve(final TestWorld world) {
    states.put(world.root(), new ServedState(world));
    codes.putAll(world.codes());
  }

  /** Caps every response, to force many requests and split storages. */
  public void maxResponseBytes(final int bytes) {
    this.maxResponseBytes = bytes;
  }

  /** Makes a peer answer every account and storage range request with nothing. */
  public void serveNoState(final int peer) {
    withoutState.add(peers.get(peer));
  }

  public List<EthPeer> peers() {
    return peers;
  }

  /** Disconnects every peer, so that no peer takes a request. */
  public void disconnect() {
    disconnected.addAll(peers);
  }

  /** Disconnects a peer, so that it takes no more requests. */
  public void disconnect(final int peer) {
    disconnected.add(peers.get(peer));
  }

  public void reconnect() {
    disconnected.clear();
  }

  /** Fails every request at once with this error, as a stopped scheduler does. */
  public void failEveryRequest(final Throwable error) {
    failure = error;
  }

  /** Returns whether requests wait for an answer. */
  public boolean hasUnanswered() {
    return !unanswered.isEmpty();
  }

  /** Returns the names of the requests that wait for an answer, in the order they were sent. */
  public List<String> unansweredRequests() {
    return unanswered.stream().map(sent -> sent.request().name()).toList();
  }

  /** Answers the requests sent so far, in the order they were sent. */
  public void answerAll() {
    final List<Sent<?>> answering = List.copyOf(unanswered);
    unanswered.clear();
    answering.forEach(this::answer);
  }

  /** Returns the number of requests waiting for an answer that the download withdrew. */
  public long withdrawnRequestCount() {
    return unanswered.stream().filter(sent -> sent.future().isCancelled()).count();
  }

  /** Returns the number of requests sent, those that failed at once included. */
  public int requestCount() {
    return requests;
  }

  /** Returns the account and storage range requests a peer received. */
  public int stateRequestsTo(final EthPeer peer) {
    return stateRequests.getOrDefault(peer, 0);
  }

  /** Returns the account and storage range requests all peers received. */
  public int stateRequestCount() {
    return stateRequests.values().stream().mapToInt(Integer::intValue).sum();
  }

  /** Returns the bytecode requests a peer received. */
  public int codeRequestsTo(final EthPeer peer) {
    return codeRequests.getOrDefault(peer, 0);
  }

  /** Returns the bytecode requests all peers received. */
  public int codeRequestCount() {
    return codeRequests.values().stream().mapToInt(Integer::intValue).sum();
  }

  /** Returns the origins of the account range requests, in the order they were sent. */
  public List<Bytes32> accountRangeOrigins() {
    return accountRangeOrigins;
  }

  @Override
  public <T> CompletableFuture<Response<T>> submit(
      final SyncRequest<T> request, final Set<EthPeer> excluded) {
    requests++;
    if (failure != null) {
      return CompletableFuture.failedFuture(failure);
    }
    final Optional<EthPeer> peer = nextPeer(excluded);
    if (peer.isEmpty()) {
      return CompletableFuture.failedFuture(
          new RequestFailedException(NO_PEER, null, "no peer takes " + request.name()));
    }
    final MessageData message = request.message(peer.get());
    (message.getCode() == SnapV2.GET_BYTECODES ? codeRequests : stateRequests)
        .merge(peer.get(), 1, Integer::sum);
    if (message.getCode() == SnapV2.GET_ACCOUNT_RANGE) {
      accountRangeOrigins.add(
          Bytes32.wrap(
              GetAccountRangeMessage.readFrom(message).range(false).startKeyHash().getBytes()));
    }
    final Sent<T> sent = new Sent<>(request, peer.get(), message, new CompletableFuture<>());
    unanswered.add(sent);
    return sent.future();
  }

  @Override
  public <T> CompletableFuture<Response<T>> submitWithRetries(
      final SyncRequest<T> request, final int attempts) {
    return submit(request);
  }

  @Override
  public int peerCount(final Protocol protocol) {
    return peers.size() - disconnected.size();
  }

  @Override
  public int capacity(final Protocol protocol) {
    return peerCount(protocol) * PEER_CAPACITY;
  }

  // the workers of the scheduler: the thread of the test
  @Override
  public Executor workers() {
    return Runnable::run;
  }

  // the next connected peer in turn not left out
  private Optional<EthPeer> nextPeer(final Set<EthPeer> excluded) {
    for (int i = 0; i < peers.size(); i++) {
      final EthPeer peer = peers.get(nextPeer);
      nextPeer = (nextPeer + 1) % peers.size();
      if (!excluded.contains(peer) && !disconnected.contains(peer)) {
        return Optional.of(peer);
      }
    }
    return Optional.empty();
  }

  private <T> void answer(final Sent<T> sent) {
    // snap responses reach a request with their request id
    final MessageData response =
        respond(sent.peer(), sent.message()).wrapMessageData(BigInteger.ONE);
    final T value;
    try {
      value = sent.request().decode(response, sent.peer());
    } catch (final SyncRequest.EmptyResponseException e) {
      sent.future()
          .completeExceptionally(new RequestFailedException(EMPTY, sent.peer(), e.getMessage()));
      return;
    } catch (final SyncRequest.InvalidResponseException e) {
      sent.future()
          .completeExceptionally(new RequestFailedException(INVALID, sent.peer(), e.getMessage()));
      return;
    }
    sent.future().complete(new Response<>(value, sent.peer()));
  }

  private MessageData respond(final EthPeer peer, final MessageData message) {
    if (message.getCode() == SnapV2.GET_BYTECODES) {
      return byteCodes(GetByteCodesMessage.readFrom(message));
    }
    final boolean accounts = message.getCode() == SnapV2.GET_ACCOUNT_RANGE;
    if (withoutState.contains(peer)) {
      return accounts
          ? AccountRangeMessage.create(new TreeMap<>(), List.of())
          : StorageRangeMessage.create(new ArrayDeque<>(), List.of());
    }
    return accounts
        ? accountRange(GetAccountRangeMessage.readFrom(message))
        : storageRanges(GetStorageRangeMessage.readFrom(message));
  }

  private MessageData accountRange(final GetAccountRangeMessage message) {
    final GetAccountRangeMessage.Range range = message.range(false);
    final ServedState state = states.get(Bytes32.wrap(range.worldStateRootHash().getBytes()));
    if (state == null) {
      return AccountRangeMessage.create(new TreeMap<>(), List.of());
    }
    final Bytes32 origin = Bytes32.wrap(range.startKeyHash().getBytes());
    final Bytes32 limit = Bytes32.wrap(range.endKeyHash().getBytes());
    final long bytes = Math.min(range.responseBytes().longValue(), maxResponseBytes);
    final TreeMap<Bytes32, Bytes> out = new TreeMap<>();
    long size = 0;
    for (final Map.Entry<Bytes32, Bytes> entry : state.accounts.tailMap(origin, true).entrySet()) {
      out.put(entry.getKey(), entry.getValue());
      size += 32 + entry.getValue().size();
      if (entry.getKey().compareTo(limit) >= 0 || size >= bytes) {
        break;
      }
    }
    final boolean whole = origin.isZero() && out.size() == state.accounts.size();
    return AccountRangeMessage.create(
        out, whole ? List.of() : proof(state.accountTrie, origin, out));
  }

  private MessageData storageRanges(final GetStorageRangeMessage message) {
    final GetStorageRangeMessage.StorageRange range = message.range(false);
    final ServedState state = states.get(Bytes32.wrap(range.worldStateRootHash().getBytes()));
    final ArrayDeque<NavigableMap<Bytes32, Bytes>> slots = new ArrayDeque<>();
    if (state == null) {
      return StorageRangeMessage.create(slots, List.of());
    }
    final Bytes32 origin = Bytes32.wrap(range.startKeyHash().getBytes());
    final Bytes32 limit = Bytes32.wrap(range.endKeyHash().getBytes());
    final long bytes = Math.min(range.responseBytes().longValue(), maxResponseBytes);
    long size = 0;
    for (final Bytes32 account : range.accountHashes()) {
      final NavigableMap<Bytes32, Bytes> storage = state.storage.get(account);
      final TreeMap<Bytes32, Bytes> out = new TreeMap<>();
      boolean truncated = false;
      for (final Map.Entry<Bytes32, Bytes> entry : storage.tailMap(origin, true).entrySet()) {
        if (size >= bytes) {
          truncated = true;
          break;
        }
        out.put(entry.getKey(), entry.getValue());
        size += 32 + entry.getValue().size();
        if (entry.getKey().compareTo(limit) >= 0) {
          truncated = entry.getKey().compareTo(storage.lastKey()) < 0;
          break;
        }
      }
      // a server sends no slot list for an account without slots in the range
      if (!out.isEmpty()) {
        slots.add(out);
      }
      if (truncated || !origin.isZero()) {
        return StorageRangeMessage.create(
            slots, proof(state.storageTries.get(account), origin, out));
      }
      if (size >= bytes) {
        break;
      }
    }
    return StorageRangeMessage.create(slots, List.of());
  }

  private MessageData byteCodes(final GetByteCodesMessage message) {
    final List<Bytes> out = new ArrayList<>();
    final long bytes = Math.min(message.responseBytes(false).longValue(), maxResponseBytes);
    long size = 0;
    for (final Bytes32 hash : message.codeHashes(false)) {
      final Bytes code = codes.get(hash);
      if (code != null) {
        out.add(code);
        size += code.size();
        if (size >= bytes) {
          break;
        }
      }
    }
    return ByteCodesMessage.create(out);
  }

  // the proof of the origin, and of the last key served if any
  private static List<Bytes> proof(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie,
      final Bytes32 origin,
      final NavigableMap<Bytes32, Bytes> served) {
    return RangeProofs.proof(trie, origin, served.isEmpty() ? origin : served.lastKey());
  }

  private static final class ServedState {
    private final NavigableMap<Bytes32, Bytes> accounts;
    private final Map<Bytes32, NavigableMap<Bytes32, Bytes>> storage;
    private final SimpleMerklePatriciaTrie<Bytes32, Bytes> accountTrie =
        new SimpleMerklePatriciaTrie<>(Function.identity());
    private final Map<Bytes32, SimpleMerklePatriciaTrie<Bytes32, Bytes>> storageTries =
        new HashMap<>();

    private ServedState(final TestWorld world) {
      this.accounts = world.accountLeaves();
      this.storage = world.storageLeaves();
      accounts.forEach(accountTrie::put);
      storage.forEach(
          (account, slots) -> {
            final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie =
                new SimpleMerklePatriciaTrie<>(Function.identity());
            slots.forEach(trie::put);
            storageTries.put(account, trie);
          });
    }
  }
}
