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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.OrderedExecutionJsonRpcMethod.Series;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExecutionEngineJsonRpcMethodExecutionTest {
  private static final Vertx vertx = Vertx.vertx();

  @Mock private ProtocolContext protocolContext;

  @Mock private EngineCallListener engineCallListener;

  @Mock private ProtocolSchedule protocolSchedule;

  @Mock private MergeMiningCoordinator mergeCoordinator;

  @Mock private EthPeers ethPeers;

  @Mock private TransactionPool transactionPool;

  private final ExecutorService callers = Executors.newCachedThreadPool();

  @AfterEach
  public void stopCallers() {
    callers.shutdownNow();
  }

  @AfterAll
  public static void tearDown() {
    vertx.close().toCompletionStage().toCompletableFuture().join();
  }

  @Test
  public void unorderedMethodCallsExecuteConcurrently() throws Exception {
    // each call blocks until the other one has started executing, so the test only
    // completes if the two calls run in parallel
    final CyclicBarrier bothStarted = new CyclicBarrier(2);
    final UnorderedStubEngineMethod method =
        new UnorderedStubEngineMethod(
            protocolSchedule,
            protocolContext,
            engineCallListener,
            ethPeers,
            transactionPool,
            req -> {
              try {
                bothStarted.await(10, TimeUnit.SECONDS);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return new JsonRpcSuccessResponse(req.getRequest().getId());
            });

    final List<JsonRpcResponse> responses = callConcurrently(method, 2);

    assertThat(responses).allMatch(resp -> resp.getType() == RpcResponseType.SUCCESS);
  }

  @Test
  public void orderedMethodCallsNeverExecuteConcurrently() throws Exception {
    final AtomicInteger active = new AtomicInteger();
    final AtomicInteger maxActive = new AtomicInteger();
    final OrderedStubEngineMethod method =
        ordered(
            Series.NEW_PAYLOAD,
            req -> {
              maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
              sleep(50);
              active.decrementAndGet();
              return success(req);
            });

    final List<JsonRpcResponse> responses = callConcurrently(method, 4);

    assertThat(responses).allMatch(resp -> resp.getType() == RpcResponseType.SUCCESS);
    assertThat(maxActive.get()).isEqualTo(1);
  }

  @Test
  public void workQueuedByANewPayloadRunsBeforeTheNextNewPayload() throws Exception {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    final AtomicReference<OrderedStubEngineMethod> method = new AtomicReference<>();
    method.set(
        ordered(
            Series.NEW_PAYLOAD,
            req -> {
              if (events.isEmpty()) {
                method
                    .get()
                    .queue(
                        () -> {
                          sleep(200);
                          events.add("queued work");
                        });
              }
              events.add("call");
              return success(req);
            }));

    assertThat(call(method.get()).getType()).isEqualTo(RpcResponseType.SUCCESS);
    // the reply does not wait for the work it queued
    assertThat(events).containsExactly("call");

    assertThat(call(method.get()).getType()).isEqualTo(RpcResponseType.SUCCESS);
    assertThat(events).containsExactly("call", "queued work", "call");
  }

  @Test
  public void workQueuedByANewPayloadRunsBeforeAForkchoiceUpdatedReceivedWhileItRan()
      throws Exception {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    final CountDownLatch newPayloadStarted = new CountDownLatch(1);
    final CountDownLatch forkchoiceUpdatedReceived = new CountDownLatch(1);
    final AtomicReference<OrderedStubEngineMethod> newPayload = new AtomicReference<>();
    newPayload.set(
        ordered(
            Series.NEW_PAYLOAD,
            req -> {
              newPayloadStarted.countDown();
              await(forkchoiceUpdatedReceived);
              newPayload.get().queue(() -> events.add("queued work"));
              events.add("newPayload");
              return success(req);
            }));
    final OrderedStubEngineMethod forkchoiceUpdated =
        ordered(
            Series.FORKCHOICE_UPDATED,
            req -> {
              events.add("forkchoiceUpdated");
              return success(req);
            });

    final Future<JsonRpcResponse> newPayloadResponse = callers.submit(() -> call(newPayload.get()));
    assertThat(newPayloadStarted.await(10, TimeUnit.SECONDS)).isTrue();
    final Future<JsonRpcResponse> forkchoiceUpdatedResponse =
        callers.submit(() -> call(forkchoiceUpdated));
    // the forkchoiceUpdated call is queued by then
    sleep(200);
    forkchoiceUpdatedReceived.countDown();

    assertThat(newPayloadResponse.get(10, TimeUnit.SECONDS).getType())
        .isEqualTo(RpcResponseType.SUCCESS);
    assertThat(forkchoiceUpdatedResponse.get(10, TimeUnit.SECONDS).getType())
        .isEqualTo(RpcResponseType.SUCCESS);
    assertThat(events).containsExactly("newPayload", "queued work", "forkchoiceUpdated");
  }

  @Test
  public void forkchoiceUpdatedThatDoesNotNeedEarlierNewPayloadsRunsWhileOneRuns()
      throws Exception {
    final CountDownLatch newPayloadStarted = new CountDownLatch(1);
    final CountDownLatch releaseNewPayload = new CountDownLatch(1);
    final OrderedStubEngineMethod newPayload =
        ordered(
            Series.NEW_PAYLOAD,
            req -> {
              newPayloadStarted.countDown();
              await(releaseNewPayload);
              return success(req);
            });
    final OrderedStubEngineMethod forkchoiceUpdated =
        ordered(Series.FORKCHOICE_UPDATED, () -> false, this::success);

    final Future<JsonRpcResponse> newPayloadResponse = callers.submit(() -> call(newPayload));
    try {
      assertThat(newPayloadStarted.await(10, TimeUnit.SECONDS)).isTrue();

      assertThat(callers.submit(() -> call(forkchoiceUpdated)).get(10, TimeUnit.SECONDS).getType())
          .isEqualTo(RpcResponseType.SUCCESS);
      assertThat(newPayloadResponse).isNotDone();
    } finally {
      releaseNewPayload.countDown();
    }
    assertThat(newPayloadResponse.get(10, TimeUnit.SECONDS).getType())
        .isEqualTo(RpcResponseType.SUCCESS);
  }

  @Test
  public void forkchoiceUpdatedThatDoesNotNeedEarlierNewPayloadsKeepsItsOrder() throws Exception {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    final CountDownLatch newPayloadStarted = new CountDownLatch(1);
    final CountDownLatch firstForkchoiceUpdatedStarted = new CountDownLatch(1);
    final CountDownLatch releaseNewPayload = new CountDownLatch(1);
    final OrderedStubEngineMethod newPayload =
        ordered(
            Series.NEW_PAYLOAD,
            req -> {
              newPayloadStarted.countDown();
              await(releaseNewPayload);
              events.add("newPayload");
              return success(req);
            });
    final OrderedStubEngineMethod first =
        ordered(
            Series.FORKCHOICE_UPDATED,
            () -> {
              firstForkchoiceUpdatedStarted.countDown();
              return true;
            },
            req -> {
              events.add("first forkchoiceUpdated");
              return success(req);
            });
    final OrderedStubEngineMethod second =
        ordered(
            Series.FORKCHOICE_UPDATED,
            () -> false,
            req -> {
              events.add("second forkchoiceUpdated");
              return success(req);
            });

    final List<Future<JsonRpcResponse>> responses = new ArrayList<>();
    try {
      responses.add(callers.submit(() -> call(newPayload)));
      assertThat(newPayloadStarted.await(10, TimeUnit.SECONDS)).isTrue();
      responses.add(callers.submit(() -> call(first)));
      assertThat(firstForkchoiceUpdatedStarted.await(10, TimeUnit.SECONDS)).isTrue();
      responses.add(callers.submit(() -> call(second)));
      sleep(200);

      assertThat(events).isEmpty();
    } finally {
      releaseNewPayload.countDown();
    }
    for (final Future<JsonRpcResponse> response : responses) {
      assertThat(response.get(10, TimeUnit.SECONDS).getType()).isEqualTo(RpcResponseType.SUCCESS);
    }
    assertThat(events)
        .containsExactly("newPayload", "first forkchoiceUpdated", "second forkchoiceUpdated");
  }

  @Test
  public void newPayloadWaitsForTheForkchoiceUpdatedsReceivedBeforeIt() throws Exception {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    final CountDownLatch forkchoiceUpdatedStarted = new CountDownLatch(1);
    final CountDownLatch releaseForkchoiceUpdated = new CountDownLatch(1);
    final OrderedStubEngineMethod forkchoiceUpdated =
        ordered(
            Series.FORKCHOICE_UPDATED,
            () -> false,
            req -> {
              forkchoiceUpdatedStarted.countDown();
              await(releaseForkchoiceUpdated);
              events.add("forkchoiceUpdated");
              return success(req);
            });
    final OrderedStubEngineMethod newPayload =
        ordered(
            Series.NEW_PAYLOAD,
            req -> {
              events.add("newPayload");
              return success(req);
            });

    final Future<JsonRpcResponse> forkchoiceUpdatedResponse =
        callers.submit(() -> call(forkchoiceUpdated));
    final Future<JsonRpcResponse> newPayloadResponse;
    try {
      assertThat(forkchoiceUpdatedStarted.await(10, TimeUnit.SECONDS)).isTrue();
      newPayloadResponse = callers.submit(() -> call(newPayload));
      sleep(200);

      assertThat(newPayloadResponse).isNotDone();
    } finally {
      releaseForkchoiceUpdated.countDown();
    }
    assertThat(forkchoiceUpdatedResponse.get(10, TimeUnit.SECONDS).getType())
        .isEqualTo(RpcResponseType.SUCCESS);
    assertThat(newPayloadResponse.get(10, TimeUnit.SECONDS).getType())
        .isEqualTo(RpcResponseType.SUCCESS);
    assertThat(events).containsExactly("forkchoiceUpdated", "newPayload");
  }

  private JsonRpcResponse success(final JsonRpcRequestContext request) {
    return new JsonRpcSuccessResponse(request.getRequest().getId());
  }

  private OrderedStubEngineMethod ordered(
      final Series series, final Function<JsonRpcRequestContext, JsonRpcResponse> body) {
    return ordered(series, () -> true, body);
  }

  private OrderedStubEngineMethod ordered(
      final Series series,
      final BooleanSupplier needsEarlierNewPayloads,
      final Function<JsonRpcRequestContext, JsonRpcResponse> body) {
    return new OrderedStubEngineMethod(
        protocolSchedule,
        protocolContext,
        engineCallListener,
        mergeCoordinator,
        ethPeers,
        transactionPool,
        series,
        needsEarlierNewPayloads,
        body);
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await(10, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static JsonRpcResponse call(final JsonRpcMethod method) {
    return method.response(
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", method.getName(), new Object[0])));
  }

  private List<JsonRpcResponse> callConcurrently(final JsonRpcMethod method, final int callers)
      throws Exception {
    final ExecutorService pool = Executors.newFixedThreadPool(callers);
    try {
      final List<Future<JsonRpcResponse>> futures = new ArrayList<>();
      for (int i = 0; i < callers; i++) {
        futures.add(
            pool.submit(
                () ->
                    method.response(
                        new JsonRpcRequestContext(
                            new JsonRpcRequest("2.0", method.getName(), new Object[0])))));
      }
      final List<JsonRpcResponse> responses = new ArrayList<>(callers);
      for (final Future<JsonRpcResponse> future : futures) {
        responses.add(future.get(30, TimeUnit.SECONDS));
      }
      return responses;
    } finally {
      pool.shutdownNow();
    }
  }

  private static class UnorderedStubEngineMethod extends ExecutionEngineJsonRpcMethod {
    private final Function<JsonRpcRequestContext, JsonRpcResponse> body;

    UnorderedStubEngineMethod(
        final ProtocolSchedule protocolSchedule,
        final ProtocolContext protocolContext,
        final EngineCallListener engineCallListener,
        final EthPeers ethPeers,
        final TransactionPool transactionPool,
        final Function<JsonRpcRequestContext, JsonRpcResponse> body) {
      super(
          new ConstructorArgumentsBuilder()
              .protocolSchedule(protocolSchedule)
              .protocolContext(protocolContext)
              .vertx(vertx)
              .engineCallListener(engineCallListener)
              .ethPeers(ethPeers)
              .metricsSystem(new NoOpMetricsSystem())
              .transactionPool(transactionPool)
              .maxRequestBlocks(0)
              .build(),
          null,
          null);
      this.body = body;
    }

    @Override
    public String getName() {
      return "engine_stub_unordered";
    }

    @Override
    public JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
      return body.apply(request);
    }
  }

  private static class OrderedStubEngineMethod extends OrderedExecutionJsonRpcMethod {
    private final BooleanSupplier needsEarlierNewPayloads;
    private final Function<JsonRpcRequestContext, JsonRpcResponse> body;

    OrderedStubEngineMethod(
        final ProtocolSchedule protocolSchedule,
        final ProtocolContext protocolContext,
        final EngineCallListener engineCallListener,
        final MergeMiningCoordinator mergeCoordinator,
        final EthPeers ethPeers,
        final TransactionPool transactionPool,
        final Series series,
        final BooleanSupplier needsEarlierNewPayloads,
        final Function<JsonRpcRequestContext, JsonRpcResponse> body) {
      super(
          new ConstructorArgumentsBuilder()
              .protocolSchedule(protocolSchedule)
              .protocolContext(protocolContext)
              .vertx(vertx)
              .engineCallListener(engineCallListener)
              .mergeCoordinator(mergeCoordinator)
              .ethPeers(ethPeers)
              .metricsSystem(new NoOpMetricsSystem())
              .transactionPool(transactionPool)
              .maxRequestBlocks(0)
              .build(),
          null,
          null,
          series);
      this.needsEarlierNewPayloads = needsEarlierNewPayloads;
      this.body = body;
    }

    @Override
    public String getName() {
      return "engine_stub_ordered";
    }

    @Override
    protected boolean needsEarlierNewPayloads(final JsonRpcRequestContext request) {
      return needsEarlierNewPayloads.getAsBoolean();
    }

    void queue(final Runnable work) {
      queueAfterReply(work);
    }

    @Override
    public JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
      return body.apply(request);
    }
  }
}
