/*
 * Copyright contributors to Hyperledger Besu.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Verifies the concurrency/ordering guarantee that {@link OrderedExecutionJsonRpcMethod} provides
 * via its internal single-threaded {@code WorkerExecutor}: at most one engine_* call body can ever
 * be executing at once, whether calls land on the same method instance or two different ones
 * sharing the same Vertx instance. This covers {@code engine_forkchoiceUpdated} and {@code
 * engine_newPayload}, which the Engine API spec requires to be processed serially in arrival order.
 */
class ExecutionEngineConcurrencySafetyTest {

  // A shared real Vertx instance; the serialization guarantee comes from
  // OrderedExecutionJsonRpcMethod's
  // internal createSharedWorkerExecutor("engine-ordered-execution", 1), not from the Vertx pool
  // size.
  private static final Vertx vertx = Vertx.vertx(new VertxOptions());

  @AfterAll
  static void closeVertx() {
    vertx.close();
  }

  /** Minimal concrete ordered engine method whose body records concurrency and completion order. */
  private static final class RecordingEngineMethod extends OrderedExecutionJsonRpcMethod {
    private final String name;
    private final long workMillis;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxObservedConcurrency = new AtomicInteger();
    private final List<String> completionOrder = new CopyOnWriteArrayList<>();

    RecordingEngineMethod(
        final Vertx vertx,
        final ProtocolContext protocolContext,
        final String name,
        final long workMillis) {
      super(
          new ExecutionEngineJsonRpcMethod.ConstructorArguments(
              null,
              protocolContext,
              vertx,
              mock(EngineCallListener.class),
              null,
              null,
              null,
              null,
              0),
          null,
          null);
      this.name = name;
      this.workMillis = workMillis;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
      final int concurrentNow = inFlight.incrementAndGet();
      maxObservedConcurrency.updateAndGet(prev -> Math.max(prev, concurrentNow));
      try {
        Thread.sleep(workMillis);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        inFlight.decrementAndGet();
      }
      final String callId = name + ":" + request.getRequest().getParams()[0];
      completionOrder.add(callId);
      return new JsonRpcSuccessResponse(request.getRequest().getId(), callId);
    }
  }

  private static ProtocolContext mockProtocolContext() {
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.safeConsensusContext(any())).thenReturn(Optional.empty());
    return protocolContext;
  }

  private static JsonRpcRequestContext request(final String method, final int paramId) {
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", method, new Object[] {paramId}));
  }

  /**
   * Fires many concurrent calls to a single engine method instance, simulating concurrent engine_*
   * HTTP requests dispatched by the outer (deliberately unordered) router blockingHandler in
   * EngineJsonRpcService. Records the maximum number of syncResponse() invocations ever in-flight
   * at once, and the order in which calls complete.
   */
  @Test
  @Timeout(30)
  void singleMethodConcurrentCallsObservedConcurrency() throws Exception {
    final int callers = 20;
    final RecordingEngineMethod method =
        new RecordingEngineMethod(vertx, mockProtocolContext(), "engine_test", 50);

    final List<String> completionOrder =
        fireConcurrently(callers, id -> method.response(request("engine_test", id)));

    assertThat(method.maxObservedConcurrency.get())
        .as(
            "engine_test calls dispatched concurrently to a single method instance must still be "
                + "serialized by OrderedExecutionJsonRpcMethod's single-thread WorkerExecutor; "
                + "completion order was: %s",
            completionOrder)
        .isEqualTo(1);
  }

  /**
   * Same as above, but across two different engine method instances sharing the same Vertx instance
   * -- mirrors, e.g., engine_newPayload and engine_forkchoiceUpdated being called concurrently for
   * the same client, which in production must not interleave with each other.
   */
  @Test
  @Timeout(30)
  void crossMethodConcurrentCallsObservedConcurrency() throws Exception {
    final int callersPerMethod = 10;
    final RecordingEngineMethod methodA =
        new RecordingEngineMethod(vertx, mockProtocolContext(), "engine_newPayload", 50);
    final RecordingEngineMethod methodB =
        new RecordingEngineMethod(vertx, mockProtocolContext(), "engine_forkchoiceUpdated", 50);

    final ExecutorService pool = Executors.newFixedThreadPool(callersPerMethod * 2);
    final CountDownLatch ready = new CountDownLatch(callersPerMethod * 2);
    final CountDownLatch go = new CountDownLatch(1);
    final AtomicInteger crossMethodInFlight = new AtomicInteger();
    final AtomicInteger maxCrossMethodConcurrency = new AtomicInteger();

    final List<Future<?>> futures = new java.util.ArrayList<>();
    for (int i = 0; i < callersPerMethod; i++) {
      final int id = i;
      futures.add(
          pool.submit(
              () -> {
                ready.countDown();
                await(go);
                final int now = crossMethodInFlight.incrementAndGet();
                maxCrossMethodConcurrency.updateAndGet(prev -> Math.max(prev, now));
                try {
                  return methodA.response(request("engine_newPayload", id));
                } finally {
                  crossMethodInFlight.decrementAndGet();
                }
              }));
      futures.add(
          pool.submit(
              () -> {
                ready.countDown();
                await(go);
                final int now = crossMethodInFlight.incrementAndGet();
                maxCrossMethodConcurrency.updateAndGet(prev -> Math.max(prev, now));
                try {
                  return methodB.response(request("engine_forkchoiceUpdated", id));
                } finally {
                  crossMethodInFlight.decrementAndGet();
                }
              }));
    }
    ready.await();
    go.countDown();
    for (final Future<?> f : futures) {
      f.get(20, TimeUnit.SECONDS);
    }
    pool.shutdown();

    final int maxA = methodA.maxObservedConcurrency.get();
    final int maxB = methodB.maxObservedConcurrency.get();

    assertThat(maxA)
        .as(
            "engine_newPayload body must never overlap with itself or engine_forkchoiceUpdated "
                + "(callers dispatched at once: %s)",
            maxCrossMethodConcurrency.get())
        .isEqualTo(1);
    assertThat(maxB)
        .as(
            "engine_forkchoiceUpdated body must never overlap with itself or engine_newPayload "
                + "(callers dispatched at once: %s)",
            maxCrossMethodConcurrency.get())
        .isEqualTo(1);
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static List<String> fireConcurrently(
      final int callers, final java.util.function.IntFunction<JsonRpcResponse> call)
      throws Exception {
    final ExecutorService pool = Executors.newFixedThreadPool(callers);
    final CountDownLatch ready = new CountDownLatch(callers);
    final CountDownLatch go = new CountDownLatch(1);
    final List<Future<JsonRpcResponse>> futures =
        IntStream.range(0, callers)
            .mapToObj(
                id ->
                    pool.submit(
                        () -> {
                          ready.countDown();
                          await(go);
                          return call.apply(id);
                        }))
            .collect(Collectors.toList());
    ready.await();
    go.countDown();
    final List<String> results = new java.util.ArrayList<>();
    for (final Future<JsonRpcResponse> f : futures) {
      final JsonRpcSuccessResponse resp = (JsonRpcSuccessResponse) f.get(20, TimeUnit.SECONDS);
      results.add(String.valueOf(resp.getResult()));
    }
    pool.shutdown();
    return results;
  }
}
