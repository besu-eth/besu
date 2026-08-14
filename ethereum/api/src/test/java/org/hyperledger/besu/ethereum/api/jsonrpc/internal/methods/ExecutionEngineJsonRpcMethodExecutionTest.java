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

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExecutionEngineJsonRpcMethodExecutionTest {
  private static final Vertx vertx = Vertx.vertx();

  @Mock private ProtocolContext protocolContext;

  @Mock private EngineCallListener engineCallListener;

  @AfterAll
  public static void tearDown() {
    vertx.close();
  }

  @Test
  public void unorderedMethodCallsExecuteConcurrently() throws Exception {
    // each call blocks until the other one has started executing, so the test only
    // completes if the two calls run in parallel
    final CyclicBarrier bothStarted = new CyclicBarrier(2);
    final StubEngineMethod method =
        new StubEngineMethod(
            protocolContext,
            engineCallListener,
            false,
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
    final StubEngineMethod method =
        new StubEngineMethod(
            protocolContext,
            engineCallListener,
            true,
            req -> {
              maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              active.decrementAndGet();
              return new JsonRpcSuccessResponse(req.getRequest().getId());
            });

    final List<JsonRpcResponse> responses = callConcurrently(method, 4);

    assertThat(responses).allMatch(resp -> resp.getType() == RpcResponseType.SUCCESS);
    assertThat(maxActive.get()).isEqualTo(1);
  }

  private List<JsonRpcResponse> callConcurrently(final StubEngineMethod method, final int callers)
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

  private static class StubEngineMethod extends ExecutionEngineJsonRpcMethod {
    private final boolean ordered;
    private final Function<JsonRpcRequestContext, JsonRpcResponse> body;

    StubEngineMethod(
        final ProtocolContext protocolContext,
        final EngineCallListener engineCallListener,
        final boolean ordered,
        final Function<JsonRpcRequestContext, JsonRpcResponse> body) {
      super(vertx, protocolContext, engineCallListener);
      this.ordered = ordered;
      this.body = body;
    }

    @Override
    public String getName() {
      return "engine_stub";
    }

    @Override
    protected boolean requiresOrderedExecution() {
      return ordered;
    }

    @Override
    public JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
      return body.apply(request);
    }
  }
}
