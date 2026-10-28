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

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.vertx.core.WorkerExecutor;
import io.vertx.core.shareddata.Shareable;

/**
 * Base class for engine methods the Engine API spec requires to be processed serially, in the same
 * order as they were received (currently the {@code engine_forkchoiceUpdated} and {@code
 * engine_newPayload} series — see the respective V1 classes).
 *
 * <p>All instances share one single-threaded {@link WorkerExecutor} created from the engine
 * consensus API's existing Vertx instance (the same one backing {@code EngineQosTimer}), so calls
 * are serialized globally in arrival order regardless of which HTTP connection they arrive on,
 * without spinning up a dedicated Vertx instance just for ordering.
 */
public abstract class OrderedExecutionJsonRpcMethod extends ExecutionEngineJsonRpcMethod {

  // Must be <= the engine HTTP timeout so Thread A is released before the HTTP timer writes a
  // response. Uses the same default (30s) as JsonRpcConfiguration.DEFAULT_HTTP_TIMEOUT_SEC.
  private static final long ENGINE_API_RESPONSE_TIMEOUT_MS = 30_000L;
  private static final String ORDERED_EXECUTOR_NAME = "engine-ordered-execution";

  // every instance gets its own wrapper, but all of them share the same named single-threaded pool
  private final WorkerExecutor orderedExecutor;
  // shared like the pool, so that every ordered call runs the work queued before it
  private final PendingWork pendingWork;

  protected OrderedExecutionJsonRpcMethod(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
    this.orderedExecutor = syncVertx.createSharedWorkerExecutor(ORDERED_EXECUTOR_NAME, 1);
    this.pendingWork =
        syncVertx
            .sharedData()
            .<String, PendingWork>getLocalMap(ORDERED_EXECUTOR_NAME)
            .computeIfAbsent(ORDERED_EXECUTOR_NAME, name -> new PendingWork());
  }

  @Override
  public final JsonRpcResponse response(final JsonRpcRequestContext request) {
    final CompletableFuture<JsonRpcResponse> cf = new CompletableFuture<>();

    // ordered=false: the executor's single thread already serializes execution, and unordered
    // submission preserves global arrival order across HTTP connections, while ordered=true
    // would only preserve it per calling context.
    orderedExecutor
        .<JsonRpcResponse>executeBlocking(
            () -> {
              runPendingWork();
              return computeResponseSafely(request);
            },
            false)
        .onComplete(
            result -> {
              // computeResponseSafely never throws, so a failure here means the executor could not
              // run the task at all (e.g. rejected because it is closing during shutdown); complete
              // exceptionally so the ExecutionException branch below returns INTERNAL_ERROR instead
              // of a null response
              if (result.succeeded()) {
                cf.complete(result.result());
              } else {
                cf.completeExceptionally(result.cause());
              }
            });

    try {
      return cf.get(ENGINE_API_RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      logger()
          .debug(
              "Timeout waiting for engine API response for {}, releasing worker thread",
              this.getName());
      return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.TIMEOUT_ERROR);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      logger().error("Failed to get execution engine response", e);
      return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.TIMEOUT_ERROR);
    } catch (final ExecutionException e) {
      logger().error("Failed to get execution engine response", e);
      return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.INTERNAL_ERROR);
    }
  }

  /**
   * Queues work on the thread that ordered calls run on. Queued while a call runs, the work runs
   * after that call returns, and before the next ordered call, even one received while the call
   * ran.
   *
   * @param work the work to run
   */
  protected final void queueOnOrderedThread(final Runnable work) {
    pendingWork.add(work);
    // runs the work if no ordered call comes to run it first
    orderedExecutor
        .<Void>executeBlocking(
            () -> {
              runPendingWork();
              return null;
            },
            false)
        .onFailure(e -> logger().error("Work queued by {} failed", getName(), e));
  }

  private void runPendingWork() {
    Runnable work;
    while ((work = pendingWork.poll()) != null) {
      try {
        work.run();
      } catch (final RuntimeException e) {
        // the ordered call that runs it still has to be answered
        logger().error("Work queued on the ordered thread failed", e);
      }
    }
  }

  private static final class PendingWork extends ConcurrentLinkedQueue<Runnable>
      implements Shareable {}
}
