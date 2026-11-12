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

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import io.vertx.core.Future;

/**
 * Base class for the engine methods whose calls are processed serially, in the order they were
 * received: the {@code engine_forkchoiceUpdated} and {@code engine_newPayload} series (see the
 * respective V1 classes).
 *
 * <p>Each series runs on a single thread of its own, created from the engine consensus API's
 * existing Vertx instance (the same one backing {@code EngineQosTimer}), so calls are serialized
 * globally in arrival order regardless of which HTTP connection they arrive on. A call also waits
 * for the calls of the other series received before it, unless it is a forkchoiceUpdated call that
 * does not need them (see {@link #needsEarlierNewPayloads}).
 */
public abstract class OrderedExecutionJsonRpcMethod extends ExecutionEngineJsonRpcMethod {

  /** A method series whose calls run on a thread of their own. */
  protected enum Series {
    /** The {@code engine_forkchoiceUpdated} calls. */
    FORKCHOICE_UPDATED,
    /** The {@code engine_newPayload} calls. */
    NEW_PAYLOAD
  }

  // Must be <= the engine HTTP timeout so Thread A is released before the HTTP timer writes a
  // response. Uses the same default (30s) as JsonRpcConfiguration.DEFAULT_HTTP_TIMEOUT_SEC.
  private static final long ENGINE_API_RESPONSE_TIMEOUT_MS = 30_000L;

  private final Series series;
  private final EngineCallSequencer sequencer;

  protected OrderedExecutionJsonRpcMethod(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork,
      final Series series) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
    this.series = series;
    this.sequencer = EngineCallSequencer.of(syncVertx);
  }

  @Override
  public final JsonRpcResponse response(final JsonRpcRequestContext request) {
    final CompletableFuture<JsonRpcResponse> cf = new CompletableFuture<>();
    final Supplier<JsonRpcResponse> call = () -> computeResponseSafely(request);
    final Future<JsonRpcResponse> response =
        switch (series) {
          case FORKCHOICE_UPDATED ->
              sequencer.forkchoiceUpdated(() -> needsEarlierNewPayloads(request), call);
          case NEW_PAYLOAD -> sequencer.newPayload(call);
        };
    response.onComplete(
        result -> {
          // computeResponseSafely never throws, so a failure here means the executor could not run
          // the task at all (e.g. rejected because it is closing during shutdown); complete
          // exceptionally so the ExecutionException branch below returns INTERNAL_ERROR instead of
          // a null response
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
   * Returns whether a forkchoiceUpdated call waits for the newPayload calls received before it.
   * Asked on the forkchoiceUpdated thread, once the forkchoiceUpdated calls received before it have
   * finished.
   *
   * @param request the forkchoiceUpdated request
   * @return whether the call waits for the newPayload calls received before it
   */
  protected boolean needsEarlierNewPayloads(final JsonRpcRequestContext request) {
    return true;
  }

  /**
   * Queues work to run once this newPayload call has replied, before the next newPayload call and
   * before any forkchoiceUpdated call that waits for this one. Called while the call runs.
   *
   * @param work the work to run
   */
  protected final void queueAfterReply(final Runnable work) {
    checkState(series == Series.NEW_PAYLOAD, "Only newPayload calls queue work after their reply");
    sequencer.queueAfterReply(work);
  }
}
