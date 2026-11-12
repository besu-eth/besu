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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.shareddata.Shareable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the {@code engine_forkchoiceUpdated} calls on one thread and the {@code engine_newPayload}
 * calls on another, each in the order received. A call waits for the calls of the other method
 * received before it, except a forkchoiceUpdated call that does not need them.
 *
 * <p>One instance is shared by all the ordered engine methods of a Vertx instance.
 */
final class EngineCallSequencer implements Shareable {

  private static final Logger LOG = LoggerFactory.getLogger(EngineCallSequencer.class);

  private static final String NAME = "engine-call-sequencer";
  private static final String FORKCHOICE_UPDATED_EXECUTOR_NAME = "engine-forkchoice-updated";
  private static final String NEW_PAYLOAD_EXECUTOR_NAME = "engine-new-payload";

  private final WorkerExecutor forkchoiceUpdatedExecutor;
  private final WorkerExecutor newPayloadExecutor;
  private final Queue<Runnable> workAfterReply = new ConcurrentLinkedQueue<>();

  // complete once the last call of the method received so far, and so every earlier one, finished
  private CompletableFuture<Void> forkchoiceUpdatesDone = CompletableFuture.completedFuture(null);
  private CompletableFuture<Void> newPayloadsDone = CompletableFuture.completedFuture(null);

  private EngineCallSequencer(final Vertx vertx) {
    this.forkchoiceUpdatedExecutor =
        vertx.createSharedWorkerExecutor(FORKCHOICE_UPDATED_EXECUTOR_NAME, 1);
    this.newPayloadExecutor = vertx.createSharedWorkerExecutor(NEW_PAYLOAD_EXECUTOR_NAME, 1);
  }

  /**
   * Returns the sequencer of the given Vertx instance.
   *
   * @param vertx the engine consensus API's Vertx instance
   * @return the sequencer shared by the ordered engine methods of that instance
   */
  static EngineCallSequencer of(final Vertx vertx) {
    return vertx
        .sharedData()
        .<String, EngineCallSequencer>getLocalMap(NAME)
        .computeIfAbsent(NAME, name -> new EngineCallSequencer(vertx));
  }

  /**
   * Queues a forkchoiceUpdated call.
   *
   * @param needsEarlierNewPayloads whether the call waits for the newPayload calls received before
   *     it, asked once the forkchoiceUpdated calls received before it have finished
   * @param call the call
   * @return the call's response
   */
  synchronized Future<JsonRpcResponse> forkchoiceUpdated(
      final BooleanSupplier needsEarlierNewPayloads, final Supplier<JsonRpcResponse> call) {
    final CompletableFuture<Void> earlierNewPayloads = newPayloadsDone;
    final CompletableFuture<Void> done = new CompletableFuture<>();
    forkchoiceUpdatesDone = done;
    // ordered=false: the single thread already runs the calls in the order they were queued, while
    // ordered=true would order them per calling context only
    return forkchoiceUpdatedExecutor
        .<JsonRpcResponse>executeBlocking(
            () -> {
              try {
                if (needsEarlierNewPayloads.getAsBoolean()) {
                  earlierNewPayloads.join();
                }
                return call.get();
              } finally {
                done.complete(null);
              }
            },
            false)
        // a call the executor rejects must not hold up the calls received after it
        .onFailure(e -> done.complete(null));
  }

  /**
   * Queues a newPayload call, to run once the forkchoiceUpdated calls received before it have
   * finished.
   *
   * @param call the call
   * @return the call's response
   */
  synchronized Future<JsonRpcResponse> newPayload(final Supplier<JsonRpcResponse> call) {
    final CompletableFuture<Void> earlierForkchoiceUpdates = forkchoiceUpdatesDone;
    final CompletableFuture<Void> done = new CompletableFuture<>();
    newPayloadsDone = done;
    final Future<JsonRpcResponse> response =
        newPayloadExecutor.executeBlocking(
            () -> {
              earlierForkchoiceUpdates.join();
              return call.get();
            },
            false);
    // queued right behind the call, so that the work it queued for after its reply runs before any
    // call received after it that waits for it
    newPayloadExecutor
        .<Void>executeBlocking(
            () -> {
              try {
                runWorkAfterReply();
              } finally {
                done.complete(null);
              }
              return null;
            },
            false)
        .onFailure(e -> done.complete(null));
    return response;
  }

  /**
   * Queues work to run once the running newPayload call has replied.
   *
   * @param work the work
   */
  void queueAfterReply(final Runnable work) {
    workAfterReply.add(work);
  }

  private void runWorkAfterReply() {
    Runnable work;
    while ((work = workAfterReply.poll()) != null) {
      try {
        work.run();
      } catch (final RuntimeException e) {
        // the calls received after it still have to run
        LOG.error("Work queued for after a newPayload reply failed", e);
      }
    }
  }
}
