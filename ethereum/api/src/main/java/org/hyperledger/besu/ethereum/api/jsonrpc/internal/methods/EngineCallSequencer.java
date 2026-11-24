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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.shareddata.Shareable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the {@code engine_forkchoiceUpdated} calls on one thread and the {@code engine_newPayload}
 * calls on another, each in the order received. A newPayload call waits for the forkchoiceUpdated
 * calls received before it. A forkchoiceUpdated call waits for the newPayload calls received before
 * it until it no longer needs them, which it is asked again each time one of them finishes.
 *
 * <p>A forkchoiceUpdated call and the work a newPayload call queued for after its reply, which
 * stores its block, never run at the same time.
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

  // a forkchoiceUpdated call that no longer waits for the newPayload calls received before it must
  // not move the chain head while one of them stores its block
  private final ReentrantLock chainUpdates = new ReentrantLock();

  // complete once the last forkchoiceUpdated call received so far, and so every earlier one,
  // finished
  private CompletableFuture<Void> forkchoiceUpdatesDone = CompletableFuture.completedFuture(null);

  private long newPayloadsReceived;

  private final Object newPayloadProgress = new Object();
  // the newPayload calls finish in the order received, so this many of the first ones have finished
  private long newPayloadsFinished;

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
   * @param needsEarlierNewPayloads whether the call still waits for the newPayload calls received
   *     before it, asked once the forkchoiceUpdated calls received before it have finished and
   *     again each time one of those newPayload calls finishes
   * @param call the call
   * @return the call's response
   */
  synchronized Future<JsonRpcResponse> forkchoiceUpdated(
      final BooleanSupplier needsEarlierNewPayloads, final Supplier<JsonRpcResponse> call) {
    final long earlierNewPayloads = newPayloadsReceived;
    final CompletableFuture<Void> done = new CompletableFuture<>();
    forkchoiceUpdatesDone = done;
    // ordered=false: the single thread already runs the calls in the order they were queued, while
    // ordered=true would order them per calling context only
    return forkchoiceUpdatedExecutor
        .<JsonRpcResponse>executeBlocking(
            () -> {
              try {
                awaitNewPayloads(earlierNewPayloads, needsEarlierNewPayloads);
                return withChainUpdates(call);
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
    final long position = ++newPayloadsReceived;
    final Promise<JsonRpcResponse> response = Promise.promise();
    newPayloadExecutor
        .<Void>executeBlocking(
            () -> {
              try {
                awaitForkchoiceUpdates(earlierForkchoiceUpdates);
                response.complete(call.get());
                // the work the call queued for after its reply runs before any call received
                // after it that waits for it
                runWorkAfterReply();
              } catch (final RuntimeException e) {
                response.tryFail(e);
                throw e;
              } finally {
                newPayloadFinished(position);
              }
              return null;
            },
            false)
        .onFailure(
            e -> {
              // a call the executor rejects must not hold up the calls received after it
              response.tryFail(e);
              newPayloadFinished(position);
            });
    return response.future();
  }

  /**
   * Queues work to run once the running newPayload call has replied, or, if no call runs, on the
   * newPayload thread.
   *
   * @param work the work
   */
  void queueAfterReply(final Runnable work) {
    workAfterReply.add(work);
    // runs the work if no newPayload call does it first
    newPayloadExecutor
        .<Void>executeBlocking(
            () -> {
              runWorkAfterReply();
              return null;
            },
            false)
        .onFailure(e -> LOG.warn("Work queued for after a newPayload reply could not run", e));
  }

  private void runWorkAfterReply() {
    if (workAfterReply.isEmpty()) {
      return;
    }
    withChainUpdates(
        () -> {
          Runnable work;
          while ((work = workAfterReply.poll()) != null) {
            try {
              work.run();
            } catch (final RuntimeException e) {
              // the calls received after it still have to run
              LOG.error("Work queued for after a newPayload reply failed", e);
            }
          }
          return null;
        });
  }

  private <T> T withChainUpdates(final Supplier<T> action) {
    chainUpdates.lock();
    try {
      return action.get();
    } finally {
      chainUpdates.unlock();
    }
  }

  private void newPayloadFinished(final long position) {
    synchronized (newPayloadProgress) {
      newPayloadsFinished = Math.max(newPayloadsFinished, position);
      newPayloadProgress.notifyAll();
    }
  }

  /**
   * Waits until the first {@code count} newPayload calls have finished, or until the
   * forkchoiceUpdated call no longer needs them. The wait is interruptible, as closing the
   * executors drops the queued calls without finishing them.
   */
  private void awaitNewPayloads(final long count, final BooleanSupplier needsEarlierNewPayloads) {
    try {
      long finished;
      synchronized (newPayloadProgress) {
        finished = newPayloadsFinished;
      }
      while (finished < count && needsEarlierNewPayloads.getAsBoolean()) {
        synchronized (newPayloadProgress) {
          while (newPayloadsFinished == finished) {
            newPayloadProgress.wait();
          }
          finished = newPayloadsFinished;
        }
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for earlier engine calls", e);
    }
  }

  /**
   * Waits for the forkchoiceUpdated calls received before a newPayload call. The wait is
   * interruptible, as closing the executors drops the queued calls without completing what they
   * would complete.
   */
  private static void awaitForkchoiceUpdates(final CompletableFuture<Void> earlierCalls) {
    try {
      earlierCalls.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for earlier engine calls", e);
    } catch (final ExecutionException e) {
      // completed normally only
      throw new IllegalStateException(e);
    }
  }
}
