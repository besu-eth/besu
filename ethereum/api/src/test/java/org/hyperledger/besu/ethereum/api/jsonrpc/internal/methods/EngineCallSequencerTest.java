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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EngineCallSequencerTest {
  private static final JsonRpcResponse RESPONSE = new JsonRpcSuccessResponse(1);

  private Vertx vertx;
  private EngineCallSequencer sequencer;

  @BeforeEach
  void createSequencer() {
    vertx = Vertx.vertx();
    sequencer = EngineCallSequencer.of(vertx);
  }

  @AfterEach
  void closeVertx() {
    vertx.close().toCompletionStage().toCompletableFuture().join();
  }

  @Test
  void forkchoiceUpdatedRunsOnceAnEarlierNewPayloadStoredWhatItNeeds() throws Exception {
    final AtomicBoolean headStored = new AtomicBoolean();
    final CountDownLatch releaseFirstNewPayload = new CountDownLatch(1);
    final CountDownLatch secondNewPayloadStarted = new CountDownLatch(1);
    final CountDownLatch releaseSecondNewPayload = new CountDownLatch(1);

    final Future<JsonRpcResponse> first =
        sequencer.newPayload(
            () -> {
              await(releaseFirstNewPayload);
              sequencer.queueAfterReply(() -> headStored.set(true));
              return RESPONSE;
            });
    final Future<JsonRpcResponse> second =
        sequencer.newPayload(
            () -> {
              secondNewPayloadStarted.countDown();
              await(releaseSecondNewPayload);
              return RESPONSE;
            });
    final Future<JsonRpcResponse> forkchoiceUpdated =
        sequencer.forkchoiceUpdated(() -> !headStored.get(), () -> RESPONSE);
    try {
      sleep(200);
      assertThat(forkchoiceUpdated.isComplete()).isFalse();

      releaseFirstNewPayload.countDown();
      assertThat(result(first)).isSameAs(RESPONSE);
      assertThat(secondNewPayloadStarted.await(10, TimeUnit.SECONDS)).isTrue();

      assertThat(result(forkchoiceUpdated)).isSameAs(RESPONSE);
      assertThat(second.isComplete()).isFalse();
    } finally {
      releaseFirstNewPayload.countDown();
      releaseSecondNewPayload.countDown();
    }
    assertThat(result(second)).isSameAs(RESPONSE);
  }

  @Test
  void forkchoiceUpdatedWaitsForEveryEarlierNewPayloadWhileItNeedsThem() throws Exception {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    final CountDownLatch releaseNewPayloads = new CountDownLatch(1);

    for (final String name : List.of("first newPayload", "second newPayload")) {
      sequencer.newPayload(
          () -> {
            await(releaseNewPayloads);
            events.add(name);
            return RESPONSE;
          });
    }
    final Future<JsonRpcResponse> forkchoiceUpdated =
        sequencer.forkchoiceUpdated(
            () -> true,
            () -> {
              events.add("forkchoiceUpdated");
              return RESPONSE;
            });
    releaseNewPayloads.countDown();

    assertThat(result(forkchoiceUpdated)).isSameAs(RESPONSE);
    assertThat(events)
        .containsExactly("first newPayload", "second newPayload", "forkchoiceUpdated");
  }

  @Test
  void forkchoiceUpdatedDoesNotRunWhileANewPayloadStoresItsBlock() throws Exception {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    final CountDownLatch writeStarted = new CountDownLatch(1);
    final CountDownLatch releaseWrite = new CountDownLatch(1);

    final Future<JsonRpcResponse> newPayload =
        sequencer.newPayload(
            () -> {
              sequencer.queueAfterReply(
                  () -> {
                    writeStarted.countDown();
                    await(releaseWrite);
                    events.add("write");
                  });
              return RESPONSE;
            });
    final Future<JsonRpcResponse> forkchoiceUpdated;
    try {
      assertThat(result(newPayload)).isSameAs(RESPONSE);
      assertThat(writeStarted.await(10, TimeUnit.SECONDS)).isTrue();

      forkchoiceUpdated =
          sequencer.forkchoiceUpdated(
              () -> false,
              () -> {
                events.add("forkchoiceUpdated");
                return RESPONSE;
              });
      sleep(200);
      assertThat(forkchoiceUpdated.isComplete()).isFalse();
    } finally {
      releaseWrite.countDown();
    }

    assertThat(result(forkchoiceUpdated)).isSameAs(RESPONSE);
    assertThat(events).containsExactly("write", "forkchoiceUpdated");
  }

  @Test
  void newPayloadStoresItsBlockOnceAForkchoiceUpdatedThatRanBeforeItHasFinished() throws Exception {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    final CountDownLatch newPayloadStarted = new CountDownLatch(1);
    final CountDownLatch releaseNewPayload = new CountDownLatch(1);
    final CountDownLatch forkchoiceUpdatedStarted = new CountDownLatch(1);
    final CountDownLatch releaseForkchoiceUpdated = new CountDownLatch(1);
    final CountDownLatch written = new CountDownLatch(1);

    final Future<JsonRpcResponse> newPayload =
        sequencer.newPayload(
            () -> {
              newPayloadStarted.countDown();
              await(releaseNewPayload);
              sequencer.queueAfterReply(
                  () -> {
                    events.add("write");
                    written.countDown();
                  });
              return RESPONSE;
            });
    final Future<JsonRpcResponse> forkchoiceUpdated;
    try {
      assertThat(newPayloadStarted.await(10, TimeUnit.SECONDS)).isTrue();
      forkchoiceUpdated =
          sequencer.forkchoiceUpdated(
              () -> false,
              () -> {
                forkchoiceUpdatedStarted.countDown();
                await(releaseForkchoiceUpdated);
                events.add("forkchoiceUpdated");
                return RESPONSE;
              });
      assertThat(forkchoiceUpdatedStarted.await(10, TimeUnit.SECONDS)).isTrue();

      releaseNewPayload.countDown();
      // the reply does not wait for the forkchoiceUpdated call, only the write does
      assertThat(result(newPayload)).isSameAs(RESPONSE);
      sleep(200);
      assertThat(events).isEmpty();
    } finally {
      releaseNewPayload.countDown();
      releaseForkchoiceUpdated.countDown();
    }

    assertThat(result(forkchoiceUpdated)).isSameAs(RESPONSE);
    assertThat(written.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(events).containsExactly("forkchoiceUpdated", "write");
  }

  private static <T> T result(final Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
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
}
