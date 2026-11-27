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
package org.hyperledger.besu.ethereum.api.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class JsonRpcParserHandlerTest {

  private static final String SINGLE_REQUEST =
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_chainId\",\"params\":[]}";
  private static final String BATCH_REQUEST =
      "[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_chainId\",\"params\":[]},"
          + "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"eth_chainId\",\"params\":[]}]";
  private static final int MAX_BUFFERED_BYTES = 10 * 1024 * 1024;

  @Test
  void parsesRequestSplitAcrossReads() {
    final Recorder recorder = new Recorder();

    recorder.handler.handle(Buffer.buffer(SINGLE_REQUEST.substring(0, 20)));
    assertThat(recorder.objects).isEmpty();
    assertThat(recorder.errors.get()).isEqualTo(0);

    recorder.handler.handle(Buffer.buffer(SINGLE_REQUEST.substring(20)));

    assertThat(recorder.objects).hasSize(1);
    assertThat(recorder.objects.get(0).getString("method")).isEqualTo("eth_chainId");
    assertThat(recorder.errors.get()).isEqualTo(0);
  }

  @Test
  void parsesBatchSplitAcrossReads() {
    final Recorder recorder = new Recorder();

    recorder.handler.handle(Buffer.buffer(BATCH_REQUEST.substring(0, 60)));
    assertThat(recorder.arrays).isEmpty();
    assertThat(recorder.errors.get()).isEqualTo(0);

    recorder.handler.handle(Buffer.buffer(BATCH_REQUEST.substring(60)));

    assertThat(recorder.arrays).hasSize(1);
    assertThat(recorder.arrays.get(0)).hasSize(2);
    assertThat(recorder.errors.get()).isEqualTo(0);
  }

  @Test
  void parsesRequestSplitAtEveryByteBoundary() {
    for (int splitAt = 1; splitAt < SINGLE_REQUEST.length(); splitAt++) {
      final Recorder recorder = new Recorder();

      recorder.handler.handle(Buffer.buffer(SINGLE_REQUEST.substring(0, splitAt)));
      recorder.handler.handle(Buffer.buffer(SINGLE_REQUEST.substring(splitAt)));

      assertThat(recorder.objects).as("split at byte %s", splitAt).hasSize(1);
      assertThat(recorder.errors.get()).as("split at byte %s", splitAt).isEqualTo(0);
    }
  }

  @Test
  void parsesConsecutiveDocumentsSplitAcrossReads() {
    final Recorder recorder = new Recorder();
    final String firstRequest = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_chainId\"}";
    final String secondRequest = "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"net_version\"}";
    final String stream = firstRequest + secondRequest;

    // Split inside the second document so the first one is already complete.
    final int splitAt = firstRequest.length() + 10;
    recorder.handler.handle(Buffer.buffer(stream.substring(0, splitAt)));
    assertThat(recorder.objects).hasSize(1);

    recorder.handler.handle(Buffer.buffer(stream.substring(splitAt)));

    assertThat(recorder.objects).hasSize(2);
    assertThat(recorder.objects.get(1).getString("method")).isEqualTo("net_version");
    assertThat(recorder.errors.get()).isEqualTo(0);
  }

  @Test
  void parsesUtf8RequestSplitInsideCharacter() {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_call\",\"params\":[\"测试\"]}";
    final byte[] bytes = request.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    final int splitAt = request.indexOf("测") + 1;

    final Recorder recorder = new Recorder();
    recorder.handler.handle(Buffer.buffer(Arrays.copyOfRange(bytes, 0, splitAt)));
    recorder.handler.handle(Buffer.buffer(Arrays.copyOfRange(bytes, splitAt, bytes.length)));

    assertThat(recorder.objects).hasSize(1);
    assertThat(recorder.objects.get(0).getJsonArray("params").getString(0)).isEqualTo("测试");
    assertThat(recorder.errors.get()).isEqualTo(0);
  }

  @Test
  void reportsParseErrorForNonJsonInput() {
    final Recorder recorder = new Recorder();

    recorder.handler.handle(Buffer.buffer("bad request"));

    assertThat(recorder.objects).isEmpty();
    assertThat(recorder.arrays).isEmpty();
    assertThat(recorder.errors.get()).isEqualTo(1);
  }

  @Test
  void reportsParseErrorForMalformedJson() {
    final Recorder recorder = new Recorder();

    recorder.handler.handle(Buffer.buffer("{\"id\":1]"));

    assertThat(recorder.errors.get()).isEqualTo(1);
  }

  @Test
  void recoversAfterParseError() {
    final Recorder recorder = new Recorder();

    recorder.handler.handle(Buffer.buffer("bad request"));
    recorder.handler.handle(Buffer.buffer(SINGLE_REQUEST));

    assertThat(recorder.errors.get()).isEqualTo(1);
    assertThat(recorder.objects).hasSize(1);
  }

  @Test
  void reportsErrorForEmptyRead() {
    final Recorder recorder = new Recorder();

    recorder.handler.handle(Buffer.buffer());

    assertThat(recorder.errors.get()).isEqualTo(1);
  }

  @Test
  void reportsErrorWhenIncompleteMessageExceedsBufferLimit() {
    final byte[] oversized = new byte[MAX_BUFFERED_BYTES + 1];
    Arrays.fill(oversized, (byte) '{');

    final Recorder recorder = new Recorder();
    recorder.handler.handle(Buffer.buffer(oversized));

    assertThat(recorder.objects).isEmpty();
    assertThat(recorder.errors.get()).isEqualTo(1);
  }

  private static final class Recorder {
    private final List<JsonObject> objects = new ArrayList<>();
    private final List<JsonArray> arrays = new ArrayList<>();
    private final AtomicInteger errors = new AtomicInteger();
    private final Handler<Buffer> handler =
        JsonRpcParserHandler.ipcHandler(
            (jsonObject, jsonArray) -> {
              if (jsonObject != null) {
                objects.add(jsonObject);
              }
              if (jsonArray != null) {
                arrays.add(jsonArray);
              }
            },
            errors::incrementAndGet);
  }
}
