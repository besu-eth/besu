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
package org.hyperledger.besu.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.StreamReadConstraints;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link BesuCommand#configureVertxJsonDecodingConstraints(long, long)} bounds the
 * shared Vert.x JSON decoder so a single request cannot be expanded into an arbitrarily large
 * in-memory node tree. The decoder configured here ({@link DatabindCodec#mapper()}) is the same one
 * that backs {@code ctx.body().asJsonObject()} / {@code asJsonArray()} in the JSON-RPC parser
 * handler, so these tests exercise the real decode path.
 *
 * <p>Every request reaching the decoder has already been bounded in bytes by the HTTP body limit,
 * so the payloads that matter are the ones that fit under it. The tests therefore keep the bodies
 * well below the configured byte limit and show that only the token count bounds their cost.
 */
public class VertxJsonDecodingConfigurationTest {

  private static final long BODY_LIMIT = 1024 * 1024;
  private static final long TOKEN_LIMIT = 1_000;

  private StreamReadConstraints originalConstraints;

  @BeforeEach
  public void captureOriginalConstraints() {
    originalConstraints = DatabindCodec.mapper().getFactory().streamReadConstraints();
  }

  @AfterEach
  public void restoreOriginalConstraints() {
    // The decoder is a global singleton; leave it exactly as we found it for other tests.
    DatabindCodec.mapper().getFactory().setStreamReadConstraints(originalConstraints);
  }

  @Test
  public void setsTokenAndStringLimits() {
    BesuCommand.configureVertxJsonDecodingConstraints(BODY_LIMIT, TOKEN_LIMIT);

    final StreamReadConstraints configured =
        DatabindCodec.mapper().getFactory().streamReadConstraints();
    assertThat(configured.getMaxTokenCount()).isEqualTo(TOKEN_LIMIT);
    assertThat(configured.getMaxStringLength()).isEqualTo(BODY_LIMIT);
  }

  @Test
  public void nonPositiveBodyLimitDoesNotLimitStrings() {
    // A non-positive body limit means no limit, so it must not reach maxStringLength, which
    // rejects negative values and would fail startup.
    BesuCommand.configureVertxJsonDecodingConstraints(-1, TOKEN_LIMIT);

    assertThat(DatabindCodec.mapper().getFactory().streamReadConstraints().getMaxStringLength())
        .isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void tokenDenseBodyUnderTheByteLimitIsRejected() {
    BesuCommand.configureVertxJsonDecodingConstraints(BODY_LIMIT, TOKEN_LIMIT);

    // A flat array of empty objects: no long string and no deep nesting, and far below the byte
    // limit, so it would clear BodyHandler. Each element is two tokens, so it is over the token
    // limit and the parser must abort before building the tree.
    final Buffer dense = Buffer.buffer(denseJsonArray(10 * (int) TOKEN_LIMIT));
    assertThat((long) dense.length()).isLessThan(BODY_LIMIT);

    assertThatThrownBy(() -> new JsonArray(dense)).isInstanceOf(DecodeException.class);
  }

  @Test
  public void tokenDenseBodyUnderTheByteLimitDecodesWithoutTheTokenLimit() {
    // Control for the test above: with the token limit disabled the same body decodes in full,
    // so the byte-based limits alone do not bound its cost and the token limit is what rejects it.
    BesuCommand.configureVertxJsonDecodingConstraints(BODY_LIMIT, 0);

    final int elements = 10 * (int) TOKEN_LIMIT;
    final Buffer dense = Buffer.buffer(denseJsonArray(elements));
    assertThat((long) dense.length()).isLessThan(BODY_LIMIT);

    assertThat(new JsonArray(dense).size()).isEqualTo(elements);
  }

  @Test
  public void largeTokenSparsePayloadStillDecodes() {
    BesuCommand.configureVertxJsonDecodingConstraints(BODY_LIMIT, TOKEN_LIMIT);

    // Shaped like an engine payload: large in bytes but with few tokens, since transactions are
    // single hex strings. The token limit must not get in its way.
    final String transaction = "0x" + "ab".repeat(64 * 1024);
    final StringBuilder sb = new StringBuilder("{\"transactions\":[");
    for (int i = 0; i < 4; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('"').append(transaction).append('"');
    }
    final Buffer sparse = Buffer.buffer(sb.append("]}").toString());
    assertThat((long) sparse.length()).isGreaterThan(TOKEN_LIMIT * 100);

    assertThat(new JsonObject(sparse).getJsonArray("transactions").size()).isEqualTo(4);
  }

  @Test
  public void bodyWithinTheTokenLimitStillDecodes() {
    BesuCommand.configureVertxJsonDecodingConstraints(BODY_LIMIT, TOKEN_LIMIT);

    final Buffer small = Buffer.buffer(denseJsonArray(10));

    assertThat(new JsonArray(small).size()).isEqualTo(10);
  }

  private static String denseJsonArray(final int elements) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < elements; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{}");
    }
    return sb.append(']').toString();
  }
}
