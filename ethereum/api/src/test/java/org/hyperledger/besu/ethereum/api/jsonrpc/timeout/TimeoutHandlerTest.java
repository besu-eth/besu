/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.timeout;

import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod.ETH_BLOCK_NUMBER;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod.ETH_GET_LOGS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.handlers.TimeoutHandler;
import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.context.ContextKey;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TimeoutHandlerTest {

  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {Optional.empty(), ETH_GET_LOGS, DEFAULT_OPTS.getTimeoutSeconds(), true},
          {Optional.empty(), ETH_BLOCK_NUMBER, DEFAULT_OPTS.getTimeoutSeconds(), false},
          {Optional.of(DEFAULT_OPTS), ETH_BLOCK_NUMBER, DEFAULT_OPTS.getTimeoutSeconds(), true}
        });
  }

  private static final TimeoutOptions DEFAULT_OPTS = TimeoutOptions.defaultOptions();

  @ParameterizedTest
  @MethodSource("data")
  public void test(
      final Optional<TimeoutOptions> globalOptions,
      final RpcMethod method,
      final long timeoutSec,
      final boolean timerMustBeSet) {
    final Map<String, TimeoutOptions> options;
    if (timerMustBeSet) {
      options =
          ImmutableMap.of(
              method.getMethodName(), new TimeoutOptions(timeoutSec, DEFAULT_OPTS.getErrorCode()));
    } else {
      options = Collections.emptyMap();
    }
    final Handler<RoutingContext> handler = TimeoutHandler.handler(globalOptions, options);
    final RoutingContext ctx = Mockito.spy(RoutingContext.class);
    final Vertx vertx = Mockito.spy(Vertx.class);

    final JsonObject requestBody = Mockito.mock(JsonObject.class);
    when(requestBody.getString("method")).thenReturn(method.getMethodName());
    when(ctx.data()).thenReturn(Map.of(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name(), requestBody));
    when(ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name())).thenReturn(requestBody);

    when(ctx.vertx()).thenReturn(vertx);
    handler.handle(ctx);
    verify(vertx, times(timerMustBeSet ? 1 : 0))
        .setTimer(eq(TimeUnit.SECONDS.toMillis(timeoutSec)), any());
    verify(ctx, times(timerMustBeSet ? 1 : 0)).addBodyEndHandler(any());
  }

  // --- Timer callback behavior tests ---

  /**
   * Creates a RoutingContext with a timer that captures the timer callback, so tests can invoke it
   * directly.
   */
  @SuppressWarnings("unchecked")
  private static TimerSetup setupTimerCapture() {
    final RoutingContext ctx = mock(RoutingContext.class);
    final Vertx vertx = mock(Vertx.class);
    final HttpServerResponse response = mock(HttpServerResponse.class);
    final HttpServerRequest request = mock(HttpServerRequest.class);
    final HttpConnection connection = mock(HttpConnection.class);

    final JsonObject requestBody = mock(JsonObject.class);
    when(requestBody.getString("method")).thenReturn(ETH_GET_LOGS.getMethodName());
    when(ctx.data()).thenReturn(Map.of(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name(), requestBody));
    when(ctx.get(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name())).thenReturn(requestBody);
    when(ctx.vertx()).thenReturn(vertx);
    when(ctx.response()).thenReturn(response);
    when(ctx.request()).thenReturn(request);
    when(request.connection()).thenReturn(connection);

    final ArgumentCaptor<Handler<Long>> timerCaptor = ArgumentCaptor.forClass(Handler.class);
    when(vertx.setTimer(anyLong(), timerCaptor.capture())).thenReturn(1L);

    final Map<String, TimeoutOptions> options =
        ImmutableMap.of(
            ETH_GET_LOGS.getMethodName(),
            new TimeoutOptions(DEFAULT_OPTS.getTimeoutSeconds(), DEFAULT_OPTS.getErrorCode()));
    TimeoutHandler.handler(Optional.empty(), options).handle(ctx);

    return new TimerSetup(ctx, response, connection, timerCaptor.getValue());
  }

  private record TimerSetup(
      RoutingContext ctx,
      HttpServerResponse response,
      HttpConnection connection,
      Handler<Long> timerCallback) {}

  @Test
  void timeoutBeforeHeaders_callsCtxFail() {
    final TimerSetup setup = setupTimerCapture();
    when(setup.response().ended()).thenReturn(false);
    when(setup.response().closed()).thenReturn(false);
    when(setup.response().headWritten()).thenReturn(false);

    setup.timerCallback().handle(1L);

    verify(setup.ctx()).fail(DEFAULT_OPTS.getErrorCode());
    verify(setup.response(), never()).reset();
    verify(setup.connection()).close();
  }

  @Test
  void timeoutAfterHeaders_resetsInsteadOfFailing() {
    final TimerSetup setup = setupTimerCapture();
    when(setup.response().ended()).thenReturn(false);
    when(setup.response().closed()).thenReturn(false);
    when(setup.response().headWritten()).thenReturn(true);

    setup.timerCallback().handle(1L);

    verify(setup.response()).reset();
    verify(setup.ctx(), never()).fail(DEFAULT_OPTS.getErrorCode());
    verify(setup.connection()).close();
  }

  @Test
  void timeoutAfterResponseEnded_doesNothing() {
    final TimerSetup setup = setupTimerCapture();
    when(setup.response().ended()).thenReturn(true);

    setup.timerCallback().handle(1L);

    verify(setup.response(), never()).reset();
    verify(setup.ctx(), never()).fail(DEFAULT_OPTS.getErrorCode());
    verify(setup.connection(), never()).close();
  }

  @Test
  void timeoutAfterResponseClosed_doesNothing() {
    final TimerSetup setup = setupTimerCapture();
    when(setup.response().ended()).thenReturn(false);
    when(setup.response().closed()).thenReturn(true);

    setup.timerCallback().handle(1L);

    verify(setup.response(), never()).reset();
    verify(setup.ctx(), never()).fail(DEFAULT_OPTS.getErrorCode());
    verify(setup.connection(), never()).close();
  }
}
