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
package org.hyperledger.besu.ethereum.api.jsonrpc.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.context.ContextKey.REQUEST_BODY_AS_JSON_OBJECT;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ContextKeyTest<T> {

  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {REQUEST_BODY_AS_JSON_OBJECT, ctx(null), supplier("default"), "default"},
          {REQUEST_BODY_AS_JSON_OBJECT, ctx("non-default"), supplier("default"), "non-default"},
          {
            REQUEST_BODY_AS_JSON_OBJECT,
            ctx(new JsonObject(JSON)),
            supplier(null),
            new JsonObject(JSON)
          }
        });
  }

  private static final String JSON = "{\"key\": \"value\"}";

  @ParameterizedTest
  @MethodSource("data")
  public void test(
      final ContextKey key,
      final RoutingContext ctx,
      final Supplier<T> defaultSupplier,
      final T expected) {
    assertThat(key.extractFrom(ctx, defaultSupplier)).isEqualTo(expected);
  }

  private static <T> RoutingContext ctx(final T value) {
    final RoutingContext ctx = mock(RoutingContext.class);
    when(ctx.get(anyString())).thenReturn(value);
    return ctx;
  }

  private static <T> Supplier<T> supplier(final T value) {
    return () -> value;
  }
}
