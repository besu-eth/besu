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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.Configuration.DEFAULT;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.Configuration.FAIL_ON_UNKNOWN_BUT_NULL;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Test;

class JsonRpcParameterTest {

  private final JsonRpcParameter parameterAccessor = new JsonRpcParameter();

  // ---- shape / sentinel handling (no Jackson involvement) ----

  @Test
  void optional_returnsEmpty_whenParamsAreNull() throws Exception {
    assertThat(parameterAccessor.optional(null, 0, KnownOnly.class, DEFAULT)).isEmpty();
  }

  @Test
  void optional_returnsEmpty_whenIndexOutOfBounds() throws Exception {
    assertThat(parameterAccessor.optional(new Object[] {}, 0, KnownOnly.class, DEFAULT)).isEmpty();
  }

  @Test
  void optional_returnsEmpty_whenValueAtIndexIsNull() throws Exception {
    assertThat(parameterAccessor.optional(new Object[] {null}, 0, KnownOnly.class, DEFAULT))
        .isEmpty();
  }

  @Test
  void optional_castsDirectly_whenRawParamIsAlreadyExpectedType() throws Exception {
    final KnownOnly direct = new KnownOnly("hello");
    final Optional<KnownOnly> result =
        parameterAccessor.optional(new Object[] {direct}, 0, KnownOnly.class, DEFAULT);
    assertThat(result).containsSame(direct);
  }

  @Test
  void required_throws_whenParamMissing() {
    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(() -> parameterAccessor.required(new Object[] {}, 0, KnownOnly.class, DEFAULT))
        .withMessageContaining("Missing required json rpc parameter at index 0");
  }

  @Test
  void optionalList_returnsEmpty_whenParamsAreNull() throws Exception {
    assertThat(parameterAccessor.optionalList(null, 0, String.class, DEFAULT)).isEmpty();
  }

  @Test
  void optionalList_returnsEmpty_whenValueAtIndexIsNotAList() throws Exception {
    assertThat(
            parameterAccessor.optionalList(new Object[] {"not-a-list"}, 0, String.class, DEFAULT))
        .isEmpty();
  }

  @Test
  void optionalList_convertsStringElements() throws Exception {
    final Optional<List<String>> result =
        parameterAccessor.optionalList(new Object[] {List.of("a", "b")}, 0, String.class, DEFAULT);
    assertThat(result).contains(List.of("a", "b"));
  }

  @Test
  void optionalList_convertsComplexPojoElements() throws Exception {
    // Without TypeFactory.constructCollectionType(List.class, listClass), Jackson cannot drive
    // element conversion and silently returns List<LinkedHashMap> instead of List<KnownOnly>;
    // the cast to KnownOnly would then fail at the first element access.
    final Map<String, Object> raw = Map.of("known", "hello");
    final Optional<List<KnownOnly>> result =
        parameterAccessor.optionalList(new Object[] {List.of(raw)}, 0, KnownOnly.class, DEFAULT);
    assertThat(result).isPresent();
    assertThat(result.get()).hasSize(1);
    assertThat(result.get().get(0).known).isEqualTo("hello");
  }

  // ---- Configuration.DEFAULT behaviour: strict (Jackson's default) ----

  @Test
  void defaultConfiguration_fails_whenUnknownPropertyPresent_andClassNotAnnotated() {
    // mapperDefault inherits Jackson's default FAIL_ON_UNKNOWN_PROPERTIES=true, so a class that
    // doesn't opt out with @JsonIgnoreProperties(ignoreUnknown=true) will fail on unknown fields.
    final Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("known", "value");
    raw.put("unknown", "anything");

    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(
            () -> parameterAccessor.optional(new Object[] {raw}, 0, KnownOnly.class, DEFAULT))
        .withMessageContaining("Invalid json rpc parameter at index 0");
  }

  @Test
  void defaultConfiguration_succeeds_whenClassAnnotatedIgnoreUnknown() throws Exception {
    // Classes that carry @JsonIgnoreProperties(ignoreUnknown=true) keep the legacy "lenient"
    // behaviour even with the default configuration.
    final Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("known", "value");
    raw.put("unknown", "ignored");

    final Optional<KnownLenient> result =
        parameterAccessor.optional(new Object[] {raw}, 0, KnownLenient.class, DEFAULT);

    assertThat(result).isPresent();
    assertThat(result.get().known).isEqualTo("value");
  }

  // ---- FAIL_ON_UNKNOWN_BUT_NULL behaviour ----

  @Test
  void failOnUnknownButNull_succeeds_whenAllPropertiesAreKnown() throws Exception {
    final Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("known", "value");

    final Optional<KnownOnly> result =
        parameterAccessor.optional(
            new Object[] {raw}, 0, KnownOnly.class, FAIL_ON_UNKNOWN_BUT_NULL);

    assertThat(result).isPresent();
    assertThat(result.get().known).isEqualTo("value");
  }

  @Test
  void failOnUnknownButNull_succeeds_whenUnknownPropertyIsNull() throws Exception {
    final Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("known", "value");
    raw.put("unknown", null);

    final Optional<KnownOnly> result =
        parameterAccessor.optional(
            new Object[] {raw}, 0, KnownOnly.class, FAIL_ON_UNKNOWN_BUT_NULL);

    assertThat(result).isPresent();
    assertThat(result.get().known).isEqualTo("value");
  }

  @Test
  void failOnUnknownButNull_succeeds_whenMultipleUnknownPropertiesAreAllNull() throws Exception {
    final Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("known", "value");
    raw.put("unknownA", null);
    raw.put("unknownB", null);

    final Optional<KnownOnly> result =
        parameterAccessor.optional(
            new Object[] {raw}, 0, KnownOnly.class, FAIL_ON_UNKNOWN_BUT_NULL);

    assertThat(result).isPresent();
    assertThat(result.get().known).isEqualTo("value");
  }

  @Test
  void failOnUnknownButNull_fails_whenUnknownPropertyIsNonNull() {
    final Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("known", "value");
    raw.put("unknown", "not-null");

    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(
            () ->
                parameterAccessor.optional(
                    new Object[] {raw}, 0, KnownOnly.class, FAIL_ON_UNKNOWN_BUT_NULL))
        .withMessageContaining("Invalid json rpc parameter at index 0")
        .withMessageContaining(KnownOnly.class.getName());
  }

  @Test
  void failOnUnknownButNull_fails_whenMixOfNullAndNonNullUnknownProperties() {
    final Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("known", "value");
    raw.put("unknownNull", null);
    raw.put("unknownNonNull", 42);

    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(
            () ->
                parameterAccessor.optional(
                    new Object[] {raw}, 0, KnownOnly.class, FAIL_ON_UNKNOWN_BUT_NULL))
        .withMessageContaining("Invalid json rpc parameter at index 0");
  }

  @Test
  void required_propagatesConfigurationToOptional() throws Exception {
    // required() delegates to optional(); verifying the configuration is forwarded.
    final Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("known", "value");
    raw.put("unknown", null);

    final KnownOnly result =
        parameterAccessor.required(
            new Object[] {raw}, 0, KnownOnly.class, FAIL_ON_UNKNOWN_BUT_NULL);
    assertThat(result.known).isEqualTo("value");

    final Map<String, Object> bad = new LinkedHashMap<>();
    bad.put("known", "value");
    bad.put("unknown", "not-null");
    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(
            () ->
                parameterAccessor.required(
                    new Object[] {bad}, 0, KnownOnly.class, FAIL_ON_UNKNOWN_BUT_NULL))
        .withMessageContaining("Invalid json rpc parameter at index 0");
  }

  // ---- DEFAULT vs FAIL_ON_UNKNOWN_BUT_NULL: differing behaviour for the same payload ----

  @Test
  void defaultAndFailOnUnknownButNull_disagree_whenUnknownPropertyIsNull() throws Exception {
    // Same payload, two configurations, two outcomes — anchors that the two values are
    // genuinely distinct.
    final Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("known", "value");
    raw.put("unknown", null);

    // DEFAULT: strict, no null-tolerance handler → throws.
    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(
            () -> parameterAccessor.optional(new Object[] {raw}, 0, KnownOnly.class, DEFAULT));

    // FAIL_ON_UNKNOWN_BUT_NULL: same strictness but the null-valued unknown is dropped → succeeds.
    final Optional<KnownOnly> result =
        parameterAccessor.optional(
            new Object[] {raw}, 0, KnownOnly.class, FAIL_ON_UNKNOWN_BUT_NULL);
    assertThat(result).isPresent();
    assertThat(result.get().known).isEqualTo("value");
  }

  /** Test POJO with a single known property; strict, no @JsonIgnoreProperties opt-out. */
  static class KnownOnly {
    public final String known;

    @JsonCreator
    KnownOnly(@JsonProperty("known") final String known) {
      this.known = known;
    }
  }

  /** Test POJO that opts into lenient (ignore unknown) deserialization. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class KnownLenient {
    public final String known;

    @JsonCreator
    KnownLenient(@JsonProperty("known") final String known) {
      this.known = known;
    }
  }
}
