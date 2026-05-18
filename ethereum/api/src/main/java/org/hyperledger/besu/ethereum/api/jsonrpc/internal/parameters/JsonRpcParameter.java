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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public class JsonRpcParameter {

  /**
   * Jackson's default {@link ObjectMapper}. Classes that need to tolerate unknown JSON properties
   * must opt out individually via {@code @JsonIgnoreProperties(ignoreUnknown = true)} — without
   * that annotation, an unknown property fails deserialization regardless of its value.
   */
  private static final ObjectMapper mapperDefault =
      new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
          .registerModule(new Jdk8Module()); // Handle JDK8 Optionals (de)serialization

  /**
   * Like mapperDefault but with {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES}
   * explicitly enabled and an {@link IgnoreNullUnknownHandler} attached: unknown JSON properties
   * whose value is {@code null} are silently dropped, while unknown properties with a non-{@code
   * null} value still cause deserialization to fail.
   */
  private static final ObjectMapper mapperFailOnUnknownButNull =
      mapperDefault
          .copy()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
          .addHandler(new IgnoreNullUnknownHandler());

  /**
   * Retrieves a required parameter at the given index interpreted as the given class. Throws
   * InvalidJsonRpcParameters if parameter is missing or of the wrong type.
   *
   * @param params the list of objects from which to extract a typed object.
   * @param index Which index of the params array to access.
   * @param paramClass What type is expected at this index.
   * @param configuration the {@link Configuration} controlling deserialization behaviour (e.g.
   *     {@link Configuration#DEFAULT} for Jackson's default mapper, {@link
   *     Configuration#FAIL_ON_UNKNOWN_BUT_NULL} to reject unknown JSON properties unless their
   *     value is {@code null}).
   * @param <T> The type of parameter.
   * @return Returns the parameter cast as T if available, otherwise throws exception.
   * @throws JsonRpcParameterException if the parameter is missing or fails deserialization
   */
  public <T> T required(
      final Object[] params,
      final int index,
      final Class<T> paramClass,
      final Configuration configuration)
      throws JsonRpcParameterException {
    return optional(params, index, paramClass, configuration)
        .orElseThrow(
            () ->
                new JsonRpcParameterException(
                    "Missing required json rpc parameter at index " + index));
  }

  public <T> T required(final Object[] params, final int index, final Class<T> paramClass)
      throws JsonRpcParameterException {
    return required(params, index, paramClass, Configuration.DEFAULT);
  }

  /**
   * Retrieves an optional parameter at the given index interpreted as the given class. Throws
   * InvalidJsonRpcParameters if parameter is of the wrong type.
   *
   * @param params the list of objects from which to extract a typed object.
   * @param index Which index of the params array to access.
   * @param paramClass What type is expected at this index.
   * @param configuration the {@link Configuration} controlling deserialization behaviour (e.g.
   *     {@link Configuration#DEFAULT} for Jackson's default mapper, {@link
   *     Configuration#FAIL_ON_UNKNOWN_BUT_NULL} to reject unknown JSON properties unless their
   *     value is {@code null}).
   * @param <T> The type of parameter.
   * @return Returns the parameter cast as T if available.
   * @throws JsonRpcParameterException if the parameter is present but fails deserialization
   */
  public <T> Optional<T> optional(
      final Object[] params,
      final int index,
      final Class<T> paramClass,
      final Configuration configuration)
      throws JsonRpcParameterException {
    if (params == null || params.length <= index || params[index] == null) {
      return Optional.empty();
    }

    final T param;
    final Object rawParam = params[index];
    if (paramClass.isAssignableFrom(rawParam.getClass())) {
      // If we're dealing with a simple type, just cast the value
      param = paramClass.cast(rawParam);
    } else {
      try {
        param = configuration.mapper.convertValue(rawParam, paramClass);
      } catch (final Exception e) {
        throw new JsonRpcParameterException(
            String.format(
                "Invalid json rpc parameter at index %d. Supplied value was: '%s' of type: '%s' - expected type: '%s'",
                index, rawParam, rawParam.getClass().getName(), paramClass.getName()),
            e);
      }
    }

    return Optional.of(param);
  }

  public <T> Optional<T> optional(final Object[] params, final int index, final Class<T> paramClass)
      throws JsonRpcParameterException {
    return optional(params, index, paramClass, Configuration.DEFAULT);
  }

  /**
   * Retrieves an optional list parameter at the given index whose elements are interpreted as the
   * given class.
   *
   * @param params the list of objects from which to extract a typed object.
   * @param index Which index of the params array to access.
   * @param listClass What element type is expected at this index.
   * @param configuration the {@link Configuration} controlling deserialization behaviour (e.g.
   *     {@link Configuration#DEFAULT} for Jackson's default mapper, {@link
   *     Configuration#FAIL_ON_UNKNOWN_BUT_NULL} to reject unknown JSON properties unless their
   *     value is {@code null}).
   * @param <T> The element type of the list parameter.
   * @return Returns the list cast as {@code List<T>} if available; empty when the value at index is
   *     missing/null or is not a list.
   * @throws JsonRpcParameterException if the value at index is a list but fails deserialization
   */
  public <T> Optional<List<T>> optionalList(
      final Object[] params,
      final int index,
      final Class<T> listClass,
      final Configuration configuration)
      throws JsonRpcParameterException {
    if (params == null || params.length <= index || params[index] == null) {
      return Optional.empty();
    }
    Object rawParam = params[index];
    if (List.class.isAssignableFrom(rawParam.getClass())) {
      try {
        List<T> returnedList =
            configuration.mapper.convertValue(
                rawParam,
                configuration
                    .mapper
                    .getTypeFactory()
                    .constructCollectionType(List.class, listClass));
        return Optional.of(returnedList);
      } catch (Exception e) {
        throw new JsonRpcParameterException(
            String.format(
                "Invalid json rpc parameter at index %d. Supplied value was: '%s' of type: '%s' - expected type: '%s'",
                index, rawParam, rawParam.getClass().getName(), listClass.getName()),
            e);
      }
    }
    return Optional.empty();
  }

  public <T> Optional<List<T>> optionalList(
      final Object[] params, final int index, final Class<T> listClass)
      throws JsonRpcParameterException {
    return optionalList(params, index, listClass, Configuration.DEFAULT);
  }

  public static class JsonRpcParameterException extends Exception {
    public JsonRpcParameterException(final String message) {
      super(message);
    }

    public JsonRpcParameterException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Controls how Jackson deserializes the raw parameter into the requested target class. Callers
   * pick the configuration that matches the strictness their RPC method needs.
   */
  public enum Configuration {
    /**
     * Jackson's default {@link ObjectMapper}. Classes that need to tolerate unknown JSON properties
     * must opt out individually via {@code @JsonIgnoreProperties(ignoreUnknown = true)} — without
     * that annotation, an unknown property fails deserialization regardless of its value.
     */
    DEFAULT(mapperDefault),
    /**
     * Like {@link #DEFAULT} but with {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES}
     * explicitly enabled and an {@link IgnoreNullUnknownHandler} attached: unknown JSON properties
     * whose value is {@code null} are silently dropped, while unknown properties with a non-{@code
     * null} value still cause deserialization to fail.
     */
    FAIL_ON_UNKNOWN_BUT_NULL(mapperFailOnUnknownButNull);

    final ObjectMapper mapper;

    Configuration(final ObjectMapper mapper) {
      this.mapper = mapper;
    }
  }

  private static class IgnoreNullUnknownHandler extends DeserializationProblemHandler {
    @Override
    public boolean handleUnknownProperty(
        final DeserializationContext ctxt,
        final JsonParser p,
        final JsonDeserializer<?> deserializer,
        final Object beanOrClass,
        final String propertyName)
        throws java.io.IOException {
      if (p.currentToken() != JsonToken.VALUE_NULL) {
        return false;
      }
      // Per DeserializationProblemHandler contract, the handler must consume the value
      // when returning true. skipChildren() is the idiomatic call; it is a no-op for
      // scalar tokens (including VALUE_NULL) but satisfies the contract and protects
      // against different Jackson code paths that do not advance the parser after
      // seeing a true return value.
      p.skipChildren();
      return true;
    }
  }
}
