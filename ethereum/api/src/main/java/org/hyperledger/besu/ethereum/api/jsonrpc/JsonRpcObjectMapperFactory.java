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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import org.hyperledger.besu.ethereum.core.json.BesuJsonModule;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public final class JsonRpcObjectMapperFactory {
  private static final ObjectMapper BASE_MAPPER = createBaseMapper();
  private static final ObjectMapper PARAMETER_MAPPER = createParameterMapper();
  private static final ObjectMapper PARAMETER_MAPPER_IGNORING_UNKNOWN_NULLS =
      createParameterMapperIgnoringUnknownNulls();
  private static final ObjectMapper RESPONSE_MAPPER = createResponseMapper();

  private JsonRpcObjectMapperFactory() {}

  private static ObjectMapper createBaseMapper() {
    return new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new BesuJsonModule());
  }

  private static ObjectMapper createParameterMapper() {
    // copy() so configuring this mapper does not mutate the shared base/response mapper
    return getBaseMapper()
        .copy()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
  }

  private static ObjectMapper createParameterMapperIgnoringUnknownNulls() {
    // copy() so adding the handler does not mutate the strict (DEFAULT) parameter mapper
    return getParameterMapper().copy().addHandler(new IgnoreNullUnknownHandler());
  }

  private static ObjectMapper createResponseMapper() {
    return getBaseMapper().copy();
  }

  public static ObjectMapper getBaseMapper() {
    return BASE_MAPPER;
  }

  public static ObjectMapper getParameterMapper() {
    return PARAMETER_MAPPER;
  }

  public static ObjectMapper getParameterMapperIgnoringUnknownNulls() {
    return PARAMETER_MAPPER_IGNORING_UNKNOWN_NULLS;
  }

  public static ObjectMapper getResponseMapper() {
    return RESPONSE_MAPPER;
  }

  private static class IgnoreNullUnknownHandler extends DeserializationProblemHandler {
    @Override
    public boolean handleUnknownProperty(
        final DeserializationContext ctxt,
        final JsonParser p,
        final JsonDeserializer<?> deserializer,
        final Object beanOrClass,
        final String propertyName)
        throws IOException {
      if (p.currentToken() != JsonToken.VALUE_NULL) {
        return false;
      }
      p.skipChildren();
      return true;
    }
  }
}
