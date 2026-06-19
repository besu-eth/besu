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

  private JsonRpcObjectMapperFactory() {}

  public static ObjectMapper createBaseMapper() {
    return new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new BesuJsonModule());
  }

  public static ObjectMapper createParameterMapper() {
    return createBaseMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
  }

  public static ObjectMapper createParameterMapperIgnoringUnknownNulls() {
    return createParameterMapper().addHandler(new IgnoreNullUnknownHandler());
  }

  public static ObjectMapper createResponseMapper() {
    return createBaseMapper();
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
