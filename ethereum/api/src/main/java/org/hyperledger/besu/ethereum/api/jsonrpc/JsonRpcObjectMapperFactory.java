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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.json.BesuJsonModule;

import java.io.IOException;
import java.util.Optional;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public final class JsonRpcObjectMapperFactory {
  private static final ObjectMapper BASE_MAPPER = createBaseMapper();
  private static final ObjectMapper PARAMETER_MAPPER = createParameterMapper();
  private static final ObjectMapper PARAMETER_MAPPER_IGNORING_UNKNOWN_NULLS =
      createParameterMapperIgnoringUnknownNulls();
  private static final ObjectMapper PARAMETER_MAPPER_IGNORING_UNKNOWN_EMPTIES =
      createParameterMapperIgnoringUnknownEmpties();
  private static final ObjectMapper RESPONSE_MAPPER = createResponseMapper();

  private JsonRpcObjectMapperFactory() {}

  private static ObjectMapper createBaseMapper() {
    return new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new BesuJsonModule());
  }

  private static ObjectMapper createParameterMapper() {
    // copy() so configuring this mapper does not mutate the shared base/response mapper
    return getBaseMapper()
        .copy()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
        .registerModule(
            new SimpleModule("json-rpc-parameters")
                .addDeserializer(Address.class, new StrictAddressDeserializer())
                .addKeyDeserializer(Address.class, new StrictAddressKeyDeserializer()));
  }

  private static ObjectMapper createParameterMapperIgnoringUnknownNulls() {
    // copy() so adding the handler does not mutate the strict (DEFAULT) parameter mapper
    return getParameterMapper().copy().addHandler(new IgnoreNullUnknownHandler());
  }

  private static ObjectMapper createParameterMapperIgnoringUnknownEmpties() {
    return getParameterMapper().copy().addHandler(new IgnoreEmptyUnknownHandler());
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

  public static ObjectMapper getParameterMapperIgnoringUnknownEmpties() {
    return PARAMETER_MAPPER_IGNORING_UNKNOWN_EMPTIES;
  }

  public static ObjectMapper getResponseMapper() {
    return RESPONSE_MAPPER;
  }

  /**
   * Accepts only the execution-apis address form, {@code 0x} (or {@code 0X}) followed by exactly 40
   * hex digits, as geth's {@code hexutil.UnmarshalFixedText} does, so a truncated address is
   * rejected instead of being left-padded to a different account.
   */
  private static final Pattern STRICT_ADDRESS = Pattern.compile("0[xX][0-9a-fA-F]{40}");

  /** Describes the address form {@link #parseStrictAddress(String)} accepts. */
  public static final String STRICT_ADDRESS_MESSAGE =
      "expected 0x or 0X followed by 40 hexadecimal digits";

  /**
   * Parses a strict address, for address inputs that are read as strings rather than through the
   * parameter mappers.
   *
   * @param value the address string
   * @return the address, or empty if {@code value} is null or not a strict address
   */
  public static Optional<Address> parseStrictAddress(final String value) {
    if (value == null || !STRICT_ADDRESS.matcher(value).matches()) {
      return Optional.empty();
    }
    return Optional.of(Address.fromHexStringStrict("0x" + value.substring(2)));
  }

  private static class StrictAddressDeserializer extends StdDeserializer<Address> {
    private StrictAddressDeserializer() {
      super(Address.class);
    }

    @Override
    public Address deserialize(final JsonParser p, final DeserializationContext ctxt)
        throws IOException {
      if (p.currentToken() != JsonToken.VALUE_STRING) {
        return (Address) ctxt.handleUnexpectedToken(Address.class, p);
      }
      final String value = p.getText();
      final Optional<Address> address = parseStrictAddress(value);
      if (address.isEmpty()) {
        return (Address) ctxt.handleWeirdStringValue(Address.class, value, STRICT_ADDRESS_MESSAGE);
      }
      return address.get();
    }
  }

  /**
   * Applies the same rule to address map keys, such as the accounts of a state override, which
   * Jackson would otherwise build through the lenient {@link Address#fromHexString(String)}.
   */
  private static class StrictAddressKeyDeserializer extends KeyDeserializer {
    @Override
    public Object deserializeKey(final String key, final DeserializationContext ctxt)
        throws IOException {
      final Optional<Address> address = parseStrictAddress(key);
      if (address.isEmpty()) {
        return ctxt.handleWeirdKey(Address.class, key, STRICT_ADDRESS_MESSAGE);
      }
      return address.get();
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
        throws IOException {
      if (p.currentToken() != JsonToken.VALUE_NULL) {
        return false;
      }
      p.skipChildren();
      return true;
    }
  }

  /**
   * An empty unknown property states nothing, so an engine payload carrying one (e.g. {@code
   * blockAccessList} on {@code engine_newPayloadV4}) is left for block validation to judge rather
   * than rejected as invalid params. A non-empty value, {@code 0xc0} included, is a real attempt to
   * set the field and still fails.
   */
  private static class IgnoreEmptyUnknownHandler extends DeserializationProblemHandler {
    @Override
    public boolean handleUnknownProperty(
        final DeserializationContext ctxt,
        final JsonParser p,
        final JsonDeserializer<?> deserializer,
        final Object beanOrClass,
        final String propertyName)
        throws IOException {
      if (!isValueless(p)) {
        return false;
      }
      p.skipChildren();
      return true;
    }

    private boolean isValueless(final JsonParser p) throws IOException {
      final JsonToken token = p.currentToken();
      if (token == JsonToken.VALUE_NULL) {
        return true;
      }
      if (token != JsonToken.VALUE_STRING) {
        return false;
      }
      final String value = p.getText();
      return value.isEmpty() || "0x".equalsIgnoreCase(value);
    }
  }
}
