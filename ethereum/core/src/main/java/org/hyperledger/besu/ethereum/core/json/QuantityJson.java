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
package org.hyperledger.besu.ethereum.core.json;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.tuweni.units.bigints.UInt64;
import org.apache.tuweni.units.bigints.UInt64s;

public final class QuantityJson {

  private static final UInt64 LONG_MAX_VALUE = UInt64.valueOf(Long.MAX_VALUE);

  /**
   * Engine/execution-apis {@code uint64} QUANTITY: {@code 0x}-prefixed hex, no leading zeros except
   * {@code 0x0}, at most 16 hex digits ({@code
   * ^0x(0|[1-9a-fA-F][0-9a-fA-F]{0,15})$}). Hex digits may be upper or lower case; the {@code 0x}
   * prefix must be lower case, matching the OpenRPC base type.
   */
  private static final Pattern UINT64_QUANTITY =
      Pattern.compile("^0x(0|[1-9a-fA-F][0-9a-fA-F]{0,15})$");

  private QuantityJson() {}

  public static String format(final long value) {
    return "0x" + Long.toHexString(value);
  }

  /**
   * Parses an Engine API {@code uint64} QUANTITY into a long (unsigned semantics for values above
   * {@link Long#MAX_VALUE}).
   *
   * @param quantity the JSON string form of the quantity
   * @return the value as a long
   * @throws IllegalArgumentException if {@code quantity} is not a {@code 0x}-prefixed QUANTITY
   */
  public static long parseUint64(final String quantity) {
    if (quantity == null || !UINT64_QUANTITY.matcher(quantity).matches()) {
      throw new IllegalArgumentException(
          "Invalid QUANTITY: must be a 0x-prefixed hex string with no leading zeros (except 0x0)"
              + " and at most 16 hex digits");
    }
    return UInt64.fromHexString(quantity).toBytes().toLong();
  }

  public static class LongSerializer extends StdSerializer<Long> {

    public LongSerializer() {
      super(Long.class);
    }

    @Override
    public void serialize(
        final Long value, final JsonGenerator gen, final SerializerProvider provider)
        throws IOException {
      gen.writeString(format(value));
    }
  }

  public static class LongDeserializer extends StdDeserializer<Long> {

    public LongDeserializer() {
      this(null);
    }

    public LongDeserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public Long deserialize(final JsonParser jsonParser, final DeserializationContext context)
        throws IOException {
      return UInt64.fromHexString(jsonParser.getCodec().readValue(jsonParser, String.class))
          .toLong();
    }
  }

  /**
   * Engine API QUANTITYs are {@code uint64}, so {@code 0xffffffffffffffff} is syntactically valid
   * and must be answered by block validation rather than an "invalid params" error. Values above
   * {@link Long#MAX_VALUE} are carried as negative longs instead of being rejected.
   */
  public static class UnsignedLongDeserializer extends StdDeserializer<Long> {

    public UnsignedLongDeserializer() {
      this(null);
    }

    public UnsignedLongDeserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public Long deserialize(final JsonParser jsonParser, final DeserializationContext context)
        throws IOException {
      return parseUint64(jsonParser.getCodec().readValue(jsonParser, String.class));
    }
  }

  public static class GasDeserializer extends StdDeserializer<Long> {

    public GasDeserializer() {
      this(null);
    }

    public GasDeserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public Long deserialize(final JsonParser jsonParser, final DeserializationContext context)
        throws IOException {
      final UInt64 uint64 =
          UInt64.fromHexString(jsonParser.getCodec().readValue(jsonParser, String.class));
      return UInt64s.min(uint64, LONG_MAX_VALUE).toLong();
    }
  }

  public static class OptionalLongDeserializer extends StdDeserializer<OptionalLong> {

    public OptionalLongDeserializer() {
      this(null);
    }

    public OptionalLongDeserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public OptionalLong deserialize(
        final JsonParser jsonParser, final DeserializationContext context) throws IOException {
      final UInt64 uint64 =
          UInt64.fromHexString(jsonParser.getCodec().readValue(jsonParser, String.class));
      return OptionalLong.of(UInt64s.min(uint64, LONG_MAX_VALUE).toLong());
    }
  }
}
