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

  private QuantityJson() {}

  public static String format(final long value) {
    return "0x" + Long.toHexString(value);
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
