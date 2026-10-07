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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.tuweni.units.bigints.UInt64;

public final class UInt64Json {

  private UInt64Json() {}

  public static class Serializer extends StdSerializer<UInt64> {

    public Serializer() {
      super(UInt64.class);
    }

    @Override
    public void serialize(
        final UInt64 value, final JsonGenerator gen, final SerializerProvider provider)
        throws IOException {
      gen.writeString(value.toBytes().toQuantityHexString());
    }
  }

  public static class Deserializer extends StdDeserializer<UInt64> {

    public Deserializer() {
      this(null);
    }

    public Deserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public UInt64 deserialize(final JsonParser jsonParser, final DeserializationContext context)
        throws IOException {
      return UInt64.fromHexString(jsonParser.getCodec().readValue(jsonParser, String.class));
    }
  }
}
