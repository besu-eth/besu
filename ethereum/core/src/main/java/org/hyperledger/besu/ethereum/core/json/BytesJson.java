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
import org.apache.tuweni.bytes.Bytes;

public final class BytesJson {

  private BytesJson() {}

  public static class Serializer extends StdSerializer<Bytes> {

    public Serializer() {
      super(Bytes.class);
    }

    @Override
    public void serialize(
        final Bytes value, final JsonGenerator gen, final SerializerProvider provider)
        throws IOException {
      gen.writeString(value.toString());
    }
  }

  public static class Deserializer extends StdDeserializer<Bytes> {

    public Deserializer() {
      this(null);
    }

    public Deserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public Bytes deserialize(final JsonParser jsonParser, final DeserializationContext context)
        throws IOException {
      final String value = jsonParser.getCodec().readValue(jsonParser, String.class);
      if (value.startsWith("0x") || value.startsWith("0X")) {
        return Bytes.fromHexString(value);
      }
      throw new IllegalArgumentException(
          "Invalid bytes: must be a hex string with 0x prefix, got: " + value);
    }
  }
}
