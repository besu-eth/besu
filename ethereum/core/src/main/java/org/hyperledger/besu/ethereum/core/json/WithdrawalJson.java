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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.tuweni.units.bigints.UInt64;

public final class WithdrawalJson {

  private WithdrawalJson() {}

  public static class Serializer extends StdSerializer<Withdrawal> {

    public Serializer() {
      super(Withdrawal.class);
    }

    @Override
    public void serialize(
        final Withdrawal value, final JsonGenerator gen, final SerializerProvider provider)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("index", value.getIndex().toBytes().toQuantityHexString());
      gen.writeStringField(
          "validatorIndex", value.getValidatorIndex().toBytes().toQuantityHexString());
      gen.writeStringField("address", value.getAddress().toString());
      gen.writeStringField("amount", value.getAmount().toShortHexString());
      gen.writeEndObject();
    }
  }

  public static class Deserializer extends StdDeserializer<Withdrawal> {

    public Deserializer() {
      this(null);
    }

    public Deserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public Withdrawal deserialize(final JsonParser jsonParser, final DeserializationContext context)
        throws IOException {
      final JsonNode node = jsonParser.getCodec().readTree(jsonParser);
      return new Withdrawal(
          UInt64.fromHexString(requiredText(node, "index")),
          UInt64.fromHexString(requiredText(node, "validatorIndex")),
          Address.fromHexString(requiredText(node, "address")),
          GWei.fromHexString(requiredText(node, "amount")));
    }

    private static String requiredText(final JsonNode node, final String fieldName) {
      final JsonNode field = node.get(fieldName);
      if (field == null || field.isNull()) {
        throw new IllegalArgumentException("Missing withdrawal field: " + fieldName);
      }
      return field.asText();
    }
  }
}
