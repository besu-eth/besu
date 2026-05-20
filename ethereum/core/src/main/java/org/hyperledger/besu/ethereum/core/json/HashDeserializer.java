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

import org.hyperledger.besu.datatypes.Hash;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class HashDeserializer extends StdDeserializer<Hash> {

  public HashDeserializer() {
    this(null);
  }

  public HashDeserializer(final Class<?> vc) {
    super(vc);
  }

  @Override
  public Hash deserialize(final JsonParser jsonParser, final DeserializationContext context)
      throws IOException {
    final String value = jsonParser.getCodec().readValue(jsonParser, String.class);
    if (!value.startsWith("0x") && !value.startsWith("0X")) {
      throw new IllegalArgumentException(
          "Invalid hash: must be a hex string with 0x prefix, got: " + value);
    }
    return Hash.fromHexString(value);
  }
}
