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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.tuweni.units.bigints.UInt64;

public class UInt64Deserializer extends StdDeserializer<UInt64> {

  public UInt64Deserializer() {
    this(null);
  }

  public UInt64Deserializer(final Class<?> vc) {
    super(vc);
  }

  @Override
  public UInt64 deserialize(final JsonParser jsonparser, final DeserializationContext context)
      throws IOException {
    return UInt64.fromHexString(jsonparser.getCodec().readValue(jsonparser, String.class));
  }
}
