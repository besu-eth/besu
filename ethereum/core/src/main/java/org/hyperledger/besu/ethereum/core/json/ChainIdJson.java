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
import java.math.BigInteger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.tuweni.units.bigints.UInt256;

public final class ChainIdJson {

  private ChainIdJson() {}

  public static class Deserializer extends StdDeserializer<BigInteger> {

    public Deserializer() {
      this(null);
    }

    public Deserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public BigInteger deserialize(final JsonParser jsonParser, final DeserializationContext context)
        throws IOException {
      final BigInteger chainId =
          UInt256.fromHexString(jsonParser.getCodec().readValue(jsonParser, String.class))
              .toBigInteger();
      if (chainId.signum() < 0) {
        throw new IllegalArgumentException("Negative chain id: " + chainId);
      }
      return chainId;
    }
  }
}
