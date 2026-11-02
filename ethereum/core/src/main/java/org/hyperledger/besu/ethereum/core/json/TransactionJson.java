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

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.util.HexUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public final class TransactionJson {

  private TransactionJson() {}

  public static class BlockBodySerializer extends StdSerializer<Transaction> {

    public BlockBodySerializer() {
      super(Transaction.class);
    }

    @Override
    public void serialize(
        final Transaction value, final JsonGenerator gen, final SerializerProvider provider)
        throws IOException {
      gen.writeString(
          HexUtils.toFastHex(
              TransactionEncoder.encodeOpaqueBytes(value, EncodingContext.BLOCK_BODY), true));
    }
  }

  public static class BlockBodyDeserializer extends StdDeserializer<Transaction> {

    public BlockBodyDeserializer() {
      this(null);
    }

    public BlockBodyDeserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public Transaction deserialize(
        final JsonParser jsonParser, final DeserializationContext context) throws IOException {
      return decode(jsonParser.getCodec().readValue(jsonParser, String.class));
    }
  }

  /**
   * Deserializes a list of transactions, each block-body encoded, decoding a long list in parallel.
   * Decoding hashes every transaction, which for a large block would otherwise run one transaction
   * at a time on the thread that reads the request.
   */
  public static class BlockBodyListDeserializer extends StdDeserializer<List<Transaction>> {

    private static final int MIN_TRANSACTIONS_TO_DECODE_IN_PARALLEL = 64;

    public BlockBodyListDeserializer() {
      super(List.class);
    }

    @Override
    public List<Transaction> deserialize(
        final JsonParser jsonParser, final DeserializationContext context) throws IOException {
      final String[] encoded = jsonParser.getCodec().readValue(jsonParser, String[].class);
      final IntStream indices = IntStream.range(0, encoded.length);
      return (encoded.length >= MIN_TRANSACTIONS_TO_DECODE_IN_PARALLEL
              ? indices.parallel()
              : indices)
          .mapToObj(i -> encoded[i] == null ? null : decode(encoded[i]))
          .toList();
    }
  }

  private static Transaction decode(final String encoded) {
    return TransactionDecoder.decodeOpaqueBytes(
        HexUtils.fromHexString(encoded), EncodingContext.BLOCK_BODY);
  }
}
