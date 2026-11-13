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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

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

    @VisibleForTesting static final int MIN_TRANSACTIONS_TO_DECODE_IN_PARALLEL = 64;

    public BlockBodyListDeserializer() {
      super(List.class);
    }

    @Override
    public List<Transaction> deserialize(
        final JsonParser jsonParser, final DeserializationContext context) throws IOException {
      final String[] encoded = context.readValue(jsonParser, String[].class);
      final Transaction[] decoded = new Transaction[encoded.length];
      final AtomicReference<Failure> firstFailure = new AtomicReference<>();
      final IntFunction<Transaction> decodeAt = i -> decodeAt(encoded, i, firstFailure);
      if (encoded.length >= MIN_TRANSACTIONS_TO_DECODE_IN_PARALLEL) {
        Arrays.parallelSetAll(decoded, decodeAt);
      } else {
        Arrays.setAll(decoded, decodeAt);
      }
      final Failure failure = firstFailure.get();
      if (failure != null) {
        throw JsonMappingException.wrapWithPath(failure.cause(), List.class, failure.index());
      }
      return new ArrayList<>(Arrays.asList(decoded));
    }

    // A failure is recorded rather than thrown: an exception thrown on a pool worker reaches the
    // caller re-wrapped, and the error reported has to be the first in the list, not the first in
    // time.
    private static Transaction decodeAt(
        final String[] encoded, final int index, final AtomicReference<Failure> firstFailure) {
      final Failure failure = firstFailure.get();
      if (failure != null && failure.index() < index) {
        return null;
      }
      try {
        if (encoded[index] == null) {
          throw new IllegalArgumentException("Null transaction");
        }
        return decode(encoded[index]);
      } catch (final RuntimeException e) {
        firstFailure.accumulateAndGet(
            new Failure(index, e),
            (current, next) -> current == null || next.index() < current.index() ? next : current);
        return null;
      }
    }

    private record Failure(int index, RuntimeException cause) {}
  }

  private static Transaction decode(final String encoded) {
    return TransactionDecoder.decodeOpaqueBytes(
        Bytes.fromHexString(encoded), EncodingContext.BLOCK_BODY);
  }
}
