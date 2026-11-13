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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.hyperledger.besu.ethereum.core.json.TransactionJson.BlockBodyListDeserializer.MIN_TRANSACTIONS_TO_DECODE_IN_PARALLEL;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonMappingException.Reference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TransactionJsonTest {

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new BesuJsonModule());
  private final BlockDataGenerator gen = new BlockDataGenerator();

  private static final int SHORT_LIST = MIN_TRANSACTIONS_TO_DECODE_IN_PARALLEL - 1;
  private static final int LONG_LIST = 3 * MIN_TRANSACTIONS_TO_DECODE_IN_PARALLEL;

  @ParameterizedTest
  @ValueSource(ints = {SHORT_LIST, LONG_LIST})
  void listDeserializerDecodesInOrder(final int count) throws Exception {
    final List<Transaction> transactions = transactions(count);
    final String json = "{\"transactions\":" + mapper.writeValueAsString(transactions) + "}";

    final Holder holder = mapper.readValue(json, Holder.class);

    assertThat(holder.transactions)
        .isInstanceOf(ArrayList.class)
        .extracting(Transaction::getHash)
        .containsExactlyElementsOf(transactions.stream().map(Transaction::getHash).toList());
  }

  @ParameterizedTest
  @ValueSource(ints = {SHORT_LIST, LONG_LIST})
  void listDeserializerReportsTheFirstUndecodableTransaction(final int count) throws Exception {
    final List<String> encoded = encoded(count);
    final int first = count / 3;
    encoded.set(2 * first, "0xDEAD");
    encoded.set(first, "0x");

    assertDecodingFailsAt(encoded, first, "Empty opaque bytes");
  }

  @ParameterizedTest
  @ValueSource(ints = {SHORT_LIST, LONG_LIST})
  void listDeserializerRejectsANullTransaction(final int count) throws Exception {
    final List<String> encoded = encoded(count);
    final int index = count / 2;
    encoded.set(index, null);

    assertDecodingFailsAt(encoded, index, "Null transaction");
  }

  private void assertDecodingFailsAt(
      final List<String> encoded, final int index, final String message) throws Exception {
    final String json = "{\"transactions\":" + mapper.writeValueAsString(encoded) + "}";

    assertThatThrownBy(() -> mapper.readValue(json, Holder.class))
        .isInstanceOfSatisfying(
            JsonMappingException.class,
            e -> {
              assertThat(e.getPath())
                  .extracting(Reference::getFieldName, Reference::getIndex)
                  .containsExactly(tuple("transactions", -1), tuple(null, index));
              assertThat(e.getOriginalMessage()).isEqualTo(message);
              assertThat(e.getCause())
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessage(message)
                  .hasNoCause();
            });
  }

  private List<String> encoded(final int count) throws Exception {
    final List<String> encoded = new ArrayList<>();
    for (final Transaction transaction : transactions(count)) {
      encoded.add(mapper.readValue(mapper.writeValueAsString(transaction), String.class));
    }
    return encoded;
  }

  private List<Transaction> transactions(final int count) {
    return IntStream.range(0, count).mapToObj(i -> gen.transaction()).toList();
  }

  static class Holder {
    @JsonDeserialize(using = TransactionJson.BlockBodyListDeserializer.class)
    public List<Transaction> transactions;
  }
}
