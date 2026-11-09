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

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.junit.jupiter.api.Test;

class TransactionJsonTest {

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new BesuJsonModule());
  private final BlockDataGenerator gen = new BlockDataGenerator();

  @Test
  void listDeserializerDecodesALongListInOrder() throws Exception {
    assertDecodesInOrder(200);
  }

  @Test
  void listDeserializerDecodesAShortListInOrder() throws Exception {
    assertDecodesInOrder(3);
  }

  @Test
  void listDeserializerFailsOnTheTransactionsPropertyForAnEmptyTransaction() throws Exception {
    final List<String> encoded = new ArrayList<>();
    for (final Transaction transaction : transactions(100)) {
      encoded.add(mapper.readValue(mapper.writeValueAsString(transaction), String.class));
    }
    encoded.set(50, "0x");
    final String json = "{\"transactions\":" + mapper.writeValueAsString(encoded) + "}";

    assertThatThrownBy(() -> mapper.readValue(json, Holder.class))
        .isInstanceOfSatisfying(
            JsonMappingException.class,
            e -> assertThat(e.getPath().getFirst().getFieldName()).isEqualTo("transactions"));
  }

  private void assertDecodesInOrder(final int count) throws Exception {
    final List<Transaction> transactions = transactions(count);
    final String json = "{\"transactions\":" + mapper.writeValueAsString(transactions) + "}";

    final Holder holder = mapper.readValue(json, Holder.class);

    assertThat(holder.transactions)
        .extracting(Transaction::getHash)
        .containsExactlyElementsOf(transactions.stream().map(Transaction::getHash).toList());
  }

  private List<Transaction> transactions(final int count) {
    return IntStream.range(0, count).mapToObj(i -> gen.transaction()).toList();
  }

  static class Holder {
    @JsonDeserialize(using = TransactionJson.BlockBodyListDeserializer.class)
    public List<Transaction> transactions;
  }
}
