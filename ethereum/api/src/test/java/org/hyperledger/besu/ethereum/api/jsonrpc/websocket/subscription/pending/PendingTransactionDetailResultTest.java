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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.pending;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionBaseResult;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

class PendingTransactionDetailResultTest {

  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void shouldSerializeLikeTransactionBaseResultForAllTransactionTypes() {
    for (final TransactionType transactionType :
        List.of(
            TransactionType.FRONTIER,
            TransactionType.ACCESS_LIST,
            TransactionType.EIP1559,
            TransactionType.BLOB,
            TransactionType.DELEGATE_CODE)) {
      final Transaction transaction = createTransaction(transactionType);
      final JsonNode pendingTransactionJson =
          objectMapper.valueToTree(new PendingTransactionDetailResult(transaction));
      final JsonNode transactionBaseJson = nonNullFields(new TransactionBaseResult(transaction));

      assertThat(pendingTransactionJson).as(transactionType.name()).isEqualTo(transactionBaseJson);
    }
  }

  private JsonNode nonNullFields(final TransactionBaseResult result) {
    final ObjectNode resultJson = objectMapper.valueToTree(result);
    final Iterator<Map.Entry<String, JsonNode>> fields = resultJson.fields();
    while (fields.hasNext()) {
      if (fields.next().getValue().isNull()) {
        fields.remove();
      }
    }
    return resultJson;
  }

  private Transaction createTransaction(final TransactionType transactionType) {
    final TransactionTestFixture fixture = new TransactionTestFixture().type(transactionType);
    if (transactionType == TransactionType.BLOB) {
      fixture.versionedHashes(Optional.of(List.of(VersionedHash.DEFAULT_VERSIONED_HASH)));
    }
    return fixture.createTransaction(KEY_PAIR);
  }
}
