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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class TransactionPendingResultTest {
  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  @Test
  void frameTransactionRendersWithoutPublicKey() {
    final Transaction frameTx =
        new TransactionTestFixture().type(TransactionType.FRAME).createTransaction(KEY_PAIR);

    final TransactionPendingResult result = new TransactionPendingResult(frameTx);
    final JsonNode json = new ObjectMapper().valueToTree(result);

    assertThat(result.getPublicKey()).isNull();
    assertThat(json.has("publicKey")).isFalse();
    assertThat(json.get("from").asText()).isEqualTo(frameTx.getSender().toString());
    assertThat(json.get("raw").asText())
        .isEqualTo(
            TransactionEncoder.encodeOpaqueBytes(frameTx, EncodingContext.POOLED_TRANSACTION)
                .toHexString());
  }

  @Test
  void signedTransactionRendersTheRecoveredPublicKey() {
    final Transaction eip1559Tx =
        new TransactionTestFixture().type(TransactionType.EIP1559).createTransaction(KEY_PAIR);

    final TransactionPendingResult result = new TransactionPendingResult(eip1559Tx);

    assertThat(result.getPublicKey()).isEqualTo(KEY_PAIR.getPublicKey().toString());
  }
}
