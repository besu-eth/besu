/*
 * Copyright ConsenSys AG.
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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TransactionWithMetadataResultTest {

  @Test
  public void eip1559TransactionWithShortWeiVals() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    TransactionWithMetadataResult zeroPriorityFeeTx =
        new TransactionWithMetadataResult(
            new TransactionWithMetadata(
                new TransactionTestFixture()
                    .type(TransactionType.EIP1559)
                    .maxFeePerGas(Optional.of(Wei.ONE))
                    .maxPriorityFeePerGas(Optional.of(Wei.ZERO))
                    .createTransaction(gen.generateKeyPair()),
                0L,
                Optional.of(Wei.of(7L)),
                Hash.ZERO,
                0,
                0L));

    assertThat(zeroPriorityFeeTx.getMaxFeePerGas()).isEqualTo("0x1");
    assertThat(zeroPriorityFeeTx.getMaxPriorityFeePerGas()).isEqualTo("0x0");
  }

  @Test
  public void eip1559TransactionFields() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Transaction transaction = gen.transaction(TransactionType.EIP1559);
    TransactionWithMetadataResult tcr =
        new TransactionWithMetadataResult(
            new TransactionWithMetadata(
                transaction, 0L, Optional.of(Wei.of(7L)), Hash.ZERO, 0, 0L));
    assertThat(tcr.getMaxFeePerGas()).isNotEmpty();
    assertThat(tcr.getMaxPriorityFeePerGas()).isNotEmpty();
    assertThat(tcr.getGasPrice()).isNotEmpty();
    assertThat(tcr.getGasPrice())
        .isEqualTo(Quantity.create(transaction.getEffectiveGasPrice(Optional.of(Wei.of(7L)))));
  }

  @Test
  public void legacyTransactionPostLondonFields() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Transaction transaction = gen.transaction(TransactionType.FRONTIER);
    TransactionWithMetadataResult tcr =
        new TransactionWithMetadataResult(
            new TransactionWithMetadata(
                transaction, 0L, Optional.of(Wei.of(7L)), Hash.ZERO, 0, 0L));
    assertThat(tcr.getMaxFeePerGas()).isNull();
    assertThat(tcr.getMaxPriorityFeePerGas()).isNull();
    assertThat(tcr.getGasPrice()).isNotEmpty();
    assertThat(tcr.getGasPrice())
        .isEqualTo(Quantity.create(transaction.getGasPrice().orElseThrow()));
  }

  @Test
  public void legacyTransactionPreLondonFields() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Transaction transaction = gen.transaction(TransactionType.FRONTIER);
    TransactionWithMetadataResult tcr =
        new TransactionWithMetadataResult(
            new TransactionWithMetadata(transaction, 0L, Optional.empty(), Hash.ZERO, 0, 0L));
    assertThat(tcr.getMaxFeePerGas()).isNull();
    assertThat(tcr.getMaxPriorityFeePerGas()).isNull();
    assertThat(tcr.getGasPrice()).isNotEmpty();
    assertThat(tcr.getGasPrice())
        .isEqualTo(Quantity.create(transaction.getGasPrice().orElseThrow()));
  }

  @Test
  public void accessListTransactionFields() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Transaction transaction = gen.transaction(TransactionType.ACCESS_LIST);
    final TransactionWithMetadataResult transactionWithMetadataResult =
        new TransactionWithMetadataResult(
            new TransactionWithMetadata(
                transaction,
                136,
                Optional.empty(),
                Hash.fromHexString(
                    "0xfc84c3946cb419cbd8c2c68d5e79a3b2a03a8faff4d9e2be493f5a07eb5da95e"),
                0,
                0L));

    assertThat(transactionWithMetadataResult.getGasPrice()).isNotEmpty();
    assertThat(transactionWithMetadataResult.getMaxFeePerGas()).isNull();
    assertThat(transactionWithMetadataResult.getMaxPriorityFeePerGas()).isNull();
    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonNode transactionCompleteResultJson =
        objectMapper.valueToTree(transactionWithMetadataResult);
    final List<JsonNode> accessListJson =
        ImmutableList.copyOf(transactionCompleteResultJson.get("accessList").elements());
    assertThat(accessListJson).hasSizeGreaterThan(0);
    accessListJson.forEach(
        accessListEntryJson -> {
          assertThat(accessListEntryJson.get("address").asText()).matches("^0x\\X{40}$");
          ImmutableList.copyOf(accessListEntryJson.get("storageKeys").elements())
              .forEach(
                  storageKeyJson -> assertThat(storageKeyJson.asText()).matches("^0x\\X{64}$"));
        });
  }
}
