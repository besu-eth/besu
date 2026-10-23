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
package org.hyperledger.besu.ethereum.api.graphql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.TransactionAdapter;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * An EIP-8141 frame transaction has no outer signature. The schema declares {@code r} and {@code s}
 * as non-null {@code BigInt!}, so they resolve to zero; {@code v} and {@code yParity} are nullable
 * and resolve to null.
 */
class FrameTransactionGraphQLTest {

  private static final Address SENDER =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  private static final String QUERY =
      "{ transaction(hash: \"%s\") { hash type nonce from { address } r s v yParity } }";

  private static Transaction frameTransaction() {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(7)
        .nonceKeys(List.of(UInt256.valueOf(0xbeef)))
        .sender(SENDER)
        .frames(
            List.of(
                new Frame(
                    Frame.MODE_VERIFY, 3, Optional.empty(), 50_000L, 0L, Wei.ZERO, Bytes.EMPTY)))
        .frameSignatures(
            List.of(
                new FrameSignature(
                    FrameSignature.SCHEME_SECP256K1,
                    Bytes.EMPTY,
                    Bytes.EMPTY,
                    Bytes.repeat((byte) 7, 65))))
        .maxPriorityFeePerGas(Wei.of(2))
        .maxFeePerGas(Wei.of(100))
        .maxFeePerBlobGas(Wei.ZERO)
        .build();
  }

  @Test
  void adapterReportsZeroForTheMissingOuterSignature() {
    final TransactionAdapter adapter =
        new TransactionAdapter(new TransactionWithMetadata(frameTransaction()));

    assertThat(adapter.getR()).isEqualTo(BigInteger.ZERO);
    assertThat(adapter.getS()).isEqualTo(BigInteger.ZERO);
    assertThat(adapter.getV()).isEmpty();
    assertThat(adapter.getYParity()).isEmpty();
    assertThat(adapter.getType()).contains(0x06);
  }

  @Test
  void adapterReportsTheSignatureOfSignedTransactions() {
    final BlockDataGenerator generator = new BlockDataGenerator();
    for (final TransactionType type :
        List.of(
            TransactionType.FRONTIER,
            TransactionType.ACCESS_LIST,
            TransactionType.EIP1559,
            TransactionType.BLOB)) {
      final Transaction transaction = generator.transaction(type);
      final TransactionAdapter adapter =
          new TransactionAdapter(new TransactionWithMetadata(transaction));

      assertThat(adapter.getR()).isEqualTo(transaction.getR()).isNotEqualTo(BigInteger.ZERO);
      assertThat(adapter.getS()).isEqualTo(transaction.getS()).isNotEqualTo(BigInteger.ZERO);
    }
  }

  /** The type used to be the enum ordinal, which equals the type byte for every type but FRAME. */
  @ParameterizedTest
  @EnumSource(
      value = TransactionType.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"FRAME"})
  void typeIsUnchangedForEveryTypeBeforeFrame(final TransactionType type) {
    final Transaction transaction = mock(Transaction.class);
    when(transaction.getType()).thenReturn(type);

    assertThat(new TransactionAdapter(new TransactionWithMetadata(transaction)).getType())
        .contains(type.ordinal());
  }

  @Test
  void frameTransactionResolvesAgainstTheSchema() throws Exception {
    final Transaction transaction = frameTransaction();
    final BlockchainQueries query = mock(BlockchainQueries.class);
    when(query.transactionByHash(any()))
        .thenReturn(Optional.of(new TransactionWithMetadata(transaction)));

    final GraphQL graphQL = GraphQLProvider.buildGraphQL(new GraphQLDataFetchers(Set.of()));
    final ExecutionResult result =
        graphQL.execute(
            ExecutionInput.newExecutionInput()
                .query(QUERY.formatted(transaction.getHash().toHexString()))
                .graphQLContext(Map.of(GraphQLContextType.BLOCKCHAIN_QUERIES, query))
                .build());

    // A null for a non-null field would be reported here and null out the whole transaction.
    assertThat(result.getErrors()).isEmpty();
    final Map<String, Object> data = result.getData();
    @SuppressWarnings("unchecked")
    final Map<String, Object> rendered = (Map<String, Object>) data.get("transaction");
    assertThat(rendered)
        .containsEntry("hash", transaction.getHash().toHexString())
        .containsEntry("type", "0x6")
        .containsEntry("nonce", "0x7")
        .containsEntry("from", Map.of("address", SENDER.toHexString()))
        .containsEntry("r", "0x0")
        .containsEntry("s", "0x0")
        .containsEntry("v", null)
        .containsEntry("yParity", null);
  }
}
