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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class DebugGetRawTransactionTest {

  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  private BlockchainQueries blockchainQueries;
  private DebugGetRawTransaction method;

  @BeforeEach
  public void setUp() {
    blockchainQueries = mock(BlockchainQueries.class);
    method = new DebugGetRawTransaction(blockchainQueries);
  }

  @Test
  public void returnsNullForUnknownTransaction() {
    final Hash txHash = Hash.fromHexStringLenient("0x1234");
    when(blockchainQueries.transactionByHash(txHash)).thenReturn(Optional.empty());

    final JsonRpcRequestContext request = requestFor(txHash.toHexString());
    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    assertThat(response.getResult()).isNull();
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionType.class,
      names = {"FRONTIER", "ACCESS_LIST", "EIP1559", "DELEGATE_CODE"})
  public void rawBytesHashMatchesTxHashForAllTypes(final TransactionType type) {
    final Transaction tx = new TransactionTestFixture().type(type).createTransaction(KEY_PAIR);
    mockTransaction(tx);

    final JsonRpcRequestContext request = requestFor(tx.getHash().toHexString());
    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    final Bytes raw = Bytes.fromHexString((String) response.getResult());
    assertThat(Hash.hash(raw))
        .as("keccak256(raw) must equal txHash for type %s", type)
        .isEqualTo(tx.getHash());
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionType.class,
      names = {"ACCESS_LIST", "EIP1559", "DELEGATE_CODE"})
  public void typedTransactionRawStartsWithTypeByte(final TransactionType type) {
    final Transaction tx = new TransactionTestFixture().type(type).createTransaction(KEY_PAIR);
    mockTransaction(tx);

    final JsonRpcRequestContext request = requestFor(tx.getHash().toHexString());
    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    final Bytes raw = Bytes.fromHexString((String) response.getResult());
    assertThat(raw.get(0))
        .as("first byte must be the EIP-2718 type byte for type %s", type)
        .isEqualTo(type.getSerializedType());
  }

  @Test
  public void blobTransactionRawBytesHashMatchesTxHash() {
    final Transaction tx =
        new TransactionTestFixture()
            .type(TransactionType.BLOB)
            .versionedHashes(Optional.of(List.of(VersionedHash.DEFAULT_VERSIONED_HASH)))
            .createTransaction(KEY_PAIR);
    mockTransaction(tx);

    final JsonRpcRequestContext request = requestFor(tx.getHash().toHexString());
    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    final Bytes raw = Bytes.fromHexString((String) response.getResult());
    assertThat(Hash.hash(raw))
        .as("keccak256(raw) must equal txHash for BLOB type")
        .isEqualTo(tx.getHash());
    assertThat(raw.get(0))
        .as("first byte must be the EIP-2718 type byte (0x03) for BLOB")
        .isEqualTo(TransactionType.BLOB.getSerializedType());
  }

  private void mockTransaction(final Transaction tx) {
    final TransactionWithMetadata txWithMeta = mock(TransactionWithMetadata.class);
    when(txWithMeta.getTransaction()).thenReturn(tx);
    when(blockchainQueries.transactionByHash(tx.getHash())).thenReturn(Optional.of(txWithMeta));
  }

  private JsonRpcRequestContext requestFor(final String txHash) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "debug_getRawTransaction", new Object[] {txHash}));
  }
}
