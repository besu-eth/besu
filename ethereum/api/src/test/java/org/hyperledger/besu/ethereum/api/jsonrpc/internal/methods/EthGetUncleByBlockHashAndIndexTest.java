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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResult;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EthGetUncleByBlockHashAndIndexTest {

  private final BlockHeaderTestFixture blockHeaderTestFixture = new BlockHeaderTestFixture();
  private final TransactionTestFixture transactionTestFixture = new TransactionTestFixture();

  private EthGetUncleByBlockHashAndIndex method;
  private final Hash zeroHash = Hash.ZERO;

  @Mock private BlockchainQueries blockchainQueries;

  @BeforeEach
  public void before() {
    this.method = new EthGetUncleByBlockHashAndIndex(blockchainQueries);
  }

  @Test
  public void methodShouldReturnExpectedName() {
    assertThat(method.getName()).isEqualTo("eth_getUncleByBlockHashAndIndex");
  }

  @Test
  public void shouldReturnErrorWhenMissingBlockHashParam() {
    final JsonRpcRequestContext request = getUncleByBlockHashAndIndex(new Object[] {});

    final Throwable thrown = catchThrowable(() -> method.response(request));

    assertThat(thrown)
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage("Invalid block hash parameter (index 0)");
  }

  @Test
  public void shouldReturnErrorWhenMissingIndexParam() {
    final JsonRpcRequestContext request = getUncleByBlockHashAndIndex(new Object[] {zeroHash});

    final Throwable thrown = catchThrowable(() -> method.response(request));

    assertThat(thrown)
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage("Invalid block index parameter (index 1)");
  }

  @Test
  public void shouldReturnErrorWhenInvalidBlockHashParam() {
    final JsonRpcRequestContext request = getUncleByBlockHashAndIndex(new Object[] {"not-a-hash"});

    final Throwable thrown = catchThrowable(() -> method.response(request));

    assertThat(thrown)
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid block hash parameter (index 0)");
  }

  @Test
  public void shouldReturnErrorWhenInvalidIndexParam() {
    final JsonRpcRequestContext request =
        getUncleByBlockHashAndIndex(new Object[] {zeroHash, "not-an-index"});

    final Throwable thrown = catchThrowable(() -> method.response(request));

    assertThat(thrown)
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid block index parameter (index 1)");
  }

  @Test
  public void shouldReturnNullResultWhenBlockDoesNotHaveOmmer() {
    final JsonRpcRequestContext request =
        getUncleByBlockHashAndIndex(new Object[] {zeroHash, "0x0"});
    final JsonRpcResponse expectedResponse = new JsonRpcSuccessResponse(null, null);

    when(blockchainQueries.getOmmer(eq(zeroHash), eq(0))).thenReturn(Optional.empty());

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnExpectedBlockResult() {
    final JsonRpcRequestContext request =
        getUncleByBlockHashAndIndex(new Object[] {zeroHash, "0x0"});
    final BlockHeader header = blockHeaderTestFixture.buildHeader();
    final BlockResult expectedBlockResult = blockResult(header);
    final JsonRpcResponse expectedResponse = new JsonRpcSuccessResponse(null, expectedBlockResult);

    when(blockchainQueries.getOmmer(eq(zeroHash), eq(0))).thenReturn(Optional.of(header));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  private BlockResult blockResult(final BlockHeader header) {
    final Block block =
        new Block(header, new BlockBody(Collections.emptyList(), Collections.emptyList()));
    return new BlockResult(
        header, Collections.emptyList(), Collections.emptyList(), block.getSize());
  }

  private JsonRpcRequestContext getUncleByBlockHashAndIndex(final Object[] params) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "eth_getUncleByBlockHashAndIndex", params));
  }

  public BlockWithMetadata<TransactionWithMetadata, Hash> blockWithMetadata(
      final BlockHeader header) {
    final KeyPair keyPair = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    final List<TransactionWithMetadata> transactions = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final Transaction transaction = transactionTestFixture.createTransaction(keyPair);
      transactions.add(
          new TransactionWithMetadata(
              transaction,
              header.getNumber(),
              header.getBaseFee(),
              header.getHash(),
              0,
              header.getTimestamp()));
    }

    final List<Hash> ommers = new ArrayList<>();
    ommers.add(Hash.ZERO);

    return new BlockWithMetadata<>(header, transactions, ommers, header.getDifficulty(), 0);
  }
}
