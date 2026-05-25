/*
 * Copyright contributors to Hyperledger Besu.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EthGetRawTransactionByHashTest {

  // EIP-7702 Set Code Transaction
  private static final String VALID_TRANSACTION =
      "0xb8c404f8c1018080078307a12094a94f5374fce5edbc8e2a8697c15331677e6ebf0b8080c0f85cf85a809400000000000000000000000000000000000010008080a0dbcff17ff6c249f13b334fa86bcbaa1afd9f566ca9b06e4ea5fab9bdde9a9202a05c34c9d8af5b20e4a425fc1daf2d9d484576857eaf1629145b4686bac733868e01a0d61673cd58ffa5fc605c3215aa4647fa3afbea1d1f577e08402442992526d980a0063068ca818025c7b8493d0623cb70ef3a2ba4b3e2ae0af1146d1c9b065c0aff";
  private static final String JSON_RPC_VERSION = "2.0";
  private static final String ETH_METHOD = "eth_getRawTransactionByHash";

  @Mock private BlockchainQueries blockchainQueries;

  private EthGetRawTransactionByHash method;

  @BeforeEach
  void setUp() {
    method = new EthGetRawTransactionByHash(blockchainQueries);
  }

  @Test
  void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  void shouldThrowIfTransactionHashParameterIsInvalid() {
    final JsonRpcRequest request =
        new JsonRpcRequest(JSON_RPC_VERSION, method.getName(), new Object[] {"invalid"});

    assertThatThrownBy(() -> method.response(new JsonRpcRequestContext(request)))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid transaction hash parameter");
  }

  @Test
  void shouldReturnNullResultWhenTransactionDoesNotExist() {
    final String transactionHash =
        "0xf9ef5f0cf02685711cdf687b72d4754901729b942f4ea7f956e7fb206cae2f9e";
    when(blockchainQueries.transactionByHash(Hash.fromHexString(transactionHash)))
        .thenReturn(Optional.empty());

    final JsonRpcRequest request =
        new JsonRpcRequest(JSON_RPC_VERSION, method.getName(), new Object[] {transactionHash});

    final JsonRpcResponse actualResponse = method.response(new JsonRpcRequestContext(request));

    assertThat(actualResponse)
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(request.getId(), null));
  }

  @Test
  void shouldReturnRlpEncodedTransactionWhenTransactionExists() {
    final org.hyperledger.besu.ethereum.core.Transaction transaction =
        org.hyperledger.besu.ethereum.core.Transaction.readFrom(
            Bytes.fromHexString(VALID_TRANSACTION));
    final TransactionWithMetadata transactionWithMetadata =
        new TransactionWithMetadata(transaction, 1, Optional.empty(), Hash.ZERO, 0, 0L);

    when(blockchainQueries.transactionByHash(transaction.getHash()))
        .thenReturn(Optional.of(transactionWithMetadata));

    final JsonRpcRequest request =
        new JsonRpcRequest(
            JSON_RPC_VERSION,
            method.getName(),
            new Object[] {transaction.getHash().getBytes().toHexString()});

    final JsonRpcResponse actualResponse = method.response(new JsonRpcRequestContext(request));

    assertThat(actualResponse)
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(request.getId(), VALID_TRANSACTION));
  }
}
