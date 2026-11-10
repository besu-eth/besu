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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.BaseJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import io.opentelemetry.api.trace.Span;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

class JsonRpcAddressParameterTest {

  private static final String ADDRESS = "0x6295eE1B4F6dD65047762F924Ecd367c17eaBf8f";
  private static final Map<String, String> BALANCE_OVERRIDE = Map.of("balance", "0x1");

  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final TransactionSimulator transactionSimulator = mock(TransactionSimulator.class);
  private final TransactionPool transactionPool = mock(TransactionPool.class);
  private final BaseJsonRpcProcessor processor = new BaseJsonRpcProcessor();

  @BeforeEach
  void setUp() {
    final Blockchain blockchain = mock(Blockchain.class);
    final ChainHead chainHead = mock(ChainHead.class);
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);
    when(blockHeader.getBlockHash()).thenReturn(Hash.ZERO);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ZERO));
  }

  static Stream<Arguments> malformedAddresses() {
    return Stream.of(
            "eth_getBalance",
            "eth_getCode",
            "eth_getTransactionCount",
            "eth_getStorageAt",
            "eth_getProof",
            "eth_call.from",
            "eth_call.to",
            "eth_call.stateOverride",
            "eth_estimateGas.stateOverride")
        .flatMap(
            position ->
                Stream.of(
                        "",
                        "0x",
                        "0x0",
                        "0x123",
                        "0x1234",
                        "0x" + "1".repeat(36),
                        "0x" + "1".repeat(37),
                        "0x" + "1".repeat(39),
                        "0x" + "1".repeat(41),
                        "0x" + "1".repeat(42),
                        "0x" + "g".repeat(40),
                        "1".repeat(40),
                        "x" + "1".repeat(40),
                        " " + ADDRESS,
                        1,
                        true)
                    .map(address -> Arguments.of(position, address)));
  }

  @ParameterizedTest
  @MethodSource("malformedAddresses")
  void shouldRejectMalformedAddressBeforeAccessingState(
      final String position, final Object address) {
    final JsonRpcResponse response = process(position, address);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getError().getCode()).isEqualTo(-32602);
    // eth_estimateGas on the pending block reads the parameters before the chain head
    verify(blockchainQueries, atLeast(0)).getBlockchain();
    verifyNoMoreInteractions(blockchainQueries);
    verifyNoInteractions(transactionSimulator, transactionPool);
  }

  @Test
  void shouldQueryStateForFullAddress() {
    when(blockchainQueries.accountBalance(Address.fromHexString(ADDRESS), Hash.ZERO))
        .thenReturn(Optional.of(Wei.of(1)));

    final JsonRpcResponse response = process("eth_getBalance", ADDRESS);

    assertThat(response)
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(null, "0x1"));
  }

  @Test
  void shouldQueryStateForFullAddressWithUpperCasePrefix() {
    when(blockchainQueries.accountBalance(Address.fromHexString(ADDRESS), Hash.ZERO))
        .thenReturn(Optional.of(Wei.of(1)));

    final JsonRpcResponse response = process("eth_getBalance", "0X" + ADDRESS.substring(2));

    assertThat(response)
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(null, "0x1"));
  }

  @ParameterizedTest
  @MethodSource("fullAddresses")
  @SuppressWarnings("unchecked")
  void shouldCallWithFullStateOverrideAccountKey(final String address) {
    final JsonRpcResponse expected = new JsonRpcSuccessResponse(null, "0x");
    when(transactionSimulator.process(any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(expected));

    final JsonRpcResponse response = process("eth_call.stateOverride", address);

    assertThat(response).isSameAs(expected);
    final ArgumentCaptor<Optional<StateOverrideMap>> overrides =
        ArgumentCaptor.forClass(Optional.class);
    verify(transactionSimulator).process(any(), overrides.capture(), any(), any(), any(), any());
    assertThat(overrides.getValue()).isPresent();
    assertThat(overrides.getValue().get()).containsOnlyKeys(Address.fromHexString(ADDRESS));
  }

  static Stream<String> fullAddresses() {
    return Stream.of(ADDRESS, "0X" + ADDRESS.substring(2));
  }

  @Test
  void shouldCallWithFullAddresses() {
    final JsonRpcResponse expected = new JsonRpcSuccessResponse(null, "0x");
    when(transactionSimulator.process(any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(expected));

    final JsonRpcResponse response = process("eth_call.to", ADDRESS);

    assertThat(response).isSameAs(expected);
    final ArgumentCaptor<CallParameter> callParameter =
        ArgumentCaptor.forClass(CallParameter.class);
    verify(transactionSimulator)
        .process(callParameter.capture(), any(), any(), any(), any(), any());
    assertThat(callParameter.getValue().getSender()).contains(Address.fromHexString(ADDRESS));
    assertThat(callParameter.getValue().getTo()).contains(Address.fromHexString(ADDRESS));
  }

  private JsonRpcResponse process(final String position, final Object address) {
    final JsonRpcMethod method =
        switch (position) {
          case "eth_getBalance" -> new EthGetBalance(blockchainQueries);
          case "eth_getCode" -> new EthGetCode(blockchainQueries);
          case "eth_getTransactionCount" ->
              new EthGetTransactionCount(blockchainQueries, transactionPool);
          case "eth_getStorageAt" -> new EthGetStorageAt(blockchainQueries);
          case "eth_getProof" -> new EthGetProof(blockchainQueries);
          case "eth_estimateGas.stateOverride" ->
              new EthEstimateGas(
                  blockchainQueries,
                  transactionSimulator,
                  ImmutableApiConfiguration.builder().build());
          default -> new EthCall(blockchainQueries, transactionSimulator, new NoOpMetricsSystem());
        };
    return processor.process(
        new JsonRpcRequestId(1), method, Span.getInvalid(), request(position, address));
  }

  private static JsonRpcRequestContext request(final String position, final Object address) {
    final Object[] params =
        switch (position) {
          case "eth_getBalance", "eth_getCode", "eth_getTransactionCount" ->
              new Object[] {address, "latest"};
          case "eth_getStorageAt" -> new Object[] {address, "0x0", "latest"};
          case "eth_getProof" -> new Object[] {address, List.of(), "latest"};
          case "eth_call.from" -> new Object[] {Map.of("from", address, "to", ADDRESS), "latest"};
          case "eth_call.stateOverride" ->
              new Object[] {
                Map.of("from", ADDRESS, "to", ADDRESS), "latest", Map.of(address, BALANCE_OVERRIDE)
              };
          case "eth_estimateGas.stateOverride" ->
              new Object[] {
                Map.of("from", ADDRESS, "to", ADDRESS), "pending", Map.of(address, BALANCE_OVERRIDE)
              };
          default -> new Object[] {Map.of("from", ADDRESS, "to", address), "latest"};
        };
    final int dot = position.indexOf('.');
    final String method = dot < 0 ? position : position.substring(0, dot);
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", method, params));
  }
}
