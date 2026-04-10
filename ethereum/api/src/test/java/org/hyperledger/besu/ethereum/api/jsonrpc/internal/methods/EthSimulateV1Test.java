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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.SimulateV1Parameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.transaction.BlockSimulationParameter;
import org.hyperledger.besu.ethereum.transaction.BlockSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallException;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EthSimulateV1Test {

  private EthSimulateV1 method;

  @Mock private BlockchainQueries blockchainQueries;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private TransactionSimulator transactionSimulator;
  @Mock private MiningConfiguration miningConfiguration;
  @Mock private ApiConfiguration apiConfiguration;
  @Mock private BlockSimulator blockSimulator;
  @Mock private Blockchain blockchain;
  @Mock private BlockHeader blockHeader;
  @Mock private org.hyperledger.besu.ethereum.chain.ChainHead chainHead;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private PrecompileContractRegistry precompileContractRegistry;

  @BeforeEach
  public void setUp() {
    method =
        new EthSimulateV1(
            null,
            blockchainQueries,
            protocolSchedule,
            transactionSimulator,
            miningConfiguration,
            apiConfiguration);
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("eth_simulateV1");
  }

  @Test
  public void shouldReturnBlockNotFoundErrorWhenFutureBlockNumberSpecified() {
    final JsonRpcRequestContext request =
        ethSimulateV1Request(simulateParameter(false), Quantity.create(33L));
    when(blockchainQueries.headBlockNumber()).thenReturn(14L);

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getError().getCode())
        .isEqualTo(RpcErrorType.BLOCK_NOT_FOUND.getCode());

    verify(blockchainQueries).headBlockNumber();
  }

  @Test
  public void shouldReturnInvalidParamsWhenUpfrontCostExceedsBalanceWithValidation() {
    setupMethodWithMockSimulator();
    setupBlockchainForLatest();
    when(blockSimulator.process(any(BlockHeader.class), any(), any(OperationTracer.class)))
        .thenThrow(
            new BlockStateCallException(
                "Upfront cost exceeds balance", BlockStateCallError.UPFRONT_COST_EXCEEDS_BALANCE));

    final JsonRpcRequestContext request = ethSimulateV1Request(simulateParameter(true), "latest");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getError().getCode())
        .isEqualTo(RpcErrorType.INVALID_PARAMS.getCode());
  }

  @Test
  public void shouldReturnOriginalErrorCodeWhenUpfrontCostExceedsBalanceWithoutValidation() {
    setupMethodWithMockSimulator();
    setupBlockchainForLatest();
    when(blockSimulator.process(any(BlockHeader.class), any(), any(OperationTracer.class)))
        .thenThrow(
            new BlockStateCallException(
                "Upfront cost exceeds balance", BlockStateCallError.UPFRONT_COST_EXCEEDS_BALANCE));

    final JsonRpcRequestContext request = ethSimulateV1Request(simulateParameter(false), "latest");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getError().getCode())
        .isEqualTo(BlockStateCallError.UPFRONT_COST_EXCEEDS_BALANCE.getCode());
  }

  @Test
  public void shouldNotReturnInvalidParamsWhenInputAndDataHaveDifferentValues() {
    setupMethodWithMockSimulator();
    setupBlockchainForLatest();
    when(blockSimulator.process(any(BlockHeader.class), any(), any(OperationTracer.class)))
        .thenReturn(List.of());

    // Reproduces issue #9960: both input and data provided with different values.
    // Other EL clients (Geth, Nethermind, Reth, Erigon) accept this and use input.
    final Map<String, Object> callObj =
        Map.of(
            "from", "0xc000000000000000000000000000000000000000",
            "to", "0xd000000000000000000000000000000000000000",
            "input", "0xDEADBEEF",
            "data", "0xCAFEBABE");
    final Map<String, Object> simulateParam =
        Map.of("blockStateCalls", List.of(Map.of("calls", List.of(callObj))), "validation", false);

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "eth_simulateV1", new Object[] {simulateParam, "latest"}));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isNotInstanceOf(JsonRpcErrorResponse.class);

    final ArgumentCaptor<BlockSimulationParameter> captor =
        ArgumentCaptor.forClass(BlockSimulationParameter.class);
    verify(blockSimulator)
        .process(any(BlockHeader.class), captor.capture(), any(OperationTracer.class));
    final Bytes payload =
        captor.getValue().getBlockStateCalls().get(0).getCalls().get(0).getPayload().orElseThrow();
    assertThat(payload).isEqualTo(Bytes.fromHexString("0xDEADBEEF"));
  }

  @Test
  public void shouldReturnInvalidParamsWhenParameterParsingFails() {
    setupMethodWithMockSimulator();
    setupBlockchainForLatest();

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "eth_simulateV1", new Object[] {"invalid-param", "latest"}));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getError().getCode())
        .isEqualTo(RpcErrorType.INVALID_PARAMS.getCode());
  }

  private void setupMethodWithMockSimulator() {
    method = new EthSimulateV1(blockchainQueries, protocolSchedule, blockSimulator);
  }

  private void setupBlockchainForLatest() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(protocolSpec.getPrecompileContractRegistry()).thenReturn(precompileContractRegistry);
    when(precompileContractRegistry.getPrecompileAddresses()).thenReturn(Set.of());
  }

  private SimulateV1Parameter simulateParameter(final boolean validation) {
    return new SimulateV1Parameter(List.of(), validation, false, false, false, false);
  }

  private JsonRpcRequestContext ethSimulateV1Request(
      final SimulateV1Parameter simulateV1Parameter, final String blockNumberInHex) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0", "eth_simulateV1", new Object[] {simulateV1Parameter, blockNumberInHex}));
  }
}
