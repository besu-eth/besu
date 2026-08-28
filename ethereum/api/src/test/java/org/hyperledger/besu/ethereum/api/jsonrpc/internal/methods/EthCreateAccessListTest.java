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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CreateAccessListResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.tracing.AccessListOperationTracer;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EthCreateAccessListTest {
  private static final long MIN_TX_GAS_COST = 21_000L;
  private static final long TX_GAS_LIMIT_CAP = 1_000_000L;
  private static final long BLOCK_GAS_LIMIT = 2_000_000L;
  private static final String METHOD = "eth_createAccessList";

  private EthCreateAccessList method;

  @Mock private BlockHeader latestBlockHeader;
  @Mock private BlockHeader finalizedBlockHeader;
  @Mock private BlockHeader genesisBlockHeader;
  @Mock private BlockHeader pendingBlockHeader;
  @Mock private Blockchain blockchain;
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private TransactionSimulator transactionSimulator;
  @Mock private WorldStateArchive worldStateArchive;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private PrecompileContractRegistry precompileContractRegistry;

  @BeforeEach
  public void setUp() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchainQueries.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(blockchainQueries.headBlockNumber()).thenReturn(2L);
    when(blockchainQueries.getBlockHeaderByNumber(0L)).thenReturn(Optional.of(genesisBlockHeader));
    when(blockchainQueries.finalizedBlockHeader()).thenReturn(Optional.of(finalizedBlockHeader));
    when(blockchainQueries.getBlockHeaderByNumber(1L))
        .thenReturn(Optional.of(finalizedBlockHeader));
    when(blockchainQueries.getMinimumTransactionCost(any())).thenReturn(MIN_TX_GAS_COST);
    when(blockchainQueries.accountBalance(any(), any())).thenReturn(Optional.of(Wei.MAX_WEI));
    when(blockchainQueries.getTransactionGasLimitCap(any())).thenReturn(Long.MAX_VALUE);
    when(genesisBlockHeader.getGasLimit()).thenReturn(BLOCK_GAS_LIMIT);
    when(genesisBlockHeader.getNumber()).thenReturn(0L);
    when(finalizedBlockHeader.getGasLimit()).thenReturn(BLOCK_GAS_LIMIT);
    when(finalizedBlockHeader.getNumber()).thenReturn(1L);
    when(blockchain.getChainHeadHeader()).thenReturn(latestBlockHeader);
    when(latestBlockHeader.getGasLimit()).thenReturn(BLOCK_GAS_LIMIT);
    when(latestBlockHeader.getNumber()).thenReturn(2L);
    when(pendingBlockHeader.getGasLimit()).thenReturn(BLOCK_GAS_LIMIT);
    when(pendingBlockHeader.getNumber()).thenReturn(3L);
    when(transactionSimulator.simulatePendingBlockHeader()).thenReturn(pendingBlockHeader);
    when(worldStateArchive.isWorldStateAvailable(any(), any())).thenReturn(true);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(protocolSpec.getPrecompileContractRegistry()).thenReturn(precompileContractRegistry);
    when(precompileContractRegistry.getPrecompileAddresses()).thenReturn(Set.of());

    method = new EthCreateAccessList(blockchainQueries, transactionSimulator, protocolSchedule);
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(METHOD);
  }

  private JsonRpcRequestContext ethCreateAccessListRequest(final CallParameter callParameter) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", METHOD, new Object[] {callParameter}));
  }

  private JsonRpcRequestContext ethCreateAccessListRequest(
      final CallParameter callParameter, final String blockParam) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", METHOD, new Object[] {callParameter, blockParam}));
  }

  @Test
  public void shouldReturnGasEstimateWhenTransientLegacyTransactionProcessorReturnsResultSuccess() {
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(legacyTransactionCallParameter(Wei.ZERO));
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, new CreateAccessListResult(List.of(), MIN_TX_GAS_COST));

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  @Test
  public void shouldUseGasPriceParameterWhenIsPresent() {
    final Wei gasPrice = Wei.of(1000);
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(legacyTransactionCallParameter(gasPrice));
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, new CreateAccessListResult(List.of(), MIN_TX_GAS_COST));

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  @Test
  public void latestBlockTagEstimateOnLatestBlock() {
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(legacyTransactionCallParameter(Wei.ZERO), "latest");
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, latestBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, new CreateAccessListResult(List.of(), MIN_TX_GAS_COST));

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .process(any(), eq(Optional.empty()), any(), any(), eq(latestBlockHeader));
  }

  @Test
  public void shouldNotErrorWhenGasPricePresentForEip1559Transaction() {
    final Wei gasPrice = Wei.of(1000);
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(eip1559TransactionCallParameter(Optional.of(gasPrice)));
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, new CreateAccessListResult(List.of(), MIN_TX_GAS_COST));
    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  @Test
  public void shouldReturnErrorWhenWorldStateIsNotAvailable() {
    when(worldStateArchive.isWorldStateAvailable(any(), any())).thenReturn(false);
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(legacyTransactionCallParameter(Wei.ZERO), "latest");
    mockTransactionSimulatorResult(false, false, 1L, latestBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.WORLD_STATE_UNAVAILABLE);

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
    verifyNoInteractions(transactionSimulator);
  }

  @Test
  public void shouldReturnAccessListEvenWhenTransactionReverted() {
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(legacyTransactionCallParameter(Wei.ZERO));
    mockTransactionSimulatorResult(false, true, MIN_TX_GAS_COST, pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null,
            new CreateAccessListResult(
                List.of(), MIN_TX_GAS_COST, Optional.of("execution reverted")));

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(transactionSimulator, times(2))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  @Test
  public void shouldReturnEmptyAccessListIfNoParameterAndWithoutAccessedStorage() {
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, new CreateAccessListResult(List.of(), MIN_TX_GAS_COST));
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(eip1559TransactionCallParameter());
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, pendingBlockHeader);

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  @Test
  public void shouldReturnAccessListIfNoParameterAndWithAccessedStorage() {
    // Create a 1559 call without access lists
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(eip1559TransactionCallParameter());
    // Generate a random list with one access list entry
    final List<AccessListEntry> expectedAccessList = generateRandomAccessList();

    // expect a list with the mocked access list
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, new CreateAccessListResult(expectedAccessList, MIN_TX_GAS_COST));
    final AccessListOperationTracer tracer = createMockTracer(expectedAccessList);

    // Set TransactionSimulator.process response
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, pendingBlockHeader);
    assertThat(responseWithMockTracer(request, tracer))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  @Test
  public void shouldReturnEmptyAccessListIfNoAccessedStorage() {
    // Generate a random list with one access list entry
    final List<AccessListEntry> accessListParam = generateRandomAccessList();
    // expect empty list
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, new CreateAccessListResult(List.of(), MIN_TX_GAS_COST));
    // create a request using the accessListParam
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(eip1559TransactionCallParameter(accessListParam));

    // Set TransactionSimulator.process response
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, pendingBlockHeader);
    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  @Test
  public void shouldReturnAccessListIfParameterAndSameAccessedStorage() {
    // Generate a random list with one access list entry
    final List<AccessListEntry> expectedAccessList = generateRandomAccessList();
    // Create a 1559 call with the expected access list
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(eip1559TransactionCallParameter(expectedAccessList));

    // expect a list with the mocked access list
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, new CreateAccessListResult(expectedAccessList, MIN_TX_GAS_COST));
    final AccessListOperationTracer tracer = createMockTracer(expectedAccessList);

    // Set TransactionSimulator.process response
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, pendingBlockHeader);
    assertThat(responseWithMockTracer(request, tracer))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  @Test
  public void shouldReturnAccessListIfWithParameterAndDifferentAccessedStorage() {
    // Generate a random list with one access list entry
    final List<AccessListEntry> accessListParam = generateRandomAccessList();
    // Create a 1559 call with the accessListParam
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(eip1559TransactionCallParameter(accessListParam));

    // Generate a different random list with one access list entry
    final List<AccessListEntry> expectedAccessList = generateRandomAccessList();

    // expect a list with the mocked access list
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, new CreateAccessListResult(expectedAccessList, MIN_TX_GAS_COST));
    final AccessListOperationTracer tracer = createMockTracer(expectedAccessList);

    // Set TransactionSimulator.process response
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, pendingBlockHeader);
    assertThat(responseWithMockTracer(request, tracer))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  @Test
  public void shouldReturnAccessListWhenBlockTagParamIsPresent() {
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(eip1559TransactionCallParameter(), "finalized");
    // Generate a random list with one access list entry
    final List<AccessListEntry> expectedAccessList = generateRandomAccessList();

    // expect a list with the mocked access list
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, new CreateAccessListResult(expectedAccessList, MIN_TX_GAS_COST));
    final AccessListOperationTracer tracer = createMockTracer(expectedAccessList);

    // Set TransactionSimulator.process response
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, finalizedBlockHeader);
    assertThat(responseWithMockTracer(request, tracer))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .process(any(), eq(Optional.empty()), any(), any(), eq(finalizedBlockHeader));
  }

  @Test
  public void shouldReturnAccessListWhenBlockNumberParamIsPresent() {
    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(eip1559TransactionCallParameter(), "0x0");
    // Generate a random list with one access list entry
    final List<AccessListEntry> expectedAccessList = generateRandomAccessList();

    // expect a list with the mocked access list
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, new CreateAccessListResult(expectedAccessList, MIN_TX_GAS_COST));
    final AccessListOperationTracer tracer = createMockTracer(expectedAccessList);

    // Set TransactionSimulator.process response
    mockTransactionSimulatorResult(true, false, MIN_TX_GAS_COST, genesisBlockHeader);

    assertThat(responseWithMockTracer(request, tracer))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
    verify(transactionSimulator, times(1))
        .process(any(), eq(Optional.empty()), any(), any(), eq(genesisBlockHeader));
  }

  @Test
  public void shouldUseTxGasLimitCapIfLessThanBlockGasLimit() {
    when(blockchainQueries.getTransactionGasLimitCap(any())).thenReturn(TX_GAS_LIMIT_CAP);

    final JsonRpcRequestContext request =
        ethCreateAccessListRequest(
            eip1559TransactionCallParameter(Optional.empty(), null, Bytes.ofUnsignedLong(1L)));
    // Generate a random list with one access list entry
    final List<AccessListEntry> expectedAccessList = generateRandomAccessList();

    // expect a list with the mocked access list
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, new CreateAccessListResult(expectedAccessList, TX_GAS_LIMIT_CAP));
    final AccessListOperationTracer tracer = createMockTracer(expectedAccessList);

    mockTransactionSimulatorResult(true, false, TX_GAS_LIMIT_CAP, pendingBlockHeader);

    assertThat(responseWithMockTracer(request, tracer))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
    verify(transactionSimulator, times(2))
        .processOnPending(any(), eq(Optional.empty()), any(), any(), eq(pendingBlockHeader));
  }

  private JsonRpcResponse responseWithMockTracer(
      final JsonRpcRequestContext request, final AccessListOperationTracer tracer) {
    try (final MockedStatic<AccessListOperationTracer> tracerMockedStatic =
        Mockito.mockStatic(AccessListOperationTracer.class)) {
      tracerMockedStatic.when(AccessListOperationTracer::create).thenReturn(tracer);
      tracerMockedStatic.when(() -> AccessListOperationTracer.create(any())).thenReturn(tracer);
      return method.response(request);
    }
  }

  private AccessListOperationTracer createMockTracer(
      final List<AccessListEntry> accessListEntries) {
    final AccessListOperationTracer tracer = mock(AccessListOperationTracer.class);
    when(tracer.getAccessList()).thenReturn(accessListEntries);
    return tracer;
  }

  @SuppressWarnings("ReferenceEquality")
  private void mockTransactionSimulatorResult(
      final boolean isSuccessful,
      final boolean isReverted,
      final long estimateGas,
      final BlockHeader blockHeader) {
    final TransactionProcessingResult mockResult = mock(TransactionProcessingResult.class);
    when(mockResult.getRevertReason())
        .thenReturn(isReverted ? Optional.of(Bytes.of(0)) : Optional.empty());
    final TransactionSimulatorResult mockTxSimResult = mock(TransactionSimulatorResult.class);
    when(mockTxSimResult.result()).thenReturn(mockResult);
    when(mockTxSimResult.isSuccessful()).thenReturn(isSuccessful);
    when(mockTxSimResult.getGasEstimate()).thenReturn(estimateGas);

    if (blockHeader == pendingBlockHeader) {
      when(transactionSimulator.processOnPending(
              any(), eq(Optional.empty()), any(), any(), eq(blockHeader)))
          .thenReturn(Optional.of(mockTxSimResult));
    } else {
      when(transactionSimulator.process(any(), eq(Optional.empty()), any(), any(), eq(blockHeader)))
          .thenReturn(Optional.of(mockTxSimResult));
    }
  }

  private CallParameter legacyTransactionCallParameter(final Wei gasPrice) {
    return ImmutableCallParameter.builder()
        .sender(Address.fromHexString("0x0"))
        .to(Address.fromHexString("0x0"))
        .gas(0L)
        .gasPrice(gasPrice)
        .value(Wei.ZERO)
        .input(Bytes.EMPTY)
        .strict(true)
        .build();
  }

  private CallParameter eip1559TransactionCallParameter() {
    return eip1559TransactionCallParameter(Optional.empty(), null, Bytes.EMPTY);
  }

  private CallParameter eip1559TransactionCallParameter(final Optional<Wei> gasPrice) {
    return eip1559TransactionCallParameter(gasPrice, null, Bytes.EMPTY);
  }

  private CallParameter eip1559TransactionCallParameter(
      final List<AccessListEntry> accessListEntries) {
    return eip1559TransactionCallParameter(Optional.empty(), accessListEntries, Bytes.EMPTY);
  }

  private CallParameter eip1559TransactionCallParameter(
      final Optional<Wei> gasPrice,
      final List<AccessListEntry> accessListEntries,
      final Bytes payload) {
    return ImmutableCallParameter.builder()
        .sender(Address.fromHexString("0x0"))
        .to(Address.fromHexString("0x0"))
        .gasPrice(gasPrice)
        .maxFeePerGas(Wei.fromHexString("0x10"))
        .maxPriorityFeePerGas(Wei.fromHexString("0x10"))
        .value(Wei.ZERO)
        .input(payload)
        .strict(false)
        .accessList(Optional.ofNullable(accessListEntries))
        .build();
  }

  private List<AccessListEntry> generateRandomAccessList() {
    return List.of(
        new AccessListEntry(Address.wrap(Bytes.random(Address.SIZE)), List.of(Bytes32.random())));
  }
}
