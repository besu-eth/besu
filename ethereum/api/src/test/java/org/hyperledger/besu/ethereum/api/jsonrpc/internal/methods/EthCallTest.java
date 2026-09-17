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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INTERNAL_ERROR;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.REVERT_ERROR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.PreCloseStateHandler;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EthCallTest {

  private EthCall method;

  @Mock private Blockchain blockchain;
  @Mock private ChainHead chainHead;
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private TransactionSimulator transactionSimulator;
  @Mock private BlockHeader blockHeader;
  @Mock private MetricsSystem metricsSystem;

  @Captor ArgumentCaptor<PreCloseStateHandler<Optional<JsonRpcResponse>>> mapperCaptor;
  @Captor ArgumentCaptor<CallParameter> callParameterCaptor;

  @BeforeEach
  public void setUp() {
    @SuppressWarnings("unchecked")
    LabelledMetric<Counter> mockGasUsedCounter = mock(LabelledMetric.class);
    Counter mockCounter = mock(Counter.class);
    when(mockGasUsedCounter.labels(any(String[].class))).thenReturn(mockCounter);

    when(metricsSystem.createLabelledCounter(
            any(BesuMetricCategory.class), any(String.class), any(String.class), any(String.class)))
        .thenReturn(mockGasUsedCounter);

    method = new EthCall(blockchainQueries, transactionSimulator, metricsSystem);
    blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBlockHash()).thenReturn(Hash.ZERO);
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("eth_call");
  }

  @Test
  public void noStateOverrides() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "latest");
    Optional<StateOverrideMap> overrideMap = method.getAddressStateOverrideMap(request);
    assertThat(overrideMap.isPresent()).isFalse();
  }

  @Test
  public void someStateOverrides() {
    StateOverrideMap expectedOverrides = new StateOverrideMap();
    StateOverride override =
        new StateOverride.Builder().withNonce(new UnsignedLongParameter("0x9e")).build();
    final Address address = Address.fromHexString("0xd9c9cd5f6779558b6e0ed4e6acf6b1947e7fa1f3");
    expectedOverrides.put(address, override);

    final JsonRpcRequestContext request =
        ethCallRequestWithStateOverrides(callParameter(), "latest", expectedOverrides);

    Optional<StateOverrideMap> maybeOverrideMap = method.getAddressStateOverrideMap(request);
    assertThat(maybeOverrideMap.isPresent()).isTrue();
    StateOverrideMap overrideMap = maybeOverrideMap.get();
    assertThat(overrideMap.keySet()).hasSize(1);
    assertThat(overrideMap.values()).hasSize(1);

    assertThat(overrideMap).containsKey(address);
    assertThat(overrideMap).containsValue(override);
  }

  @Test
  public void stateOverridesWithState() {
    StateOverrideMap expectedOverrides = new StateOverrideMap();
    StateOverride override =
        new StateOverride.Builder().withState(Map.of("0x1234", "0x5678")).build();
    final Address address = Address.fromHexString("0xd9c9cd5f6779558b6e0ed4e6acf6b1947e7fa1f3");
    expectedOverrides.put(address, override);

    final JsonRpcRequestContext request =
        ethCallRequestWithStateOverrides(callParameter(), "latest", expectedOverrides);

    Optional<StateOverrideMap> maybeOverrideMap = method.getAddressStateOverrideMap(request);
    assertThat(maybeOverrideMap.isPresent()).isTrue();
    StateOverrideMap overrideMap = maybeOverrideMap.get();
    assertThat(overrideMap.keySet()).hasSize(1);
    assertThat(overrideMap.values()).hasSize(1);

    assertThat(overrideMap).containsKey(address);
    assertThat(overrideMap).containsValue(override);
  }

  @Test
  public void fullStateOverrides() {
    StateOverrideMap suppliedOverrides = new StateOverrideMap();
    StateOverride override =
        new StateOverride.Builder()
            .withNonce(new UnsignedLongParameter("0x9e"))
            .withBalance(Wei.of(100))
            .withCode("0x1234")
            .withStateDiff(Map.of("0x1234", "0x5678"))
            .withMovePrecompileToAddress(Address.fromHexString("0x1234"))
            .build();
    final Address address = Address.fromHexString("0xd9c9cd5f6779558b6e0ed4e6acf6b1947e7fa1f3");
    suppliedOverrides.put(address, override);

    final JsonRpcRequestContext request =
        ethCallRequestWithStateOverrides(callParameter(), "latest", suppliedOverrides);

    Optional<StateOverrideMap> maybeOverrideMap = method.getAddressStateOverrideMap(request);
    assertThat(maybeOverrideMap.isPresent()).isTrue();
    StateOverrideMap actualOverrideMap = maybeOverrideMap.get();
    assertThat(actualOverrideMap.keySet()).hasSize(1);
    assertThat(actualOverrideMap.values()).hasSize(1);

    assertThat(actualOverrideMap).containsKey(address);
    assertThat(actualOverrideMap).containsValue(override);
  }

  @Test
  public void shouldReturnInternalErrorWhenProcessorReturnsEmpty() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "latest");
    final JsonRpcResponse expectedResponse = new JsonRpcErrorResponse(null, INTERNAL_ERROR);

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);
    when(transactionSimulator.process(any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.empty());

    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ZERO));
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(transactionSimulator).process(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void shouldAcceptRequestWhenMissingOptionalFields() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder().strict(Boolean.FALSE).build();

    final JsonRpcRequestContext request = ethCallRequest(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Bytes.of().toString());

    mockTransactionProcessorSuccessResult(expectedResponse);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);

    final JsonRpcResponse response = method.response(request);

    final TransactionSimulatorResult result = mock(TransactionSimulatorResult.class);
    when(result.isSuccessful()).thenReturn(true);
    when(result.getValidationResult()).thenReturn(ValidationResult.valid());
    when(result.getOutput()).thenReturn(Bytes.of());
    verify(transactionSimulator)
        .process(
            eq(callParameter), eq(Optional.empty()), any(), any(), mapperCaptor.capture(), any());
    assertThat(mapperCaptor.getValue().apply(mock(MutableWorldState.class), Optional.of(result)))
        .isEqualTo(Optional.of(expectedResponse));

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnExecutionResultWhenExecutionIsSuccessful() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Bytes.of(1).toString());
    mockTransactionProcessorSuccessResult(expectedResponse);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);

    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ZERO));
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);

    final JsonRpcResponse response = method.response(request);

    final TransactionSimulatorResult result = mock(TransactionSimulatorResult.class);
    when(result.isSuccessful()).thenReturn(true);
    when(result.getValidationResult()).thenReturn(ValidationResult.valid());
    when(result.getOutput()).thenReturn(Bytes.of(1));
    verify(transactionSimulator)
        .process(
            eq(callParameter()), eq(Optional.empty()), any(), any(), mapperCaptor.capture(), any());
    assertThat(mapperCaptor.getValue().apply(mock(MutableWorldState.class), Optional.of(result)))
        .isEqualTo(Optional.of(expectedResponse));

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(blockchainQueries, atLeastOnce()).getBlockchain();
    verifyNoMoreInteractions(blockchainQueries);
  }

  @Test
  public void shouldReturnBasicExecutionRevertErrorWithoutReason() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "latest");

    // Expect a revert error with no decoded reason (error doesn't begin "Error(string)" so ignored)
    final String abiHexString = "0x1234";
    final JsonRpcError expectedError = new JsonRpcError(REVERT_ERROR, abiHexString);
    final JsonRpcErrorResponse expectedResponse = new JsonRpcErrorResponse(null, expectedError);

    assertThat(expectedResponse.getError().getMessage()).isEqualTo("Execution reverted");

    mockTransactionProcessorSuccessResult(expectedResponse);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);

    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ZERO));
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);

    final JsonRpcResponse response = method.response(request);

    final TransactionProcessingResult processingResult =
        new TransactionProcessingResult(
            null,
            null,
            0,
            0,
            null,
            null,
            Optional.of(Bytes.fromHexString(abiHexString)),
            Optional.empty());

    final TransactionSimulatorResult result = mock(TransactionSimulatorResult.class);
    when(result.isSuccessful()).thenReturn(false);
    when(result.getValidationResult()).thenReturn(ValidationResult.valid());
    when(result.result()).thenReturn(processingResult);
    verify(transactionSimulator)
        .process(any(), eq(Optional.empty()), any(), any(), mapperCaptor.capture(), any());
    assertThat(mapperCaptor.getValue().apply(mock(MutableWorldState.class), Optional.of(result)))
        .isEqualTo(Optional.of(expectedResponse));

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
    assertThat(((JsonRpcErrorResponse) response).getError().getMessage())
        .isEqualTo("Execution reverted");
  }

  @Test
  public void shouldReturnExecutionRevertErrorWithABIParseError() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "latest");

    // Expect a revert error with no decoded reason (error begins with "Error(string)" but trailing
    // bytes are invalid ABI)
    final String abiHexString = "0x08c379a002d36d";
    final JsonRpcError expectedError = new JsonRpcError(REVERT_ERROR, abiHexString);
    final JsonRpcErrorResponse expectedResponse = new JsonRpcErrorResponse(null, expectedError);

    assertThat(expectedResponse.getError().getMessage())
        .isEqualTo("Execution reverted (ABI decode error)");

    mockTransactionProcessorSuccessResult(expectedResponse);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);

    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ZERO));
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);

    final JsonRpcResponse response = method.response(request);
    final TransactionProcessingResult processingResult =
        new TransactionProcessingResult(
            null,
            null,
            0,
            0,
            null,
            null,
            Optional.of(Bytes.fromHexString(abiHexString)),
            Optional.empty());

    final TransactionSimulatorResult result = mock(TransactionSimulatorResult.class);
    when(result.isSuccessful()).thenReturn(false);
    when(result.getValidationResult()).thenReturn(ValidationResult.valid());
    when(result.result()).thenReturn(processingResult);
    verify(transactionSimulator)
        .process(any(), eq(Optional.empty()), any(), any(), mapperCaptor.capture(), any());
    assertThat(mapperCaptor.getValue().apply(mock(MutableWorldState.class), Optional.of(result)))
        .isEqualTo(Optional.of(expectedResponse));

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
    assertThat(((JsonRpcErrorResponse) response).getError().getMessage())
        .isEqualTo("Execution reverted (ABI decode error)");
  }

  @Test
  public void shouldReturnExecutionRevertErrorWithParsedABI() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "latest");

    // Expect a revert error with decoded reason (error begins with "Error(string)", trailing bytes
    // = "ERC20: transfer from the zero address")
    final String abiHexString =
        "0x08c379a00000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000002545524332303a207472616e736665722066726f6d20746865207a65726f2061646472657373000000000000000000000000000000000000000000000000000000";
    final JsonRpcError expectedError = new JsonRpcError(REVERT_ERROR, abiHexString);
    final JsonRpcErrorResponse expectedResponse = new JsonRpcErrorResponse(null, expectedError);

    assertThat(expectedResponse.getError().getMessage())
        .isEqualTo("Execution reverted (ERC20: transfer from the zero address)");

    mockTransactionProcessorSuccessResult(expectedResponse);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);

    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ZERO));
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);

    final JsonRpcResponse response = method.response(request);

    final TransactionProcessingResult processingResult =
        new TransactionProcessingResult(
            null,
            null,
            0,
            0,
            null,
            null,
            Optional.of(Bytes.fromHexString(abiHexString)),
            Optional.empty());

    final TransactionSimulatorResult result = mock(TransactionSimulatorResult.class);
    when(result.isSuccessful()).thenReturn(false);
    when(result.getValidationResult()).thenReturn(ValidationResult.valid());
    when(result.result()).thenReturn(processingResult);

    verify(transactionSimulator)
        .process(any(), eq(Optional.empty()), any(), any(), mapperCaptor.capture(), any());
    assertThat(mapperCaptor.getValue().apply(mock(MutableWorldState.class), Optional.of(result)))
        .isEqualTo(Optional.of(expectedResponse));

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
    assertThat(((JsonRpcErrorResponse) response).getError().getMessage())
        .isEqualTo("Execution reverted (ERC20: transfer from the zero address)");
  }

  @Test
  public void shouldUseCorrectBlockNumberWhenLatest() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "latest");
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);
    when(transactionSimulator.process(any(), eq(Optional.empty()), any(), any(), any(), any()))
        .thenReturn(Optional.empty());

    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ZERO));
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);

    method.response(request);

    verify(blockchainQueries, atLeastOnce()).getBlockchain();
    verify(transactionSimulator).process(any(), eq(Optional.empty()), any(), any(), any(), any());
  }

  @Test
  public void shouldUseCorrectBlockNumberWhenEarliest() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "earliest");
    when(blockchainQueries.getBlockHashByNumber(anyLong())).thenReturn(Optional.of(Hash.ZERO));
    when(transactionSimulator.process(
            any(), any(), any(TransactionValidationParams.class), any(), any(BlockHeader.class)))
        .thenReturn(Optional.empty());
    when(blockchainQueries.getBlockHeaderByHash(Hash.ZERO))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    method.response(request);

    verify(blockchainQueries).getBlockHeaderByHash(eq(Hash.ZERO));
    verify(transactionSimulator).process(any(), eq(Optional.empty()), any(), any(), any(), any());
  }

  @Test
  public void shouldUseCorrectBlockNumberWhenSafe() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "safe");
    when(blockchainQueries.getBlockHeaderByHash(Hash.ZERO)).thenReturn(Optional.of(blockHeader));
    when(blockchainQueries.safeBlockHeader()).thenReturn(Optional.of(blockHeader));
    when(transactionSimulator.process(any(), eq(Optional.empty()), any(), any(), any(), any()))
        .thenReturn(Optional.empty());
    method.response(request);

    verify(blockchainQueries).getBlockHeaderByHash(Hash.ZERO);
    verify(blockchainQueries).safeBlockHeader();
    verify(transactionSimulator).process(any(), eq(Optional.empty()), any(), any(), any(), any());
  }

  @Test
  public void shouldUseCorrectBlockNumberWhenFinalized() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), "finalized");
    when(blockchainQueries.getBlockHeaderByHash(Hash.ZERO)).thenReturn(Optional.of(blockHeader));
    when(blockchainQueries.finalizedBlockHeader()).thenReturn(Optional.of(blockHeader));
    when(transactionSimulator.process(any(), eq(Optional.empty()), any(), any(), any(), any()))
        .thenReturn(Optional.empty());
    method.response(request);

    verify(blockchainQueries).getBlockHeaderByHash(Hash.ZERO);
    verify(blockchainQueries).finalizedBlockHeader();
    verify(transactionSimulator).process(any(), eq(Optional.empty()), any(), any(), any(), any());
  }

  @Test
  public void shouldUseCorrectBlockNumberWhenSpecified() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), Quantity.create(13L));
    when(blockchainQueries.headBlockNumber()).thenReturn(14L);
    when(blockchainQueries.getBlockHashByNumber(anyLong())).thenReturn(Optional.of(Hash.ZERO));
    when(blockchainQueries.getBlockHeaderByHash(Hash.ZERO))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    when(transactionSimulator.process(any(), eq(Optional.empty()), any(), any(), any(), any()))
        .thenReturn(Optional.empty());

    method.response(request);

    verify(blockchainQueries).getBlockHeaderByHash(eq(Hash.ZERO));
    verify(transactionSimulator).process(any(), eq(Optional.empty()), any(), any(), any(), any());
  }

  @Test
  public void shouldReturnNullWhenInvalidBlockNumberSpecified() {
    final JsonRpcRequestContext request = ethCallRequest(callParameter(), Quantity.create(33L));
    when(blockchainQueries.headBlockNumber()).thenReturn(14L);

    final JsonRpcResponse response = method.response(request);

    Assertions.assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    Assertions.assertThat(((JsonRpcSuccessResponse) response).getResult()).isNull();

    verify(blockchainQueries).headBlockNumber();
  }

  @Test
  public void shouldAutoSelectIsAllowedExceedingBalanceToTrueWhenGasPriceIsZero() {
    final CallParameter callParameters = callParameter();
    internalAutoSelectIsAllowedExceedingBalance(callParameters, Optional.empty(), true);
  }

  @Test
  public void shouldAutoSelectIsAllowedExceedingBalanceToTrueWhenGasPriceIsZeroAfterEIP1559() {
    final CallParameter callParameters = callParameter();
    internalAutoSelectIsAllowedExceedingBalance(callParameters, Optional.of(Wei.ONE), true);
  }

  @Test
  public void shouldAutoSelectIsAllowedExceedingBalanceToFalseWhenGasPriceIsNotZero() {
    final CallParameter callParameters =
        callParameter(
            Optional.of(Wei.ONE), Optional.empty(), Optional.empty(), OptionalLong.empty());
    internalAutoSelectIsAllowedExceedingBalance(callParameters, Optional.empty(), false);
  }

  @Test
  public void shouldAutoSelectIsAllowedExceedingBalanceToFalseWhenGasPriceIsNotZeroAfterEIP1559() {
    final CallParameter callParameters =
        callParameter(
            Optional.of(Wei.ONE), Optional.empty(), Optional.empty(), OptionalLong.empty());
    internalAutoSelectIsAllowedExceedingBalance(callParameters, Optional.of(Wei.ONE), false);
  }

  @Test
  public void shouldAutoSelectIsAllowedExceedingBalanceToTrueWhenFeesAreZero() {
    final CallParameter callParameters =
        callParameter(
            Optional.empty(), Optional.of(Wei.ZERO), Optional.of(Wei.ZERO), OptionalLong.empty());
    internalAutoSelectIsAllowedExceedingBalance(callParameters, Optional.of(Wei.ONE), true);
  }

  @Test
  public void shouldAutoSelectIsAllowedExceedingBalanceToFalseWhenFeesAreZero() {
    final CallParameter callParameters =
        callParameter(
            Optional.empty(), Optional.of(Wei.ONE), Optional.of(Wei.ONE), OptionalLong.empty());
    internalAutoSelectIsAllowedExceedingBalance(callParameters, Optional.of(Wei.ONE), false);
  }

  @Test
  public void shouldAllowTxGasLimitBiggerThanCap() {
    final long txGasLimitCap = 100_000L;
    final long txGasLimitParameter = txGasLimitCap * 2;
    final CallParameter callParameters =
        callParameter(
            Optional.empty(),
            Optional.of(Wei.ONE),
            Optional.of(Wei.ONE),
            OptionalLong.of(txGasLimitParameter));
    final JsonRpcRequestContext request = ethCallRequest(callParameters, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Bytes.of(1).toString());
    mockTransactionProcessorSuccessResult(expectedResponse);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchainQueries.getTransactionGasLimitCap(any())).thenReturn(txGasLimitCap);
    when(blockchain.getChainHead()).thenReturn(chainHead);

    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ZERO));
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);

    final JsonRpcResponse response = method.response(request);

    verify(transactionSimulator)
        .process(callParameterCaptor.capture(), eq(Optional.empty()), any(), any(), any(), any());

    assertThat(callParameterCaptor.getValue().getGas()).hasValue(txGasLimitParameter);
    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  private void internalAutoSelectIsAllowedExceedingBalance(
      final CallParameter callParameter,
      final Optional<Wei> baseFee,
      final boolean isAllowedExceedingBalance) {
    final JsonRpcRequestContext request = ethCallRequest(callParameter, "latest");

    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(baseFee);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);
    when(chainHead.getBlockHeader()).thenReturn(blockHeader);

    method.response(request);

    final TransactionValidationParams transactionValidationParams =
        ImmutableTransactionValidationParams.builder()
            .from(TransactionValidationParams.transactionSimulator())
            .isAllowExceedingBalance(isAllowedExceedingBalance)
            .isAllowFutureNonce(true)
            .build();

    verify(transactionSimulator)
        .process(any(), eq(Optional.empty()), eq(transactionValidationParams), any(), any(), any());
  }

  private CallParameter callParameter() {
    return callParameter(
        Optional.of(Wei.ZERO), Optional.empty(), Optional.empty(), OptionalLong.empty());
  }

  private CallParameter callParameter(
      final Optional<Wei> gasPrice,
      final Optional<Wei> maxFeesPerGas,
      final Optional<Wei> maxPriorityFeesPerGas,
      final OptionalLong gasLimit) {
    return ImmutableCallParameter.builder()
        .sender(Address.fromHexString("0x0"))
        .to(Address.fromHexString("0x0"))
        .gasPrice(gasPrice)
        .maxFeePerGas(maxFeesPerGas)
        .maxPriorityFeePerGas(maxPriorityFeesPerGas)
        .gas(gasLimit)
        .build();
  }

  private JsonRpcRequestContext ethCallRequest(
      final CallParameter callParameter, final String blockNumberInHex) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "eth_call", new Object[] {callParameter, blockNumberInHex}));
  }

  private JsonRpcRequestContext ethCallRequestWithStateOverrides(
      final CallParameter callParameter,
      final String blockNumberInHex,
      final StateOverrideMap overrides) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0", "eth_call", new Object[] {callParameter, blockNumberInHex, overrides}));
  }

  private void mockTransactionProcessorSuccessResult(final JsonRpcResponse jsonRpcResponse) {
    when(transactionSimulator.process(any(), eq(Optional.empty()), any(), any(), any(), any()))
        .thenReturn(Optional.of(jsonRpcResponse));
  }
}
