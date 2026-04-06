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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EthEstimateGasTest {
  private static final long MIN_TX_GAS_COST = 21_000L;
  private static final long TX_GAS_LIMIT_CAP = 1_000_000L;
  private static final long BLOCK_GAS_LIMIT = 2_000_000L;

  private EthEstimateGas method;

  @Mock private BlockHeader latestBlockHeader;
  @Mock private BlockHeader finalizedBlockHeader;
  @Mock private BlockHeader genesisBlockHeader;
  @Mock private BlockHeader pendingBlockHeader;
  @Mock private Blockchain blockchain;
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private TransactionSimulator transactionSimulator;
  @Mock private WorldStateArchive worldStateArchive;

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

    method =
        new EthEstimateGas(
            blockchainQueries, transactionSimulator, ImmutableApiConfiguration.builder().build());
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("eth_estimateGas");
  }

  @Test
  public void noStateOverrides() {
    final Wei gasPrice = Wei.of(1000);
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(gasPrice), "latest");
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

    final Wei gasPrice = Wei.of(1000);
    final JsonRpcRequestContext request =
        ethEstimateGasRequestWithStateOverrides(
            defaultLegacyTransactionCallParameter(gasPrice), "latest", expectedOverrides);

    Optional<StateOverrideMap> maybeOverrideMap = method.getAddressStateOverrideMap(request);
    assertThat(maybeOverrideMap.isPresent()).isTrue();
    StateOverrideMap overrideMap = maybeOverrideMap.get();
    assertThat(overrideMap.keySet()).hasSize(1);
    assertThat(overrideMap.values()).hasSize(1);

    assertThat(overrideMap).containsKey(address);
    assertThat(overrideMap).containsValue(override);
  }

  @Test
  public void shouldReturnErrorWhenTransientLegacyTransactionProcessorReturnsEmpty() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO));
    when(transactionSimulator.process(
            eq(
                modifiedLegacyTransactionCallParameter(
                    MIN_TX_GAS_COST, Wei.ZERO, OptionalLong.empty())),
            eq(Optional.empty()), // no account overrides
            any(TransactionValidationParams.class),
            any(OperationTracer.class),
            eq(latestBlockHeader)))
        .thenReturn(Optional.empty());

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.INTERNAL_ERROR);

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWhenTransientEip1559TransactionProcessorReturnsEmpty() {
    final JsonRpcRequestContext request = ethEstimateGasRequest(eip1559TransactionCallParameter());
    when(transactionSimulator.process(
            eq(modifiedEip1559TransactionCallParameter()),
            eq(Optional.empty()),
            any(TransactionValidationParams.class),
            any(OperationTracer.class),
            eq(latestBlockHeader)))
        .thenReturn(Optional.empty());

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.INTERNAL_ERROR);

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnGasEstimateWhenTransientLegacyTransactionProcessorReturnsResultSuccess() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO));
    mockTransientProcessorResultGasEstimate(MIN_TX_GAS_COST, true, false, pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Quantity.create(MIN_TX_GAS_COST));

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldUseGasPriceParameterWhenIsPresent() {
    final Wei gasPrice = Wei.of(1000);
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(gasPrice));
    mockTransientProcessorResultGasEstimate(
        MIN_TX_GAS_COST, true, gasPrice, Optional.empty(), pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Quantity.create(MIN_TX_GAS_COST));

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldUseNonceParameterWhenIsPresent() {
    final Wei gasPrice = Wei.of(1000);
    final long nonce = 0L;
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(eip1559TransactionCallParameter(OptionalLong.of(nonce)));
    getMockTransactionSimulatorResult(
        true,
        MIN_TX_GAS_COST,
        gasPrice,
        Optional.empty(),
        pendingBlockHeader,
        OptionalLong.of(nonce),
        Optional.empty());

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Quantity.create(MIN_TX_GAS_COST));
    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldNotErrorWhenGasPricePresentForEip1559Transaction() {
    final Wei gasPrice = Wei.of(1000);
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(eip1559TransactionCallParameter(OptionalLong.empty()));
    mockTransientProcessorResultGasEstimate(
        1L, true, gasPrice, Optional.empty(), pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Quantity.create(MIN_TX_GAS_COST));
    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void
      shouldReturnGasEstimateWhenTransientEip1559TransactionProcessorReturnsResultSuccess() {
    final JsonRpcRequestContext request = ethEstimateGasRequest(eip1559TransactionCallParameter());
    mockTransientProcessorResultGasEstimate(MIN_TX_GAS_COST, true, false, pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Quantity.create(MIN_TX_GAS_COST));
    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void
      shouldReturnGasEstimateErrorWhenTransientLegacyTransactionProcessorReturnsResultFailure() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO));
    mockTransientProcessorResultGasEstimate(1L, false, false, latestBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.INTERNAL_ERROR);

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void
      shouldReturnGasEstimateErrorWhenTransientEip1559TransactionProcessorReturnsResultFailure() {
    final JsonRpcRequestContext request = ethEstimateGasRequest(eip1559TransactionCallParameter());
    mockTransientProcessorResultGasEstimate(1L, false, false, latestBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.INTERNAL_ERROR);

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWhenLegacyTransactionProcessorReturnsTxInvalidReason() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO));
    mockTransientProcessorResultTxInvalidReason(
        TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE,
        "transaction up-front cost 10 exceeds transaction sender account balance 5",
        pendingBlockHeader);

    final ValidationResult<TransactionInvalidReason> validationResult =
        ValidationResult.invalid(
            TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE,
            "transaction up-front cost 10 exceeds transaction sender account balance 5");
    final JsonRpcError rpcError = JsonRpcError.from(validationResult);
    final JsonRpcResponse expectedResponse = new JsonRpcErrorResponse(null, rpcError);

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWhenEip1559TransactionProcessorReturnsTxInvalidReason() {
    final JsonRpcRequestContext request = ethEstimateGasRequest(eip1559TransactionCallParameter());
    mockTransientProcessorResultTxInvalidReason(
        TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE,
        "transaction up-front cost 10 exceeds transaction sender account balance 5",
        pendingBlockHeader);
    final ValidationResult<TransactionInvalidReason> validationResult =
        ValidationResult.invalid(
            TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE,
            "transaction up-front cost 10 exceeds transaction sender account balance 5");
    final JsonRpcError rpcError = JsonRpcError.from(validationResult);
    final JsonRpcResponse expectedResponse = new JsonRpcErrorResponse(null, rpcError);

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWhenWorldStateIsNotAvailable() {
    when(worldStateArchive.isWorldStateAvailable(any(), any())).thenReturn(false);
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO), "latest");
    mockTransientProcessorResultGasEstimate(1L, false, false, latestBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.WORLD_STATE_UNAVAILABLE);

    JsonRpcResponse theResponse = method.response(request);

    assertThat(theResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWhenTransactionReverted() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO));
    mockTransientProcessorResultGasEstimate(1L, false, true, pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, new JsonRpcError(RpcErrorType.REVERT_ERROR, "0x00"));

    assertThat(((JsonRpcErrorResponse) expectedResponse).getError().getMessage())
        .isEqualTo("Execution reverted");

    final JsonRpcResponse actualResponse = method.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);

    assertThat(((JsonRpcErrorResponse) actualResponse).getError().getMessage())
        .isEqualTo("Execution reverted");
  }

  @Test
  public void shouldReturnErrorReasonWhenTransactionReverted() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO));

    // ABI encoding of EVM "Error(string)" prefix + "ERC20: transfer from the zero address"
    final String executionRevertedReason =
        "0x08c379a000000000000000000000000000000000000000000000000000000000"
            + "000000200000000000000000000000000000000000000000000000000000000000"
            + "00002545524332303a207472616e736665722066726f6d20746865207a65726f20"
            + "61646472657373000000000000000000000000000000000000000000000000000000";

    mockTransientProcessorTxReverted(
        1L, false, Bytes.fromHexString(executionRevertedReason), pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            null, new JsonRpcError(RpcErrorType.REVERT_ERROR, executionRevertedReason));

    assertThat(((JsonRpcErrorResponse) expectedResponse).getError().getMessage())
        .isEqualTo("Execution reverted (ERC20: transfer from the zero address)");

    final JsonRpcResponse actualResponse = method.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);

    assertThat(((JsonRpcErrorResponse) actualResponse).getError().getMessage())
        .isEqualTo("Execution reverted (ERC20: transfer from the zero address)");
  }

  @Test
  public void shouldReturnABIDecodeErrorReasonWhenInvalidRevertReason() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO));

    // Invalid ABI bytes
    final String invalidRevertReason =
        "0x08c379a000000000000000000000000000000000000000000000000000000000"
            + "123451234512345123451234512345123451234512345123451234512345123451";

    mockTransientProcessorTxReverted(
        1L, false, Bytes.fromHexString(invalidRevertReason), pendingBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            null, new JsonRpcError(RpcErrorType.REVERT_ERROR, invalidRevertReason));

    assertThat(((JsonRpcErrorResponse) expectedResponse).getError().getMessage())
        .isEqualTo("Execution reverted (ABI decode error)");

    final JsonRpcResponse actualResponse = method.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);

    assertThat(((JsonRpcErrorResponse) actualResponse).getError().getMessage())
        .isEqualTo("Execution reverted (ABI decode error)");
  }

  @Test
  public void shouldIgnoreSenderBalanceAccountWhenStrictModeDisabled() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(legacyTransactionCallParameter(Wei.ZERO, Optional.of(false)));
    mockTransientProcessorResultGasEstimate(MIN_TX_GAS_COST, false, true, pendingBlockHeader);

    method.response(request);

    verify(transactionSimulator)
        .processOnPending(
            eq(
                modifiedLegacyTransactionCallParameter(
                    BLOCK_GAS_LIMIT, Wei.ZERO, OptionalLong.empty(), Optional.of(false))),
            eq(Optional.empty()), // no account overrides
            eq(
                TransactionValidationParams
                    .transactionSimulatorAllowExceedingBalanceAndFutureNonceParams),
            any(OperationTracer.class),
            eq(pendingBlockHeader));
  }

  @Test
  public void shouldNotIgnoreSenderBalanceByDefault() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO));
    failEstimationOnTxMinGas();

    method.response(request);

    verify(transactionSimulator)
        .processOnPending(
            eq(
                modifiedLegacyTransactionCallParameter(
                    BLOCK_GAS_LIMIT, Wei.ZERO, OptionalLong.empty(), Optional.empty())),
            eq(Optional.empty()), // no account overrides
            eq(TransactionValidationParams.transactionSimulatorAllowUnderpricedAndFutureNonce()),
            any(OperationTracer.class),
            eq(pendingBlockHeader));
  }

  @Test
  public void shouldIncludeHaltReasonWhenExecutionHalts() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(defaultLegacyTransactionCallParameter(Wei.ZERO));
    mockTransientProcessorResultTxInvalidReason(
        TransactionInvalidReason.EXECUTION_HALTED, "INVALID_OPERATION", pendingBlockHeader);

    final ValidationResult<TransactionInvalidReason> validationResult =
        ValidationResult.invalid(TransactionInvalidReason.EXECUTION_HALTED, "INVALID_OPERATION");
    final JsonRpcError rpcError = JsonRpcError.from(validationResult);
    final JsonRpcResponse expectedResponse = new JsonRpcErrorResponse(null, rpcError);

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldUseBlockTagParamWhenPresent() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(eip1559TransactionCallParameter(), "finalized");
    mockTransientProcessorResultGasEstimate(MIN_TX_GAS_COST, true, false, finalizedBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Quantity.create(MIN_TX_GAS_COST));

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void latestBlockTagEstimateOnLatestBlock() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(eip1559TransactionCallParameter(), "latest");
    mockTransientProcessorResultGasEstimate(MIN_TX_GAS_COST, true, false, latestBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Quantity.create(MIN_TX_GAS_COST));

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldUseBlockHashNotParentHashForBalanceLookup() {
    // Set up distinct hashes so we can verify the correct one is used
    final Hash latestBlockHash = Hash.wrap(Bytes32.fromHexString("0x" + "aa".repeat(32)));
    final Hash latestParentHash = Hash.wrap(Bytes32.fromHexString("0x" + "bb".repeat(32)));
    when(latestBlockHeader.getHash()).thenReturn(latestBlockHash);
    when(latestBlockHeader.getParentHash()).thenReturn(latestParentHash);

    final JsonRpcRequestContext request =
        ethEstimateGasRequest(eip1559TransactionCallParameter(), "latest");
    mockTransientProcessorResultGasEstimate(MIN_TX_GAS_COST, true, false, latestBlockHeader);

    method.response(request);

    // Balance must be looked up at the block's own hash, not its parent
    verify(blockchainQueries).accountBalance(Address.fromHexString("0x0"), latestBlockHash);
  }

  @Test
  public void shouldUseBlockNumberParamWhenPresent() {
    final JsonRpcRequestContext request =
        ethEstimateGasRequest(eip1559TransactionCallParameter(), "0x0");
    mockTransientProcessorResultGasEstimate(MIN_TX_GAS_COST, true, false, genesisBlockHeader);

    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Quantity.create(MIN_TX_GAS_COST));

    assertThat(method.response(request)).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldUseTxGasLimitCapWhenLessThatBlockGasLimit() {
    when(blockchainQueries.getTransactionGasLimitCap(any())).thenReturn(TX_GAS_LIMIT_CAP);

    failEstimationOnTxMinGas();

    final JsonRpcRequestContext request = ethEstimateGasRequest(eip1559TransactionCallParameter());

    method.response(request);

    verify(transactionSimulator)
        .processOnPending(
            eq(modifiedEip1559TransactionCallParameter(TX_GAS_LIMIT_CAP, OptionalLong.empty())),
            eq(Optional.empty()), // no account overrides
            eq(TransactionValidationParams.transactionSimulatorAllowUnderpricedAndFutureNonce()),
            any(OperationTracer.class),
            eq(pendingBlockHeader));
  }

  private void failEstimationOnTxMinGas() {
    getMockTransactionSimulatorResult(
        false,
        MIN_TX_GAS_COST,
        Wei.ZERO,
        Optional.empty(),
        pendingBlockHeader,
        OptionalLong.empty(),
        Optional.empty());
  }

  private void mockTransientProcessorResultTxInvalidReason(
      final TransactionInvalidReason reason,
      final String validationFailedErrorMessage,
      final BlockHeader blockHeader) {
    final TransactionSimulatorResult mockTxSimResult =
        getMockTransactionSimulatorResult(
            false,
            0,
            Wei.ZERO,
            Optional.empty(),
            blockHeader,
            OptionalLong.empty(),
            Optional.empty());
    when(mockTxSimResult.getValidationResult())
        .thenReturn(
            validationFailedErrorMessage == null
                ? ValidationResult.invalid(reason)
                : ValidationResult.invalid(reason, validationFailedErrorMessage));
  }

  private void mockTransientProcessorTxReverted(
      final long estimateGas,
      final boolean isSuccessful,
      final Bytes revertReason,
      final BlockHeader blockHeader) {
    mockTransientProcessorResultGasEstimate(
        estimateGas, isSuccessful, Wei.ZERO, Optional.of(revertReason), blockHeader);
  }

  private void mockTransientProcessorResultGasEstimate(
      final long estimateGas,
      final boolean isSuccessful,
      final boolean isReverted,
      final BlockHeader blockHeader) {
    mockTransientProcessorResultGasEstimate(
        estimateGas,
        isSuccessful,
        Wei.ZERO,
        isReverted ? Optional.of(Bytes.of(0)) : Optional.empty(),
        blockHeader);
  }

  private void mockTransientProcessorResultGasEstimate(
      final long estimateGas,
      final boolean isSuccessful,
      final Wei gasPrice,
      final Optional<Bytes> revertReason,
      final BlockHeader blockHeader) {
    getMockTransactionSimulatorResult(
        isSuccessful,
        estimateGas,
        gasPrice,
        revertReason,
        blockHeader,
        OptionalLong.empty(),
        Optional.empty());
  }

  @SuppressWarnings("ReferenceEquality")
  private TransactionSimulatorResult getMockTransactionSimulatorResult(
      final boolean isSuccessful,
      final long estimateGas,
      final Wei gasPrice,
      final Optional<Bytes> revertReason,
      final BlockHeader blockHeader,
      final OptionalLong maybeNonce,
      final Optional<Boolean> maybeStrict) {
    final TransactionSimulatorResult mockTxSimResult = mock(TransactionSimulatorResult.class);
    if (blockHeader == pendingBlockHeader) {
      when(transactionSimulator.processOnPending(
              eq(
                  modifiedLegacyTransactionCallParameter(
                      MIN_TX_GAS_COST, gasPrice, maybeNonce, maybeStrict)),
              eq(Optional.empty()), // no account overrides
              any(TransactionValidationParams.class),
              any(OperationTracer.class),
              eq(blockHeader)))
          .thenReturn(Optional.of(mockTxSimResult));
      when(transactionSimulator.processOnPending(
              eq(
                  modifiedLegacyTransactionCallParameter(
                      BLOCK_GAS_LIMIT, gasPrice, maybeNonce, maybeStrict)),
              eq(Optional.empty()), // no account overrides
              any(TransactionValidationParams.class),
              any(OperationTracer.class),
              eq(blockHeader)))
          .thenReturn(Optional.of(mockTxSimResult));
      when(transactionSimulator.processOnPending(
              eq(modifiedEip1559TransactionCallParameter(MIN_TX_GAS_COST, maybeNonce)),
              eq(Optional.empty()), // no account overrides
              any(TransactionValidationParams.class),
              any(OperationTracer.class),
              eq(blockHeader)))
          .thenReturn(Optional.of(mockTxSimResult));
      when(transactionSimulator.processOnPending(
              eq(modifiedEip1559TransactionCallParameter(BLOCK_GAS_LIMIT, maybeNonce)),
              eq(Optional.empty()), // no account overrides
              any(TransactionValidationParams.class),
              any(OperationTracer.class),
              eq(blockHeader)))
          .thenReturn(Optional.of(mockTxSimResult));
      when(transactionSimulator.processOnPending(
              eq(
                  modifiedLegacyTransactionCallParameter(
                      TX_GAS_LIMIT_CAP, gasPrice, maybeNonce, maybeStrict)),
              eq(Optional.empty()), // no account overrides
              any(TransactionValidationParams.class),
              any(OperationTracer.class),
              eq(blockHeader)))
          .thenReturn(Optional.of(mockTxSimResult));
    } else {
      when(transactionSimulator.process(
              eq(modifiedEip1559TransactionCallParameter()),
              eq(Optional.empty()), // no account overrides
              any(TransactionValidationParams.class),
              any(OperationTracer.class),
              eq(blockHeader)))
          .thenReturn(Optional.of(mockTxSimResult));
    }
    final TransactionProcessingResult mockResult = mock(TransactionProcessingResult.class);
    when(mockResult.getEstimateGasUsedByTransaction()).thenReturn(estimateGas);
    when(mockResult.getRevertReason()).thenReturn(revertReason);

    when(mockTxSimResult.result()).thenReturn(mockResult);
    when(mockTxSimResult.isSuccessful()).thenReturn(isSuccessful);
    return mockTxSimResult;
  }

  private CallParameter defaultLegacyTransactionCallParameter(final Wei gasPrice) {
    return legacyTransactionCallParameter(gasPrice, Optional.empty());
  }

  private CallParameter legacyTransactionCallParameter(
      final Wei gasPrice, final Optional<Boolean> maybeStrict) {
    return ImmutableCallParameter.builder()
        .sender(Address.fromHexString("0x0"))
        .to(Address.fromHexString("0x0"))
        .gasPrice(gasPrice)
        .strict(maybeStrict)
        .build();
  }

  private CallParameter modifiedLegacyTransactionCallParameter(
      final long gasLimit, final Wei gasPrice, final OptionalLong maybeNonce) {
    return modifiedLegacyTransactionCallParameter(gasLimit, gasPrice, maybeNonce, Optional.empty());
  }

  private CallParameter modifiedLegacyTransactionCallParameter(
      final long gasLimit,
      final Wei gasPrice,
      final OptionalLong maybeNonce,
      final Optional<Boolean> maybeStrict) {
    return ImmutableCallParameter.builder()
        .sender(Address.fromHexString("0x0"))
        .to(Address.fromHexString("0x0"))
        .gas(gasLimit)
        .gasPrice(gasPrice)
        .strict(maybeStrict)
        .nonce(maybeNonce)
        .build();
  }

  private CallParameter eip1559TransactionCallParameter() {
    return eip1559TransactionCallParameter(OptionalLong.empty());
  }

  private CallParameter eip1559TransactionCallParameter(final OptionalLong maybeNonce) {
    return ImmutableCallParameter.builder()
        .sender(Address.fromHexString("0x0"))
        .to(Address.fromHexString("0x0"))
        .maxPriorityFeePerGas(Wei.fromHexString("0x10"))
        .maxFeePerGas(Wei.fromHexString("0x10"))
        .nonce(maybeNonce)
        .build();
  }

  private CallParameter modifiedEip1559TransactionCallParameter() {
    return modifiedEip1559TransactionCallParameter(MIN_TX_GAS_COST, OptionalLong.empty());
  }

  private CallParameter modifiedEip1559TransactionCallParameter(
      final long gasLimit, final OptionalLong maybeNonce) {
    return ImmutableCallParameter.builder()
        .sender(Address.fromHexString("0x0"))
        .to(Address.fromHexString("0x0"))
        .gas(gasLimit)
        .maxPriorityFeePerGas(Wei.fromHexString("0x10"))
        .maxFeePerGas(Wei.fromHexString("0x10"))
        .nonce(maybeNonce)
        .build();
  }

  private JsonRpcRequestContext ethEstimateGasRequest(final CallParameter callParameter) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "eth_estimateGas", new Object[] {callParameter}));
  }

  private JsonRpcRequestContext ethEstimateGasRequest(
      final CallParameter callParameter, final String blockParam) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "eth_estimateGas", new Object[] {callParameter, blockParam}));
  }

  private JsonRpcRequestContext ethEstimateGasRequestWithStateOverrides(
      final CallParameter callParameter,
      final String blockParam,
      final StateOverrideMap overrides) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0", "eth_estimateGas", new Object[] {callParameter, blockParam, overrides}));
  }
}
