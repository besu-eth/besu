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
package org.hyperledger.besu.ethereum.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;
import static org.hyperledger.besu.evm.tracing.OperationTracer.NO_TRACING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.CodeDelegation;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.blockhash.PreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult.Status;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({"rawtypes", "unchecked"})
public class TransactionSimulatorTest extends TrustedSetupClassLoaderExtension {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final BigInteger HALF_CURVE_ORDER = SIGNATURE_ALGORITHM.getHalfCurveOrder();
  private static final SECPSignature FAKE_SIGNATURE =
      SIGNATURE_ALGORITHM.createSignature(HALF_CURVE_ORDER, HALF_CURVE_ORDER, (byte) 0);

  private static final Address DEFAULT_FROM =
      Address.fromHexString("0x0000000000000000000000000000000000000000");
  private static final long RPC_GAS_CAP = 500_000L;
  private static final long TRANSFER_GAS_LIMIT = 21_000L;
  private static final long DEFAULT_BLOCK_GAS_LIMIT = 30_000_000L;
  private TransactionSimulator uncappedTransactionSimulator;
  private TransactionSimulator cappedTransactionSimulator;
  private TransactionSimulator defaultCappedTransactionSimulator;

  @Mock private Blockchain blockchain;
  @Mock private WorldStateArchive worldStateArchive;
  @Mock private MutableWorldState worldState;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private MainnetTransactionProcessor transactionProcessor;
  @Mock private GasLimitCalculator gasLimitCalculator;
  private final BlockHeaderTestFixture blockHeaderTestFixture = new BlockHeaderTestFixture();

  @BeforeEach
  public void setUp() {
    final var miningConfiguration = MiningConfiguration.newDefault().setCoinbase(Address.ZERO);
    // rpc gas cap 0 means unlimited
    this.uncappedTransactionSimulator =
        new TransactionSimulator(
            blockchain, worldStateArchive, protocolSchedule, miningConfiguration, 0);
    // capped at a lower limit
    this.cappedTransactionSimulator =
        new TransactionSimulator(
            blockchain, worldStateArchive, protocolSchedule, miningConfiguration, RPC_GAS_CAP);
    // capped at default limit
    this.defaultCappedTransactionSimulator =
        new TransactionSimulator(
            blockchain,
            worldStateArchive,
            protocolSchedule,
            miningConfiguration,
            ApiConfiguration.DEFAULT_GAS_CAP);
  }

  @Test
  public void testOverrides_whenNoOverrides_noUpdates() {
    MutableAccount mutableAccount = mock(MutableAccount.class);
    when(mutableAccount.getAddress()).thenReturn(DEFAULT_FROM); // called from logging
    StateOverride.Builder builder = new StateOverride.Builder();
    StateOverride override = builder.build();
    TransactionSimulator.applyOverrides(mutableAccount, override);
    verify(mutableAccount).getAddress();
    verifyNoMoreInteractions(mutableAccount);
  }

  @Test
  public void testOverrides_whenBalanceOverrides_balanceIsUpdated() {
    MutableAccount mutableAccount = mock(MutableAccount.class);
    when(mutableAccount.getAddress()).thenReturn(DEFAULT_FROM);
    StateOverride.Builder builder = new StateOverride.Builder().withBalance(Wei.of(99));
    StateOverride override = builder.build();
    TransactionSimulator.applyOverrides(mutableAccount, override);
    verify(mutableAccount).setBalance(eq(Wei.of(99)));
  }

  @Test
  public void testOverrides_whenStateDiffOverrides_stateIsUpdated() {
    MutableAccount mutableAccount = mock(MutableAccount.class);
    when(mutableAccount.getAddress()).thenReturn(DEFAULT_FROM);
    final String storageKey = "0x01a2";
    final String storageValue = "0x00ff";
    StateOverride.Builder builder =
        new StateOverride.Builder().withStateDiff(Map.of(storageKey, storageValue));
    StateOverride override = builder.build();
    TransactionSimulator.applyOverrides(mutableAccount, override);
    verify(mutableAccount)
        .setStorageValue(
            eq(UInt256.fromHexString(storageKey)), eq(UInt256.fromHexString(storageValue)));
  }

  @Test
  public void testOverrides_whenStateOverrides_stateIsUpdated() {
    MutableAccount mutableAccount = mock(MutableAccount.class);
    when(mutableAccount.getAddress()).thenReturn(DEFAULT_FROM);
    final String storageKey = "0x01a2";
    final String storageValue = "0x00ff";
    StateOverride.Builder builder =
        new StateOverride.Builder().withState(Map.of(storageKey, storageValue));
    StateOverride override = builder.build();
    TransactionSimulator.applyOverrides(mutableAccount, override);

    verify(mutableAccount).clearStorage();

    verify(mutableAccount)
        .setStorageValue(
            eq(UInt256.fromHexString(storageKey)), eq(UInt256.fromHexString(storageValue)));
  }

  @Test
  public void shouldReturnEmptyWhenBlockDoesNotExist() {
    when(blockchain.getBlockHeader(eq(1L))).thenReturn(Optional.empty());

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(legacyTransactionCallParameterBuilder().build(), 1L);

    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void shouldReturnSuccessfulResultWhenProcessingIsSuccessful() {
    final CallParameter callParameter = legacyTransactionCallParameterBuilder().build();

    final var blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(1L)
            .gasPrice(callParameter.getGasPrice().orElseThrow())
            .gasLimit(blockHeader.getGasLimit())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(callParameter, 1L);

    assertThat(result.get().isSuccessful()).isTrue();
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void simulateOnPendingBlockWorks() {
    final CallParameter callParameter = eip1559TransactionCallParameterBuilder().build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .gasLimit(blockHeader.getGasLimit())
            .maxFeePerGas(callParameter.getMaxFeePerGas().orElseThrow())
            .maxPriorityFeePerGas(callParameter.getMaxPriorityFeePerGas().orElseThrow())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.processOnPending(
            callParameter,
            Optional.empty(),
            TransactionValidationParams.transactionSimulator(),
            NO_TRACING,
            uncappedTransactionSimulator.simulatePendingBlockHeader());

    assertThat(result.get().isSuccessful()).isTrue();
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldSetGasPriceToZeroWhenExceedingBalanceAllowed() {
    final CallParameter callParameter =
        legacyTransactionCallParameterBuilder().gasPrice(Wei.ONE).build();

    final var blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(1L)
            .gasPrice(Wei.ZERO)
            .gasLimit(blockHeader.getGasLimit())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    uncappedTransactionSimulator.process(
        callParameter,
        ImmutableTransactionValidationParams.builder().isAllowExceedingBalance(true).build(),
        NO_TRACING,
        1L);

    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldSetFeePerGasToZeroWhenExceedingBalanceAllowed() {
    final CallParameter callParameter =
        eip1559TransactionCallParameterBuilder()
            .maxFeePerGas(Wei.ONE)
            .maxPriorityFeePerGas(Wei.ONE)
            .gas(TRANSFER_GAS_LIMIT)
            .build();

    mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .gasLimit(TRANSFER_GAS_LIMIT)
            .maxFeePerGas(Wei.ZERO)
            .maxPriorityFeePerGas(Wei.ZERO)
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();

    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    uncappedTransactionSimulator.process(
        callParameter,
        ImmutableTransactionValidationParams.builder().isAllowExceedingBalance(true).build(),
        NO_TRACING,
        1L);

    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldNotSetGasPriceToZeroWhenExceedingBalanceIsNotAllowed() {
    final CallParameter callParameter =
        legacyTransactionCallParameterBuilder().gasPrice(Wei.ONE).build();

    final var blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(1L)
            .gasPrice(callParameter.getGasPrice().orElseThrow())
            .gasLimit(blockHeader.getGasLimit())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();

    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    uncappedTransactionSimulator.process(
        callParameter,
        ImmutableTransactionValidationParams.builder().isAllowExceedingBalance(false).build(),
        NO_TRACING,
        1L);

    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldNotSetFeePerGasToZeroWhenExceedingBalanceIsNotAllowed() {
    final CallParameter callParameter =
        eip1559TransactionCallParameterBuilder()
            .maxFeePerGas(Wei.ONE)
            .maxPriorityFeePerGas(Wei.ONE)
            .gas(TRANSFER_GAS_LIMIT)
            .build();

    mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .gasLimit(TRANSFER_GAS_LIMIT)
            .maxFeePerGas(callParameter.getMaxFeePerGas().orElseThrow())
            .maxPriorityFeePerGas(callParameter.getMaxPriorityFeePerGas().orElseThrow())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    uncappedTransactionSimulator.process(
        callParameter,
        ImmutableTransactionValidationParams.builder().isAllowExceedingBalance(false).build(),
        NO_TRACING,
        1L);

    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldUseDefaultValuesWhenMissingOptionalFields() {
    final CallParameter callParameter = legacyTransactionCallParameterBuilder().build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(1L)
            .gasPrice(Wei.ZERO)
            .gasLimit(blockHeader.getGasLimit())
            .to(DEFAULT_FROM)
            .sender(Address.fromHexString("0x0"))
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    uncappedTransactionSimulator.process(callParameter, 1L);

    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldUseZeroNonceWhenAccountDoesNotExist() {
    final CallParameter callParameter = legacyTransactionCallParameterBuilder().build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);
    mockWorldStateForAbsentAccount(blockHeader);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(0L)
            .gasPrice(Wei.ZERO)
            .gasLimit(blockHeader.getGasLimit())
            .to(DEFAULT_FROM)
            .sender(Address.fromHexString("0x0"))
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    uncappedTransactionSimulator.process(callParameter, 1L);

    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldUseSpecifiedNonceWhenProvided() {
    long expectedNonce = 2L;
    long accountNonce = 1L;
    final CallParameter callParameter =
        eip1559TransactionCallParameterBuilder().nonce(expectedNonce).build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);
    mockWorldStateForAccount(blockHeader, Address.fromHexString("0x0"), accountNonce);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(expectedNonce)
            .maxFeePerGas(Wei.ZERO)
            .maxPriorityFeePerGas(Wei.ZERO)
            .gasLimit(blockHeader.getGasLimit())
            .to(DEFAULT_FROM)
            .sender(Address.fromHexString("0x0"))
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    uncappedTransactionSimulator.process(callParameter, 1L);
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldReturnFailureResultWhenProcessingFails() {
    final CallParameter callParameter = legacyTransactionCallParameterBuilder().build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(1L)
            .gasPrice(callParameter.getGasPrice().orElseThrow())
            .gasLimit(blockHeader.getGasLimit())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.FAILED);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(callParameter, 1L);

    assertThat(result.get().isSuccessful()).isFalse();
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldReturnEmptyWhenBlockDoesNotExistByHash() {
    when(blockchain.getBlockHeader(eq(Hash.ZERO))).thenReturn(Optional.empty());

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(
            legacyTransactionCallParameterBuilder().build(), Hash.ZERO);

    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void shouldReturnSuccessfulResultWhenProcessingIsSuccessfulByHash() {
    final CallParameter callParameter = legacyTransactionCallParameterBuilder().build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(1L)
            .gasPrice(callParameter.getGasPrice().orElseThrow())
            .gasLimit(blockHeader.getGasLimit())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(callParameter, blockHeader.getBlockHash());

    assertThat(result.get().isSuccessful()).isTrue();
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldUseDefaultValuesWhenMissingOptionalFieldsByHash() {
    final CallParameter callParameter = legacyTransactionCallParameterBuilder().build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(1L)
            .gasPrice(Wei.ZERO)
            .gasLimit(blockHeader.getGasLimit())
            .to(DEFAULT_FROM)
            .sender(Address.fromHexString("0x0"))
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    uncappedTransactionSimulator.process(callParameter, blockHeader.getBlockHash());

    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldUseZeroNonceWhenAccountDoesNotExistByHash() {
    final CallParameter callParameter = legacyTransactionCallParameterBuilder().build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);
    mockWorldStateForAbsentAccount(blockHeader);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(0L)
            .gasPrice(Wei.ZERO)
            .gasLimit(blockHeader.getGasLimit())
            .to(DEFAULT_FROM)
            .sender(Address.fromHexString("0x0"))
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    uncappedTransactionSimulator.process(callParameter, blockHeader.getBlockHash());

    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldReturnFailureResultWhenProcessingFailsByHash() {
    final CallParameter callParameter = legacyTransactionCallParameterBuilder().build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(1L)
            .gasPrice(callParameter.getGasPrice().orElseThrow())
            .gasLimit(blockHeader.getGasLimit())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.FAILED);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(callParameter, blockHeader.getBlockHash());

    assertThat(result.get().isSuccessful()).isFalse();
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldReturnSuccessfulResultWhenEip1559TransactionProcessingIsSuccessful() {
    final CallParameter callParameter = eip1559TransactionCallParameterBuilder().build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .gasLimit(blockHeader.getGasLimit())
            .maxFeePerGas(callParameter.getMaxFeePerGas().orElseThrow())
            .maxPriorityFeePerGas(callParameter.getMaxPriorityFeePerGas().orElseThrow())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(callParameter, 1L);

    assertThat(result.get().isSuccessful()).isTrue();
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldCapGasLimitWhenOriginalTransactionExceedsGasCap() {
    final CallParameter callParameter =
        eip1559TransactionCallParameterBuilder().gas(RPC_GAS_CAP + 1).build();

    mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .gasLimit(RPC_GAS_CAP)
            .maxFeePerGas(callParameter.getMaxFeePerGas().orElseThrow())
            .maxPriorityFeePerGas(callParameter.getMaxPriorityFeePerGas().orElseThrow())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();

    mockProtocolSpecForProcessWithWorldUpdater();

    // call process with original transaction
    cappedTransactionSimulator.process(
        callParameter, TransactionValidationParams.transactionSimulator(), NO_TRACING, 1L);

    // expect overwritten transaction to be processed
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldUseProvidedGasLimitWhenBelowRpcCapGas() {
    final CallParameter callParameter =
        eip1559TransactionCallParameterBuilder().gas(RPC_GAS_CAP / 2).build();

    final var blockHeader = mockBlockHeader(Hash.ZERO, 1L, Wei.ONE, DEFAULT_BLOCK_GAS_LIMIT);

    mockBlockchainAndWorldState(callParameter, blockHeader);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .gasLimit(RPC_GAS_CAP / 2)
            .maxFeePerGas(callParameter.getMaxFeePerGas().orElseThrow())
            .maxPriorityFeePerGas(callParameter.getMaxPriorityFeePerGas().orElseThrow())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();

    mockProtocolSpecForProcessWithWorldUpdater();

    // call process with original transaction
    cappedTransactionSimulator.process(
        callParameter, TransactionValidationParams.transactionSimulator(), NO_TRACING, 1L);

    // expect overwritten transaction to be processed
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldUseRpcGasCapWhenGasLimitNotPresent() {
    // generate call parameters that do not specify a gas limit,
    // expect the rpc gas cap to be used for simulation
    final CallParameter callParameter =
        eip1559TransactionCallParameterBuilder().gas(OptionalLong.empty()).build();

    mockBlockchainAndWorldState(callParameter);
    mockProtocolSpecForProcessWithWorldUpdater();

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .maxFeePerGas(callParameter.getMaxFeePerGas().orElseThrow())
            .maxPriorityFeePerGas(callParameter.getMaxPriorityFeePerGas().orElseThrow())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .gasLimit(RPC_GAS_CAP)
            .build();

    // call process with original transaction
    cappedTransactionSimulator.process(
        callParameter, TransactionValidationParams.transactionSimulator(), NO_TRACING, 1L);

    // expect transaction with the original gas limit to be processed
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldUseBlockGasLimitWhenGasLimitNotPresent() {
    // generate call parameters that do not specify a gas limit,
    // expect the block gas limit to be used for simulation,
    // since lower than the default rpc gas cap
    final CallParameter callParameter =
        eip1559TransactionCallParameterBuilder().gas(OptionalLong.empty()).build();

    final var blockHeader = mockBlockchainAndWorldState(callParameter);
    mockProtocolSpecForProcessWithWorldUpdater();

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .maxFeePerGas(callParameter.getMaxFeePerGas().orElseThrow())
            .maxPriorityFeePerGas(callParameter.getMaxPriorityFeePerGas().orElseThrow())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .gasLimit(blockHeader.getGasLimit())
            .build();

    // call process with original transaction
    defaultCappedTransactionSimulator.process(
        callParameter, TransactionValidationParams.transactionSimulator(), NO_TRACING, 1L);

    // expect transaction with the block gas limit to be processed
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @ParameterizedTest
  @MethodSource("shouldUseTxGasLimitCapWhenWhenGasLimitNotPresent")
  public void shouldUseTxGasLimitCapWhenWhenGasLimitNotPresent(
      final RpcGasCapVariant rpcGasCapVariant,
      final long blockGasLimit,
      final long txGasLimitCap,
      final long expectedGasLimit) {
    final CallParameter callParameter =
        eip1559TransactionCallParameterBuilder().gas(OptionalLong.empty()).build();

    final BlockHeader blockHeader = mockBlockHeader(Hash.ZERO, 1L, Wei.ONE, blockGasLimit);

    mockBlockchainAndWorldState(callParameter, blockHeader);
    mockProtocolSpecForProcessWithWorldUpdater(txGasLimitCap);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .maxFeePerGas(callParameter.getMaxFeePerGas().orElseThrow())
            .maxPriorityFeePerGas(callParameter.getMaxPriorityFeePerGas().orElseThrow())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .gasLimit(expectedGasLimit)
            .build();

    final var simulator =
        switch (rpcGasCapVariant) {
          case DEFAULT -> defaultCappedTransactionSimulator;
          case UNCAPPED -> uncappedTransactionSimulator;
          case CAPPED -> cappedTransactionSimulator;
        };

    simulator.process(
        callParameter, TransactionValidationParams.transactionSimulator(), NO_TRACING, 1L);

    verifyTransactionWasProcessed(expectedTransaction);
  }

  private enum RpcGasCapVariant {
    DEFAULT,
    CAPPED,
    UNCAPPED;
  }

  private static Stream<Arguments> shouldUseTxGasLimitCapWhenWhenGasLimitNotPresent() {
    return Stream.of(
        Arguments.of(
            RpcGasCapVariant.DEFAULT,
            DEFAULT_BLOCK_GAS_LIMIT,
            DEFAULT_BLOCK_GAS_LIMIT - 1,
            DEFAULT_BLOCK_GAS_LIMIT - 1),
        Arguments.of(
            RpcGasCapVariant.DEFAULT,
            DEFAULT_BLOCK_GAS_LIMIT,
            DEFAULT_BLOCK_GAS_LIMIT + 1,
            DEFAULT_BLOCK_GAS_LIMIT),
        Arguments.of(
            RpcGasCapVariant.CAPPED,
            DEFAULT_BLOCK_GAS_LIMIT,
            DEFAULT_BLOCK_GAS_LIMIT - 1,
            RPC_GAS_CAP),
        Arguments.of(
            RpcGasCapVariant.CAPPED,
            DEFAULT_BLOCK_GAS_LIMIT,
            DEFAULT_BLOCK_GAS_LIMIT + 1,
            RPC_GAS_CAP),
        Arguments.of(
            RpcGasCapVariant.UNCAPPED,
            DEFAULT_BLOCK_GAS_LIMIT,
            DEFAULT_BLOCK_GAS_LIMIT - 1,
            DEFAULT_BLOCK_GAS_LIMIT - 1),
        Arguments.of(
            RpcGasCapVariant.UNCAPPED,
            DEFAULT_BLOCK_GAS_LIMIT,
            DEFAULT_BLOCK_GAS_LIMIT + 1,
            DEFAULT_BLOCK_GAS_LIMIT));
  }

  @Test
  public void shouldReturnSuccessfulResultWhenBlobTransactionProcessingIsSuccessful() {
    final CallParameter callParameter = blobTransactionCallParameter();
    mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction = buildExpectedTransaction(callParameter);
    assertThat(callParameter).isEqualTo(CallParameter.fromTransaction(expectedTransaction));

    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(callParameter, 1L);

    assertThat(result.get().isSuccessful()).isTrue();
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldReturnFailureResultWhenBlobTransactionProcessingFails() {
    final CallParameter callParameter = blobTransactionCallParameter();
    mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction = buildExpectedTransaction(callParameter);
    assertThat(callParameter).isEqualTo(CallParameter.fromTransaction(expectedTransaction));

    mockProcessorStatusForTransaction(expectedTransaction, Status.FAILED);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(callParameter, 1L);

    assertThat(result.get().isSuccessful()).isFalse();
    verifyTransactionWasProcessed(expectedTransaction);
  }

  @Test
  public void shouldSimulateLegacyTransactionWhenBaseFeeNotZero() {
    // tests that the transaction simulator will simulate a legacy transaction when the base fee is
    // not zero and the transaction is a legacy transaction
    final CallParameter callParameter = legacyTransactionCallParameterBuilder().build();

    final var blockHeader = mockBlockchainAndWorldState(callParameter);

    final Transaction expectedTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(1L)
            .gasPrice(callParameter.getGasPrice().orElse(Wei.ZERO))
            .gasLimit(blockHeader.getGasLimit())
            .to(callParameter.getTo().orElseThrow())
            .sender(callParameter.getSender().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .build();
    mockProcessorStatusForTransaction(expectedTransaction, Status.SUCCESSFUL);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(callParameter, 1L);

    verifyTransactionWasProcessed(expectedTransaction);
    assertThat(result.get().isSuccessful()).isTrue();
  }

  @Test
  public void shouldGuessDelegateCodeTransactionTypeWhenAuthorizationsPresent() {
    final CodeDelegation delegation =
        new CodeDelegation(BigInteger.ONE, Address.fromHexString("0x1"), 42L, FAKE_SIGNATURE);

    final CallParameter callParameter =
        codeDelegationTransactionCallParamterBuilder(List.of(delegation)).build();

    final BlockHeader blockHeader = mockBlockchainAndWorldState(callParameter);

    mockProtocolSpecForProcessWithWorldUpdater();

    final Transaction expectedTx =
        Transaction.builder()
            .chainId(BigInteger.ONE)
            .nonce(1L)
            .gasLimit(blockHeader.getGasLimit())
            .sender(callParameter.getSender().orElseThrow())
            .to(callParameter.getTo().orElseThrow())
            .value(callParameter.getValue().orElseThrow())
            .payload(callParameter.getPayload().orElseThrow())
            .signature(FAKE_SIGNATURE)
            .codeDelegations(List.of(delegation))
            .guessType()
            .build();

    mockProcessorStatusForTransaction(expectedTx, Status.SUCCESSFUL);

    final Optional<TransactionSimulatorResult> result =
        uncappedTransactionSimulator.process(
            callParameter,
            Optional.empty(),
            TransactionValidationParams.transactionSimulator(),
            OperationTracer.NO_TRACING,
            blockHeader);

    assertThat(result).isPresent();
    assertThat(result.get().transaction().getType()).isEqualTo(TransactionType.DELEGATE_CODE);
  }

  private BlockHeader mockBlockchainAndWorldState(final CallParameter callParameter) {
    final BlockHeader blockHeader =
        mockBlockHeader(Hash.ZERO, 1L, Wei.ONE, DEFAULT_BLOCK_GAS_LIMIT);
    mockBlockchainAndWorldState(callParameter, blockHeader);
    return blockHeader;
  }

  private void mockBlockchainAndWorldState(
      final CallParameter callParameter, final BlockHeader blockHeader) {
    mockBlockchainForBlockHeader(blockHeader);
    mockWorldStateForAccount(blockHeader, callParameter.getSender().orElseThrow(), 1L);
  }

  private Transaction buildExpectedTransaction(final CallParameter callParameter) {
    return Transaction.builder()
        .type(TransactionType.BLOB)
        .chainId(callParameter.getChainId().orElseThrow())
        .nonce(callParameter.getNonce().orElseThrow())
        .gasLimit(callParameter.getGas().orElseThrow())
        .maxFeePerGas(callParameter.getMaxFeePerGas().orElseThrow())
        .maxPriorityFeePerGas(callParameter.getMaxPriorityFeePerGas().orElseThrow())
        .to(callParameter.getTo().orElseThrow())
        .sender(callParameter.getSender().orElseThrow())
        .value(callParameter.getValue().orElseThrow())
        .payload(callParameter.getPayload().orElseThrow())
        .maxFeePerBlobGas(callParameter.getMaxFeePerBlobGas().orElseThrow())
        .versionedHashes(callParameter.getBlobVersionedHashes().orElseThrow())
        .signature(FAKE_SIGNATURE)
        .build();
  }

  private void mockWorldStateForAccount(
      final BlockHeader blockHeader, final Address address, final long nonce) {
    final Account account = mock(Account.class);
    when(account.getNonce()).thenReturn(nonce);
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader)))
        .thenReturn(Optional.of(worldState));
    final WorldUpdater updater = mock(WorldUpdater.class);
    when(updater.get(address)).thenReturn(account);
    when(worldState.updater()).thenReturn(updater);
  }

  private void mockWorldStateForAbsentAccount(final BlockHeader blockHeader) {
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader)))
        .thenReturn(Optional.of(worldState));
    final WorldUpdater updater = mock(WorldUpdater.class);
    when(updater.get(any())).thenReturn(null);
    when(worldState.updater()).thenReturn(updater);
  }

  private BlockHeader mockBlockHeader(
      final Hash stateRoot, final long blockNumber, final Wei baseFee, final long gasLimit) {
    return blockHeaderTestFixture
        .stateRoot(stateRoot)
        .number(blockNumber)
        .baseFeePerGas(baseFee)
        .gasLimit(gasLimit)
        .difficulty(Difficulty.ONE)
        .buildHeader();
  }

  private void mockBlockchainForBlockHeader(final BlockHeader blockHeader) {
    when(blockchain.getBlockHeader(blockHeader.getNumber())).thenReturn(Optional.of(blockHeader));
    when(blockchain.getBlockHeader(blockHeader.getBlockHash()))
        .thenReturn(Optional.of(blockHeader));
    when(blockchain.getChainHeadHash()).thenReturn(blockHeader.getHash());
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);
  }

  private void mockProtocolSpecForProcessWithWorldUpdater() {
    mockProtocolSpecForProcessWithWorldUpdater(
        GasLimitCalculator.constant().transactionGasLimitCap());
  }

  private void mockProtocolSpecForProcessWithWorldUpdater(final long txGasLimitCap) {
    final BlockHeaderFunctions blockHeaderFunctions = mock(BlockHeaderFunctions.class);
    final PreExecutionProcessor preExecutionProcessor = mock(PreExecutionProcessor.class);
    when(protocolSchedule.getChainId()).thenReturn(Optional.of(BigInteger.ONE));
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(protocolSchedule.getForNextBlockHeader(any(), anyLong())).thenReturn(protocolSpec);
    when(protocolSpec.getTransactionProcessor()).thenReturn(transactionProcessor);
    when(protocolSpec.getMiningBeneficiaryCalculator()).thenReturn(BlockHeader::getCoinbase);
    when(protocolSpec.getBlockHeaderFunctions()).thenReturn(blockHeaderFunctions);
    when(protocolSpec.getFeeMarket()).thenReturn(FeeMarket.london(0));
    when(protocolSpec.getPreExecutionProcessor()).thenReturn(preExecutionProcessor);
    when(protocolSpec.getGasCalculator()).thenReturn(new FrontierGasCalculator());
    when(protocolSpec.getGasLimitCalculator()).thenReturn(gasLimitCalculator);
    when(protocolSpec.getDifficultyCalculator()).thenReturn((time, parent) -> BigInteger.TEN);
    when(gasLimitCalculator.transactionGasLimitCap()).thenReturn(txGasLimitCap);
    when(gasLimitCalculator.nextGasLimit(anyLong(), anyLong(), anyLong()))
        .thenReturn(DEFAULT_BLOCK_GAS_LIMIT);
  }

  private void mockProcessorStatusForTransaction(
      final Transaction transaction, final Status status) {
    mockProtocolSpecForProcessWithWorldUpdater();
    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);
    switch (status) {
      case SUCCESSFUL:
        when(result.isSuccessful()).thenReturn(true);
        break;
      case INVALID:
      case FAILED:
        when(result.isSuccessful()).thenReturn(false);
        break;
    }

    when(transactionProcessor.processTransaction(
            any(), any(), eq(transaction), any(), any(), any(), any(), any(Wei.class), any()))
        .thenReturn(result);
  }

  private void verifyTransactionWasProcessed(final Transaction expectedTransaction) {
    verify(transactionProcessor)
        .processTransaction(
            any(),
            any(),
            eq(expectedTransaction),
            any(),
            any(),
            any(),
            any(),
            any(Wei.class),
            any());
  }

  private ImmutableCallParameter.Builder legacyTransactionCallParameterBuilder() {
    return ImmutableCallParameter.builder()
        .sender(Address.fromHexString("0x0"))
        .to(Address.fromHexString("0x0"))
        .gasPrice(Wei.ZERO)
        .value(Wei.ZERO)
        .input(Bytes.EMPTY);
  }

  private ImmutableCallParameter.Builder eip1559TransactionCallParameterBuilder() {
    return legacyTransactionCallParameterBuilder()
        .gasPrice(Optional.empty())
        .maxFeePerGas(Wei.ZERO)
        .maxPriorityFeePerGas(Wei.ZERO);
  }

  private ImmutableCallParameter.Builder codeDelegationTransactionCallParamterBuilder(
      final List<CodeDelegation> delegations) {
    return legacyTransactionCallParameterBuilder().codeDelegationAuthorizations(delegations);
  }

  private CallParameter blobTransactionCallParameter() {
    BlobsWithCommitments bwc = new BlobTestFixture().createBlobsWithCommitments(3);
    return eip1559TransactionCallParameterBuilder()
        .nonce(1)
        .chainId(BigInteger.ONE)
        .maxFeePerGas(Wei.ZERO)
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerBlobGas(Wei.ZERO)
        .gas(0L)
        .blobVersionedHashes(bwc.getVersionedHashes())
        .build();
  }
}
