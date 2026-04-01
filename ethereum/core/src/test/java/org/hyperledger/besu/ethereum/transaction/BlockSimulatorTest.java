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
package org.hyperledger.besu.ethereum.transaction;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.blockhash.PreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallException;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockOverrides;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BlockSimulatorTest {

  @Mock private WorldStateArchive worldStateArchive;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private TransactionSimulator transactionSimulator;
  @Mock private MiningConfiguration miningConfiguration;
  @Mock private MutableWorldState mutableWorldState;
  @Mock private Blockchain blockchain;
  @Mock private WorldUpdater updater;
  @Mock private ProtocolSpec protocolSpec;

  private BlockHeader blockHeader;
  private BlockSimulator blockSimulator;

  @BeforeEach
  public void setUp() {
    blockSimulator =
        new BlockSimulator(
            worldStateArchive,
            protocolSchedule,
            transactionSimulator,
            miningConfiguration,
            blockchain,
            0);
    blockHeader = BlockHeaderBuilder.createDefault().buildBlockHeader();
    when(miningConfiguration.getCoinbase()).thenReturn(Optional.of(Address.fromHexString("0x1")));
    when(protocolSchedule.getForNextBlockHeader(any(), anyLong())).thenReturn(protocolSpec);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(protocolSpec.getMiningBeneficiaryCalculator())
        .thenReturn(mock(MiningBeneficiaryCalculator.class));
    GasLimitCalculator gasLimitCalculator = mock(GasLimitCalculator.class);
    when(protocolSpec.getGasLimitCalculator()).thenReturn(gasLimitCalculator);
    when(gasLimitCalculator.nextGasLimit(anyLong(), anyLong(), anyLong())).thenReturn(1L);
    when(protocolSpec.getFeeMarket()).thenReturn(mock(FeeMarket.class));
    when(protocolSpec.getPreExecutionProcessor()).thenReturn(mock(PreExecutionProcessor.class));
    when(protocolSpec.getSlotDuration()).thenReturn(Duration.ofSeconds(12));
    when(gasLimitCalculator.computeExcessBlobGas(anyLong(), anyLong(), anyLong())).thenReturn(0L);
  }

  @Test
  public void shouldProcessWithValidWorldState() {

    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader)))
        .thenReturn(Optional.of(mutableWorldState));

    List<BlockSimulationResult> results =
        blockSimulator.process(blockHeader, BlockSimulationParameter.EMPTY);
    assertNotNull(results);
    verify(worldStateArchive).getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader));
  }

  @Test
  public void shouldNotProcessWithInvalidWorldState() {
    when(worldStateArchive.getWorldState(any(WorldStateQueryParams.class)))
        .thenAnswer(invocation -> Optional.empty());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> blockSimulator.process(blockHeader, BlockSimulationParameter.EMPTY));

    assertEquals(
        String.format("Public world state not available for block %s", blockHeader.toLogString()),
        exception.getMessage());
  }

  @Test
  public void shouldStopWhenTransactionSimulationIsInvalid() {
    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader)))
        .thenReturn(Optional.of(mutableWorldState));
    when(mutableWorldState.updater()).thenReturn(updater);

    CallParameter callParameter = mock(CallParameter.class);
    when(callParameter.getGas()).thenReturn(OptionalLong.empty());
    BlockStateCall blockStateCall = new BlockStateCall(List.of(callParameter), null, null);

    TransactionSimulatorResult transactionSimulatorResult = mock(TransactionSimulatorResult.class);
    when(transactionSimulatorResult.isInvalid()).thenReturn(true);
    when(transactionSimulatorResult.getInvalidReason())
        .thenReturn(Optional.of("Invalid Transaction"));
    when(transactionSimulatorResult.getValidationResult())
        .thenReturn(
            ValidationResult.invalid(
                TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE, "Invalid Transaction"));
    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), anyLong(), any(), any(), any(), any(),
            any()))
        .thenReturn(Optional.of(transactionSimulatorResult));

    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () ->
                blockSimulator.process(
                    blockHeader, createSimulationParameter(blockStateCall), mutableWorldState));

    assertThat(exception.getError()).isEqualTo(BlockStateCallError.UPFRONT_COST_EXCEEDS_BALANCE);
    assertEquals("Invalid Transaction", exception.getMessage());
  }

  @Test
  public void shouldStopWhenTransactionSimulationIsEmpty() {

    when(worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader)))
        .thenReturn(Optional.of(mutableWorldState));
    when(mutableWorldState.updater()).thenReturn(updater);

    CallParameter callParameter = mock(CallParameter.class);
    when(callParameter.getGas()).thenReturn(OptionalLong.empty());
    BlockStateCall blockStateCall = new BlockStateCall(List.of(callParameter), null, null);

    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), anyLong(), any(), any(), any(), any(),
            any()))
        .thenReturn(Optional.empty());

    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () ->
                blockSimulator.process(
                    blockHeader, createSimulationParameter(blockStateCall), mutableWorldState));

    assertEquals("Transaction simulation returned no result", exception.getMessage());
  }

  @Test
  public void shouldApplyStateOverridesCorrectly() {
    StateOverrideMap stateOverrideMap = new StateOverrideMap();
    Address address = mock(Address.class);
    StateOverride stateOverride =
        new StateOverride.Builder()
            .withBalance(Wei.of(456L))
            .withNonce(new UnsignedLongParameter(123L))
            .withCode("")
            .withStateDiff(Map.of("0x0", "0x1"))
            .build();

    stateOverrideMap.put(address, stateOverride);

    WorldUpdater worldUpdater = mock(WorldUpdater.class);
    when(mutableWorldState.updater()).thenReturn(worldUpdater);

    MutableAccount mutableAccount = mock(MutableAccount.class);
    when(mutableAccount.getAddress()).thenReturn(address);
    when(worldUpdater.getOrCreate(address)).thenReturn(mutableAccount);

    blockSimulator.applyStateOverrides(stateOverrideMap, mutableWorldState);

    verify(mutableAccount).setNonce(anyLong());
    verify(mutableAccount).setBalance(any(Wei.class));
    verify(mutableAccount).setCode(any(Bytes.class));
    verify(mutableAccount).setStorageValue(any(UInt256.class), any(UInt256.class));
  }

  @Test
  public void shouldOverrideBlockHeaderCorrectly() {
    FeeMarket feeMarket = mock(FeeMarket.class);
    when(feeMarket.implementsBaseFee()).thenReturn(true);
    when(protocolSpec.getFeeMarket()).thenReturn(feeMarket);
    when(protocolSpec.isPoS()).thenReturn(true);

    var expectedTimestamp = 1L;
    var expectedBlockNumber = 2L;
    var expectedFeeRecipient = Address.fromHexString("0x1");
    var expectedBaseFeePerGas = Wei.of(7L);
    var expectedGasLimit = 5L;
    var expectedDifficulty = BigInteger.ONE;
    var expectedMixHashOrPrevRandao = Hash.hash(Bytes.fromHexString("0x01"));
    var expectedPrevRandao = Hash.hash(Bytes.fromHexString("0x01"));
    var expectedParentBeaconBlockRoot =
        Bytes32.wrap(Hash.hash(Bytes.fromHexString("0x03")).getBytes());
    var expectedExtraData = Bytes.fromHexString("0x02");

    BlockOverrides blockOverrides =
        BlockOverrides.builder()
            .timestamp(expectedTimestamp)
            .blockNumber(expectedBlockNumber)
            .feeRecipient(expectedFeeRecipient)
            .baseFeePerGas(expectedBaseFeePerGas)
            .gasLimit(expectedGasLimit)
            .difficulty(expectedDifficulty)
            .mixHashOrPrevRandao(Bytes32.wrap(expectedMixHashOrPrevRandao.getBytes()))
            .extraData(expectedExtraData)
            .parentBeaconBlockRoot(expectedParentBeaconBlockRoot)
            .build();

    BlockHeader result =
        blockSimulator.overrideBlockHeader(blockHeader, protocolSpec, blockOverrides, true);

    assertNotNull(result);
    assertEquals(expectedTimestamp, result.getTimestamp());
    assertEquals(expectedBlockNumber, result.getNumber());
    assertEquals(expectedFeeRecipient, result.getCoinbase());
    assertEquals(Optional.of(expectedBaseFeePerGas), result.getBaseFee());
    assertEquals(expectedGasLimit, result.getGasLimit());
    assertThat(result.getDifficulty()).isEqualTo(Difficulty.of(expectedDifficulty));
    assertEquals(expectedMixHashOrPrevRandao, result.getMixHash());
    assertEquals(expectedPrevRandao.getBytes(), result.getPrevRandao().get());
    assertEquals(expectedExtraData, result.getExtraData());
  }

  @Test
  public void shouldInheritFeeRecipientFromParentBlock() {
    // When feeRecipient is set on the first block, subsequent blocks without feeRecipient
    // should inherit from the parent block's coinbase, regardless of mining configuration.

    var expectedFeeRecipient = Address.fromHexString("0xc200000000000000000000000000000000000000");

    // Block 1: with feeRecipient override
    BlockOverrides block1Overrides =
        BlockOverrides.builder()
            .timestamp(1L)
            .blockNumber(1L)
            .feeRecipient(expectedFeeRecipient)
            .build();

    BlockHeader block1Header =
        blockSimulator.overrideBlockHeader(blockHeader, protocolSpec, block1Overrides, false);
    assertEquals(expectedFeeRecipient, block1Header.getCoinbase());

    // Block 2: no feeRecipient override — should inherit from block 1
    BlockOverrides block2Overrides =
        BlockOverrides.builder().timestamp(13L).blockNumber(2L).build();

    BlockHeader block2Header =
        blockSimulator.overrideBlockHeader(block1Header, protocolSpec, block2Overrides, false);
    assertEquals(expectedFeeRecipient, block2Header.getCoinbase());
  }

  @Test
  public void shouldDetectInvalidPrecompile() {
    var stateOverrideMap = new StateOverrideMap();
    var targetAddress = Address.fromHexString("0x3");
    var precompileAddress = Address.fromHexString("0x1");

    var stateOverride =
        new StateOverride.Builder().withMovePrecompileToAddress(targetAddress).build();

    stateOverrideMap.put(precompileAddress, stateOverride);

    var validationResult = buildParameterWithOverrides(stateOverrideMap).validate(Set.of());

    assertThat(validationResult).isPresent();
    assertThat(validationResult.orElseThrow())
        .isEqualTo(BlockStateCallError.INVALID_PRECOMPILE_ADDRESS);
  }

  @Test
  public void shouldAllowDuplicatePrecompileTargetAddresses() {
    var stateOverrideMap = new StateOverrideMap();
    var targetAddress = Address.fromHexString("0x3");
    var precompileAddress1 = Address.fromHexString("0x1");
    var precompileAddress2 = Address.fromHexString("0x2");

    var stateOverride =
        new StateOverride.Builder().withMovePrecompileToAddress(targetAddress).build();

    // Map two precompile addresses to the same target address - should be allowed
    stateOverrideMap.put(precompileAddress1, stateOverride);
    stateOverrideMap.put(precompileAddress2, stateOverride);

    var validationResult =
        buildParameterWithOverrides(stateOverrideMap)
            .validate(Set.of(precompileAddress1, precompileAddress2));

    assertThat(validationResult).isEmpty();
  }

  @Test
  public void shouldThrowBlockGasLimitExceededWhenTxGasExceedsBlockLimitWithValidationDisabled() {
    when(mutableWorldState.updater()).thenReturn(updater);

    // gasLimitCalculator.nextGasLimit returns 1L (from setUp), so block gas limit = 1
    CallParameter callParameter = mock(CallParameter.class);
    when(callParameter.getGas()).thenReturn(OptionalLong.of(1_000_000L));
    BlockStateCall blockStateCall = new BlockStateCall(List.of(callParameter), null, null);

    BlockSimulationParameter parameter =
        new BlockSimulationParameter.BlockSimulationParameterBuilder()
            .blockStateCalls(List.of(blockStateCall))
            .validation(false)
            .build();

    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () -> blockSimulator.process(blockHeader, parameter, mutableWorldState));

    assertThat(exception.getError()).isEqualTo(BlockStateCallError.BLOCK_GAS_LIMIT_EXCEEDED);
    assertThat(exception.getError().getCode()).isEqualTo(-38015);
  }

  @Test
  public void shouldThrowBlockGasLimitExceededWhenTxGasExceedsBlockLimitWithValidationEnabled() {
    when(mutableWorldState.updater()).thenReturn(updater);

    // gasLimitCalculator.nextGasLimit returns 1L (from setUp), so block gas limit = 1
    CallParameter callParameter = mock(CallParameter.class);
    when(callParameter.getGas()).thenReturn(OptionalLong.of(1_000_000L));
    BlockStateCall blockStateCall = new BlockStateCall(List.of(callParameter), null, null);

    BlockSimulationParameter parameter =
        new BlockSimulationParameter.BlockSimulationParameterBuilder()
            .blockStateCalls(List.of(blockStateCall))
            .validation(true)
            .build();

    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () -> blockSimulator.process(blockHeader, parameter, mutableWorldState));

    assertThat(exception.getError()).isEqualTo(BlockStateCallError.BLOCK_GAS_LIMIT_EXCEEDED);
    assertThat(exception.getError().getCode()).isEqualTo(-38015);
  }

  @Test
  public void
      shouldThrowBlockGasLimitExceededWhenSecondTxGasExceedsRemainingAfterFirstTxConsumed() {
    // Block gas limit = 30,000. First tx consumes 21,000 (leaving 9,000 remaining).
    // Second tx explicitly requests 10,000 gas, which exceeds the 9,000 remaining.
    GasLimitCalculator gasLimitCalculator = mock(GasLimitCalculator.class);
    when(protocolSpec.getGasLimitCalculator()).thenReturn(gasLimitCalculator);
    when(gasLimitCalculator.nextGasLimit(anyLong(), anyLong(), anyLong())).thenReturn(30_000L);
    when(gasLimitCalculator.computeExcessBlobGas(anyLong(), anyLong(), anyLong())).thenReturn(0L);

    WorldUpdater transactionUpdater = mock(WorldUpdater.class);
    when(mutableWorldState.updater()).thenReturn(updater);
    when(updater.updater()).thenReturn(transactionUpdater);

    CallParameter firstCallParam = mock(CallParameter.class);
    when(firstCallParam.getGas()).thenReturn(OptionalLong.empty());
    when(firstCallParam.getGasPrice()).thenReturn(Optional.empty());
    when(firstCallParam.getMaxFeePerGas()).thenReturn(Optional.empty());
    when(firstCallParam.getMaxPriorityFeePerGas()).thenReturn(Optional.empty());

    CallParameter secondCallParam = mock(CallParameter.class);
    when(secondCallParam.getGas()).thenReturn(OptionalLong.of(10_000L));

    BlockStateCall blockStateCall =
        new BlockStateCall(List.of(firstCallParam, secondCallParam), null, null);

    Transaction tx = mock(Transaction.class);
    when(tx.getType()).thenReturn(TransactionType.FRONTIER);

    TransactionProcessingResult processingResult = mock(TransactionProcessingResult.class);
    when(processingResult.getPartialBlockAccessView()).thenReturn(Optional.empty());

    TransactionSimulatorResult firstTxResult = mock(TransactionSimulatorResult.class);
    when(firstTxResult.isInvalid()).thenReturn(false);
    when(firstTxResult.getGasEstimate()).thenReturn(21_000L);
    when(firstTxResult.transaction()).thenReturn(tx);
    when(firstTxResult.result()).thenReturn(processingResult);

    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), anyLong(), any(), any(), any(), any(),
            any()))
        .thenReturn(Optional.of(firstTxResult));

    AbstractBlockProcessor.TransactionReceiptFactory receiptFactory =
        mock(AbstractBlockProcessor.TransactionReceiptFactory.class);
    when(protocolSpec.getTransactionReceiptFactory()).thenReturn(receiptFactory);
    TransactionReceipt receipt = mock(TransactionReceipt.class);
    when(receipt.getLogsList()).thenReturn(List.of());
    when(receiptFactory.create(any(TransactionType.class), any(), any(), anyLong()))
        .thenReturn(receipt);

    BlockSimulationParameter parameter =
        new BlockSimulationParameter.BlockSimulationParameterBuilder()
            .blockStateCalls(List.of(blockStateCall))
            .validation(false)
            .build();

    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () -> blockSimulator.process(blockHeader, parameter, mutableWorldState));

    assertThat(exception.getError()).isEqualTo(BlockStateCallError.BLOCK_GAS_LIMIT_EXCEEDED);
    assertThat(exception.getError().getCode()).isEqualTo(-38015);
  }

  private BlockSimulationParameter createSimulationParameter(final BlockStateCall blockStateCall) {
    return new BlockSimulationParameter.BlockSimulationParameterBuilder()
        .blockStateCalls(List.of(blockStateCall))
        .build();
  }

  private BlockSimulationParameter buildParameterWithOverrides(
      final StateOverrideMap stateOverrideMap) {
    var blockStateCall = new BlockStateCall(List.of(), null, stateOverrideMap);
    var parameter =
        new BlockSimulationParameter.BlockSimulationParameterBuilder()
            .blockStateCalls(List.of(blockStateCall))
            .build();
    return new BlockSimulationParameter(parameter.getBlockStateCalls(), false, false, false);
  }
}
