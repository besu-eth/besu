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
package org.hyperledger.besu.ethereum.transaction.pluginadapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallException;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.data.TransactionSimulationResult;
import org.hyperledger.besu.plugin.services.TransactionSimulationService.SimulationParameters;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TransactionSimulationServiceImplTest {
  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  @Nested
  class WithMockedSimulator {
    private final Blockchain blockchain = mock(Blockchain.class);
    private final TransactionSimulator transactionSimulator = mock(TransactionSimulator.class);
    private final BlockHeader header = new BlockHeaderTestFixture().number(8).buildHeader();
    private final Hash blockHash = header.getHash();
    private final CallParameter callParameter =
        ImmutableCallParameter.builder().to(Address.fromHexString("0x01")).build();
    private final OperationTracer tracer = mock(OperationTracer.class);
    private TransactionSimulationServiceImpl service;

    @BeforeEach
    void setUp() {
      service = new TransactionSimulationServiceImpl();
      service.init(blockchain, transactionSimulator);
    }

    @Test
    void processesCallOnTheBlockHeaderAndWrapsTheExecutedTransaction() {
      final Optional<StateOverrideMap> overrides = Optional.of(new StateOverrideMap());
      final Transaction executed = mock(Transaction.class);
      when(executed.getGasLimit()).thenReturn(100_000L);
      final TransactionProcessingResult result = mock(TransactionProcessingResult.class);
      when(result.getGasRemaining()).thenReturn(78_000L);
      final TransactionSimulatorResult simulatorResult =
          new TransactionSimulatorResult(executed, result);
      when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
      when(transactionSimulator.process(
              eq(callParameter),
              eq(overrides),
              eq(TransactionValidationParams.transactionSimulatorAllowExceedingBalance()),
              eq(tracer),
              eq(header)))
          .thenReturn(Optional.of(simulatorResult));

      final Optional<TransactionSimulationResult> maybeResult =
          service.simulate(
              callParameter,
              overrides,
              blockHash,
              tracer,
              EnumSet.of(SimulationParameters.ALLOW_EXCEEDING_BALANCE));

      assertThat(maybeResult).isPresent();
      assertThat(maybeResult.get().transaction()).isSameAs(executed);
      assertThat(maybeResult.get().result()).isSameAs(result);
      assertThat(maybeResult.get().getGasEstimate())
          .isEqualTo(simulatorResult.getGasEstimate())
          .isEqualTo(22_000L);
    }

    @Test
    void futureNonceParameterMapsToTheCorePendingValidationParams() {
      when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
      when(transactionSimulator.process(
              any(), any(), any(TransactionValidationParams.class), any(), any(BlockHeader.class)))
          .thenReturn(Optional.empty());

      service.simulate(
          callParameter,
          Optional.empty(),
          blockHash,
          tracer,
          EnumSet.of(
              SimulationParameters.ALLOW_EXCEEDING_BALANCE,
              SimulationParameters.ALLOW_FUTURE_NONCE));

      verify(transactionSimulator)
          .process(
              callParameter,
              Optional.empty(),
              TransactionValidationParams.transactionSimulatorAllowExceedingBalanceAndFutureNonce(),
              tracer,
              header);
    }

    @Test
    void noParametersMapToTheCoreSimulatorValidationParams() {
      when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
      when(transactionSimulator.process(
              any(), any(), any(TransactionValidationParams.class), any(), any(BlockHeader.class)))
          .thenReturn(Optional.empty());

      service.simulate(
          callParameter,
          Optional.empty(),
          blockHash,
          tracer,
          EnumSet.noneOf(SimulationParameters.class));

      verify(transactionSimulator)
          .process(
              callParameter,
              Optional.empty(),
              TransactionValidationParams.transactionSimulator(),
              tracer,
              header);
    }

    @Test
    void fallsBackToTheSafeHeaderLookup() {
      when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.empty());
      when(blockchain.getBlockHeaderSafe(blockHash)).thenReturn(Optional.of(header));
      when(transactionSimulator.process(
              any(), any(), any(TransactionValidationParams.class), any(), any(BlockHeader.class)))
          .thenReturn(Optional.empty());

      assertThat(
              service.simulate(
                  callParameter,
                  Optional.empty(),
                  blockHash,
                  tracer,
                  EnumSet.noneOf(SimulationParameters.class)))
          .isEmpty();
      verify(transactionSimulator)
          .process(any(), any(), any(TransactionValidationParams.class), any(), eq(header));
    }

    @Test
    void unknownBlockIsEmpty() {
      when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.empty());
      when(blockchain.getBlockHeaderSafe(blockHash)).thenReturn(Optional.empty());

      final Optional<TransactionSimulationResult> maybeResult =
          service.simulate(
              callParameter,
              Optional.empty(),
              blockHash,
              tracer,
              EnumSet.noneOf(SimulationParameters.class));

      assertThat(maybeResult).isEmpty();
      verifyNoInteractions(transactionSimulator);
    }

    @Test
    void transactionOverloadStillReportsUnknownBlockAsBlockNotFound() {
      when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.empty());
      when(blockchain.getBlockHeaderSafe(blockHash)).thenReturn(Optional.empty());
      final Transaction transaction = new TransactionTestFixture().createTransaction(KEY_PAIR);

      final Optional<TransactionSimulationResult> maybeResult =
          service.simulate(
              transaction,
              Optional.empty(),
              blockHash,
              tracer,
              EnumSet.noneOf(SimulationParameters.class));

      assertThat(maybeResult).isPresent();
      assertThat(maybeResult.get().getInvalidReason())
          .contains(TransactionInvalidReason.BLOCK_NOT_FOUND.name());
      assertThat(maybeResult.get().transaction()).isSameAs(transaction);
    }

    @Test
    void unavailableWorldStateIsEmpty() {
      when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
      when(transactionSimulator.process(
              any(), any(), any(TransactionValidationParams.class), any(), any(BlockHeader.class)))
          .thenReturn(Optional.empty());

      assertThat(
              service.simulate(
                  callParameter,
                  Optional.empty(),
                  blockHash,
                  tracer,
                  EnumSet.noneOf(SimulationParameters.class)))
          .isEmpty();
      verify(transactionSimulator, never()).processOnPending(any(), any(), any(), any(), any());
    }

    @Test
    void invalidStateOverrideIsAnIllegalArgumentException() {
      when(blockchain.getBlockHeader(blockHash)).thenReturn(Optional.of(header));
      final BlockStateCallException cause =
          new BlockStateCallException(
              "not a precompile", BlockStateCallError.INVALID_PRECOMPILE_ADDRESS);
      when(transactionSimulator.process(
              any(), any(), any(TransactionValidationParams.class), any(), any(BlockHeader.class)))
          .thenThrow(cause);

      assertThatThrownBy(
              () ->
                  service.simulate(
                      callParameter,
                      Optional.of(new StateOverrideMap()),
                      blockHash,
                      tracer,
                      EnumSet.noneOf(SimulationParameters.class)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("not a precompile")
          .hasCause(cause);
    }
  }

  /**
   * Runs the same calls through the service, with a call parameter implemented outside core as a
   * plugin would, and directly through the transaction simulator with a core call parameter, on the
   * hive test chain, and checks that the results match.
   */
  @Nested
  class EquivalenceWithDirectSimulatorCall {
    private static final Address SENDER =
        Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b");
    private static final Address CONTRACT =
        Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f");
    private static final Address OTHER =
        Address.fromHexString("0x8888f1f195afa192cfee860698584c030f4c9db1");

    private BlockchainSetupUtil setup;
    private MutableBlockchain blockchain;
    private TransactionSimulator transactionSimulator;
    private TransactionSimulationServiceImpl service;

    @BeforeEach
    void setUp() {
      setup = BlockchainSetupUtil.forHiveTesting(DataStorageFormat.BONSAI);
      setup.importAllBlocks();
      blockchain = setup.getBlockchain();
      transactionSimulator =
          new TransactionSimulator(
              blockchain,
              setup.getWorldArchive(),
              setup.getProtocolSchedule(),
              ImmutableMiningConfiguration.newDefault(),
              0L);
      service = new TransactionSimulationServiceImpl();
      service.init(blockchain, transactionSimulator);
    }

    private List<PluginCallParameter> calls() {
      return List.of(
          // a contract call
          new PluginCallParameter(
              Optional.of(SENDER),
              Optional.of(CONTRACT),
              OptionalLong.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(Bytes.fromHexString("0x12a7b914"))),
          // a plain transfer
          new PluginCallParameter(
              Optional.of(SENDER),
              Optional.of(OTHER),
              OptionalLong.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty()),
          // a call from a contract address
          new PluginCallParameter(
              Optional.of(CONTRACT),
              Optional.of(SENDER),
              OptionalLong.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(Bytes.fromHexString("0x123456"))),
          // no fields set at all
          new PluginCallParameter(
              Optional.empty(),
              Optional.empty(),
              OptionalLong.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty()),
          // every field the call parameter supports
          new PluginCallParameter(
              Optional.of(SENDER),
              Optional.of(OTHER),
              OptionalLong.of(50_000L),
              Optional.of(Wei.of(1_000L)),
              Optional.of(Wei.of(5L)),
              Optional.empty(),
              Optional.empty(),
              Optional.of(Bytes.fromHexString("0x01"))),
          // reverting call: a contract creation with invalid code
          new PluginCallParameter(
              Optional.of(SENDER),
              Optional.empty(),
              OptionalLong.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(Bytes.fromHexString("0xfe"))));
    }

    @Test
    void blockCallMatchesDirectSimulatorCall() {
      for (final long number : new long[] {1, 8, blockchain.getChainHeadBlockNumber()}) {
        final BlockHeader header = blockchain.getBlockHeader(number).orElseThrow();
        for (final PluginCallParameter call : calls()) {
          final Optional<CallOutcome> direct =
              directCall(
                  call,
                  TransactionValidationParams.transactionSimulatorAllowExceedingBalance(),
                  header);
          final Optional<CallOutcome> viaService =
              service
                  .simulate(
                      call,
                      Optional.empty(),
                      header.getHash(),
                      OperationTracer.NO_TRACING,
                      EnumSet.of(SimulationParameters.ALLOW_EXCEEDING_BALANCE))
                  .map(CallOutcome::of);

          assertThat(direct).isPresent();
          assertThat(viaService).as("block %d, call %s", number, call).isEqualTo(direct);
        }
      }
    }

    @Test
    void pendingCallMatchesDirectSimulatorCall() {
      for (final PluginCallParameter call : calls()) {
        final Optional<CallOutcome> direct =
            directCall(
                call,
                TransactionValidationParams
                    .transactionSimulatorAllowExceedingBalanceAndFutureNonce(),
                blockchain.getChainHeadHeader());
        final Optional<CallOutcome> viaService =
            service
                .simulate(
                    call,
                    Optional.empty(),
                    blockchain.getChainHeadHash(),
                    OperationTracer.NO_TRACING,
                    EnumSet.of(
                        SimulationParameters.ALLOW_EXCEEDING_BALANCE,
                        SimulationParameters.ALLOW_FUTURE_NONCE))
                .map(CallOutcome::of);

        assertThat(direct).isPresent();
        assertThat(viaService).as("pending, call %s", call).isEqualTo(direct);
      }
    }

    @Test
    void missingWorldStateIsEmptyRatherThanAnError() {
      // a real simulator whose archive has no state for any block, as with pruned Bonsai state
      final WorldStateArchive emptyArchive = mock(WorldStateArchive.class);
      when(emptyArchive.getWorldState(any())).thenReturn(Optional.empty());
      final TransactionSimulator simulatorWithoutState =
          new TransactionSimulator(
              blockchain,
              emptyArchive,
              setup.getProtocolSchedule(),
              ImmutableMiningConfiguration.newDefault(),
              0L);
      final TransactionSimulationServiceImpl serviceWithoutState =
          new TransactionSimulationServiceImpl();
      serviceWithoutState.init(blockchain, simulatorWithoutState);

      for (final PluginCallParameter call : calls()) {
        // what a direct call sees through its PreCloseStateHandler: no result, not an exception
        assertThat(
                directCall(
                    simulatorWithoutState,
                    call,
                    TransactionValidationParams.transactionSimulatorAllowExceedingBalance(),
                    blockchain.getChainHeadHeader()))
            .isEmpty();
        assertThat(
                serviceWithoutState.simulate(
                    call,
                    Optional.empty(),
                    blockchain.getChainHeadHash(),
                    OperationTracer.NO_TRACING,
                    EnumSet.of(SimulationParameters.ALLOW_EXCEEDING_BALANCE)))
            .isEmpty();
      }
    }

    @Test
    void worldStateThatFailsToOpenIsEmptyRatherThanAnError() {
      final WorldStateArchive failingArchive = mock(WorldStateArchive.class);
      when(failingArchive.getWorldState(any())).thenThrow(new IllegalStateException("boom"));
      final TransactionSimulator failingSimulator =
          new TransactionSimulator(
              blockchain,
              failingArchive,
              setup.getProtocolSchedule(),
              ImmutableMiningConfiguration.newDefault(),
              0L);
      final TransactionSimulationServiceImpl failingService =
          new TransactionSimulationServiceImpl();
      failingService.init(blockchain, failingSimulator);

      assertThat(
              failingService.simulate(
                  calls().get(0),
                  Optional.empty(),
                  blockchain.getChainHeadHash(),
                  OperationTracer.NO_TRACING,
                  EnumSet.of(SimulationParameters.ALLOW_EXCEEDING_BALANCE)))
          .isEmpty();
    }

    @Test
    void unknownBlockIsEmptyOnARealChain() {
      assertThat(
              service.simulate(
                  calls().get(0),
                  Optional.empty(),
                  Hash.ZERO,
                  OperationTracer.NO_TRACING,
                  EnumSet.of(SimulationParameters.ALLOW_EXCEEDING_BALANCE)))
          .isEmpty();
    }

    @Test
    void callUsesThePostStateOfTheBlock() {
      // find a block in which the sender spent some of its balance
      BlockHeader parent = blockchain.getGenesisBlock().getHeader();
      BlockHeader header = null;
      for (long number = 1; number <= blockchain.getChainHeadBlockNumber(); number++) {
        final BlockHeader candidate = blockchain.getBlockHeader(number).orElseThrow();
        if (balanceAt(candidate).compareTo(balanceAt(parent)) < 0) {
          header = candidate;
          break;
        }
        parent = candidate;
      }
      assertThat(header).as("a block in which the sender spends").isNotNull();
      final Wei balanceAfterParent = balanceAt(parent);
      final Wei balanceAfterBlock = balanceAt(header);

      // transferring the whole balance only succeeds against the post-state it was read from
      assertThat(simulateAt(transferOf(balanceAfterBlock), header).isSuccessful()).isTrue();
      assertThat(simulateAt(transferOf(balanceAfterParent), header).isSuccessful()).isFalse();
      assertThat(simulateAt(transferOf(balanceAfterParent), parent).isSuccessful()).isTrue();
    }

    private PluginCallParameter transferOf(final Wei value) {
      return new PluginCallParameter(
          Optional.of(SENDER),
          Optional.of(OTHER),
          OptionalLong.empty(),
          Optional.empty(),
          Optional.of(value),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());
    }

    private Wei balanceAt(final BlockHeader header) {
      return transactionSimulator
          .process(
              ImmutableCallParameter.builder().build(),
              Optional.empty(),
              TransactionValidationParams.transactionSimulatorAllowExceedingBalance(),
              OperationTracer.NO_TRACING,
              (worldState, result) -> Optional.ofNullable(worldState.get(SENDER)),
              header)
          .map(account -> account.getBalance())
          .orElse(Wei.ZERO);
    }

    private TransactionSimulationResult simulateAt(
        final PluginCallParameter call, final BlockHeader header) {
      return service
          .simulate(
              call,
              Optional.empty(),
              header.getHash(),
              OperationTracer.NO_TRACING,
              EnumSet.of(SimulationParameters.ALLOW_EXCEEDING_BALANCE))
          .orElseThrow();
    }

    private Optional<CallOutcome> directCall(
        final PluginCallParameter call,
        final TransactionValidationParams validationParams,
        final BlockHeader header) {
      return directCall(transactionSimulator, call, validationParams, header);
    }

    // a direct call: a core call parameter, with the result mapped in the PreCloseStateHandler
    private Optional<CallOutcome> directCall(
        final TransactionSimulator transactionSimulator,
        final PluginCallParameter call,
        final TransactionValidationParams validationParams,
        final BlockHeader header) {
      final CallParameter callParameters =
          ImmutableCallParameter.builder()
              .sender(call.from())
              .to(call.to())
              .gas(call.gas())
              .gasPrice(call.gasPrice())
              .value(call.value())
              .maxPriorityFeePerGas(call.maxPriorityFeePerGas())
              .maxFeePerGas(call.maxFeePerGas())
              .input(call.data())
              .build();
      return transactionSimulator.process(
          callParameters,
          validationParams,
          OperationTracer.NO_TRACING,
          (mutableWorldState, transactionSimulatorResult) ->
              transactionSimulatorResult.map(
                  result ->
                      new CallOutcome(
                          result.isSuccessful() ? 1L : 0L,
                          result.getGasEstimate(),
                          result.getOutput())),
          header);
    }
  }

  private record CallOutcome(long status, long gasUsed, Bytes data) {
    static CallOutcome of(final TransactionSimulationResult result) {
      return new CallOutcome(
          result.isSuccessful() ? 1L : 0L, result.getGasEstimate(), result.result().getOutput());
    }
  }

  /** A call parameter implemented outside core, as a plugin would: only basic fields are set. */
  private record PluginCallParameter(
      Optional<Address> from,
      Optional<Address> to,
      OptionalLong gas,
      Optional<Wei> gasPrice,
      Optional<Wei> value,
      Optional<Wei> maxFeePerGas,
      Optional<Wei> maxPriorityFeePerGas,
      Optional<Bytes> data)
      implements org.hyperledger.besu.datatypes.CallParameter {

    @Override
    public Optional<BigInteger> getChainId() {
      return Optional.empty();
    }

    @Override
    public Optional<Address> getSender() {
      return from;
    }

    @Override
    public Optional<Address> getTo() {
      return to;
    }

    @Override
    public OptionalLong getGas() {
      return gas;
    }

    @Override
    public Optional<Wei> getMaxPriorityFeePerGas() {
      return maxPriorityFeePerGas;
    }

    @Override
    public Optional<Wei> getMaxFeePerGas() {
      return maxFeePerGas;
    }

    @Override
    public Optional<Wei> getMaxFeePerBlobGas() {
      return Optional.empty();
    }

    @Override
    public Optional<Wei> getGasPrice() {
      return gasPrice;
    }

    @Override
    public Optional<Wei> getValue() {
      return value;
    }

    @Override
    public Optional<List<AccessListEntry>> getAccessList() {
      return Optional.empty();
    }

    @Override
    public Optional<List<VersionedHash>> getBlobVersionedHashes() {
      return Optional.empty();
    }

    @Override
    public OptionalLong getNonce() {
      return OptionalLong.empty();
    }

    @Override
    public Optional<Boolean> getStrict() {
      return Optional.empty();
    }

    @Override
    public Optional<Bytes> getPayload() {
      return data;
    }

    @Override
    public List<CodeDelegation> getCodeDelegationAuthorizations() {
      return List.of();
    }
  }
}
