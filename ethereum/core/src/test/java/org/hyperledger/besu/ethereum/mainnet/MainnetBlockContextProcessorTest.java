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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.mainnet.systemcall.SystemCallNoCodeAtAddressException;
import org.hyperledger.besu.ethereum.mainnet.systemcall.SystemCallProcessor;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.processor.AbstractMessageProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

public class MainnetBlockContextProcessorTest {
  private static final Address CALL_ADDRESS = Address.fromHexString("0x1");
  private static final Bytes EXPECTED_OUTPUT = Bytes.fromHexString("0x01");
  private ProcessableBlockHeader mockBlockHeader;
  private MainnetTransactionProcessor mockTransactionProcessor;
  private BlockHashLookup mockBlockHashLookup;
  private AbstractMessageProcessor mockMessageCallProcessor;

  @BeforeEach
  public void setUp() {
    mockBlockHeader = mock(ProcessableBlockHeader.class);
    mockTransactionProcessor = mock(MainnetTransactionProcessor.class);
    mockMessageCallProcessor = mock(MessageCallProcessor.class);
    mockBlockHashLookup = mock(BlockHashLookup.class);
    when(mockTransactionProcessor.getMessageProcessor(any())).thenReturn(mockMessageCallProcessor);
    when(mockMessageCallProcessor.getOrCreateCachedJumpDest(any(), any()))
        .thenReturn(Code.EMPTY_CODE);
    final GasCalculator mockGasCalculator = mock(GasCalculator.class);
    when(mockGasCalculator.stateGasCostCalculator()).thenReturn(StateGasCostCalculator.NONE);
    when(mockTransactionProcessor.getGasCalculator()).thenReturn(mockGasCalculator);
  }

  @Test
  void shouldProcessSuccessfully() {
    doAnswer(
            invocation -> {
              MessageFrame messageFrame = invocation.getArgument(0);
              messageFrame.setOutputData(EXPECTED_OUTPUT);
              messageFrame.getMessageFrameStack().pop();
              messageFrame.setState(MessageFrame.State.COMPLETED_SUCCESS);
              return null;
            })
        .when(mockMessageCallProcessor)
        .process(any(), any());
    final MutableWorldState worldState = createWorldState(CALL_ADDRESS);
    Bytes actualOutput = processSystemCall(worldState);
    assertThat(actualOutput).isEqualTo(EXPECTED_OUTPUT);
  }

  @Test
  void shouldThrowExceptionOnFailedExecution() {
    doAnswer(
            invocation -> {
              MessageFrame messageFrame = invocation.getArgument(0);
              messageFrame.getMessageFrameStack().pop();
              messageFrame.setState(MessageFrame.State.COMPLETED_FAILED);
              return null;
            })
        .when(mockMessageCallProcessor)
        .process(any(), any());
    final MutableWorldState worldState = createWorldState(CALL_ADDRESS);
    var exception = assertThrows(RuntimeException.class, () -> processSystemCall(worldState));
    assertThat(exception.getMessage()).isEqualTo("System call did not execute to completion");
  }

  @Test
  void shouldThrowExceptionOnFailedExecutionWithHaltReason() {
    doAnswer(
            invocation -> {
              MessageFrame messageFrame = invocation.getArgument(0);
              messageFrame.getMessageFrameStack().pop();
              messageFrame.setState(MessageFrame.State.COMPLETED_FAILED);
              messageFrame.setExceptionalHaltReason(
                  Optional.of(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS));
              return null;
            })
        .when(mockMessageCallProcessor)
        .process(any(), any());
    final MutableWorldState worldState = createWorldState(CALL_ADDRESS);
    var exception = assertThrows(RuntimeException.class, () -> processSystemCall(worldState));
    assertThat(exception.getMessage()).isEqualTo("System call halted: Stack underflow");
  }

  @Test
  void shouldThrowExceptionIfSystemCallAddressDoesNotExist() {
    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    var exception =
        assertThrows(SystemCallNoCodeAtAddressException.class, () -> processSystemCall(worldState));
    assertThat(exception.getMessage())
        .isEqualTo("Invalid system call, no code at address " + CALL_ADDRESS);
  }

  @Test
  void shouldThrowExceptionIfSystemCallHasNoCode() {
    Bytes code = Bytes.EMPTY;
    final MutableWorldState worldState = createWorldState(CALL_ADDRESS, code);
    var exception =
        assertThrows(SystemCallNoCodeAtAddressException.class, () -> processSystemCall(worldState));
    assertThat(exception.getMessage())
        .isEqualTo("Invalid system call, no code at address " + CALL_ADDRESS);
  }

  @Test
  void systemCallUsesNoTracingWhenBlockAwareTracerDoesNotOptIn() {
    // A tracer that does NOT override isSystemCallTracingEnabled() (defaults to false)
    BlockAwareOperationTracer tracer = mock(BlockAwareOperationTracer.class);
    when(tracer.isSystemCallTracingEnabled()).thenReturn(false);
    successfulProcess();
    final MutableWorldState worldState = createWorldState(CALL_ADDRESS);

    processSystemCallWithTracer(worldState, tracer);

    ArgumentCaptor<OperationTracer> tracerCaptor = ArgumentCaptor.forClass(OperationTracer.class);
    verify(mockMessageCallProcessor).process(any(), tracerCaptor.capture());
    assertThat(tracerCaptor.getValue()).isSameAs(OperationTracer.NO_TRACING);
  }

  @Test
  void systemCallUsesContextTracerWhenBlockAwareTracerOptsIn() {
    // A tracer that DOES override isSystemCallTracingEnabled() to return true
    BlockAwareOperationTracer tracer = mock(BlockAwareOperationTracer.class);
    when(tracer.isEnabled()).thenReturn(true);
    when(tracer.isSystemCallTracingEnabled()).thenReturn(true);
    successfulProcess();
    final MutableWorldState worldState = createWorldState(CALL_ADDRESS);

    processSystemCallWithTracer(worldState, tracer);

    ArgumentCaptor<OperationTracer> tracerCaptor = ArgumentCaptor.forClass(OperationTracer.class);
    verify(mockMessageCallProcessor).process(any(), tracerCaptor.capture());
    assertThat(tracerCaptor.getValue()).isSameAs(tracer);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // PUSH1 1 PUSH1 0 SSTORE INVALID
        "0x6001600055fe",
        // PUSH1 1 PUSH1 0 SSTORE PUSH1 0 PUSH1 0 REVERT
        "0x600160005560006000fd"
      })
  void uncheckedSystemCallFailureDropsWritesButKeepsReads(final String code) {
    useRealMessageCallProcessor();
    final MutableWorldState worldState = createWorldState(CALL_ADDRESS, Bytes.fromHexString(code));
    final BlockAccessListBuilder balBuilder = BlockAccessList.builder();

    processUncheckedSystemCall(worldState, balBuilder);

    assertThat(worldState.get(CALL_ADDRESS).getStorageValue(UInt256.ZERO)).isEqualTo(UInt256.ZERO);
    final AccountChanges accountChanges = accountChanges(balBuilder);
    assertThat(accountChanges.storageChanges()).isEmpty();
    assertThat(accountChanges.storageReads())
        .containsExactly(new SlotRead(new StorageSlotKey(UInt256.ZERO)));
  }

  @Test
  void uncheckedSystemCallSuccessCommitsWrites() {
    useRealMessageCallProcessor();
    // PUSH1 1 PUSH1 0 SSTORE STOP
    final MutableWorldState worldState =
        createWorldState(CALL_ADDRESS, Bytes.fromHexString("0x600160005500"));
    final BlockAccessListBuilder balBuilder = BlockAccessList.builder();

    processUncheckedSystemCall(worldState, balBuilder);

    assertThat(worldState.get(CALL_ADDRESS).getStorageValue(UInt256.ZERO)).isEqualTo(UInt256.ONE);
    final AccountChanges accountChanges = accountChanges(balBuilder);
    assertThat(accountChanges.storageReads()).isEmpty();
    assertThat(accountChanges.storageChanges())
        .containsExactly(
            new SlotChanges(
                new StorageSlotKey(UInt256.ZERO), List.of(new StorageChange(0, UInt256.ONE))));
  }

  @Test
  void uncheckedSystemCallWithoutCodeStillTouchesAccount() {
    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final BlockAccessListBuilder balBuilder = BlockAccessList.builder();

    processUncheckedSystemCall(worldState, balBuilder);

    final AccountChanges accountChanges = accountChanges(balBuilder);
    assertThat(accountChanges.hasAnyChange()).isFalse();
    assertThat(accountChanges.storageReads()).isEmpty();
  }

  @Test
  void checkedSystemCallFailureStillThrows() {
    useRealMessageCallProcessor();
    // PUSH1 1 PUSH1 0 SSTORE INVALID
    final MutableWorldState worldState =
        createWorldState(CALL_ADDRESS, Bytes.fromHexString("0x6001600055fe"));

    assertThrows(RuntimeException.class, () -> processSystemCall(worldState));
    assertThat(worldState.get(CALL_ADDRESS).getStorageValue(UInt256.ZERO)).isEqualTo(UInt256.ZERO);
  }

  private void useRealMessageCallProcessor() {
    when(mockTransactionProcessor.getMessageProcessor(any()))
        .thenReturn(
            new MessageCallProcessor(
                MainnetEVMs.prague(EvmConfiguration.DEFAULT), new PrecompileContractRegistry()));
  }

  private void processUncheckedSystemCall(
      final MutableWorldState worldState, final BlockAccessListBuilder balBuilder) {
    final BlockProcessingContext blockProcessingContext =
        new BlockProcessingContext(
            mockBlockHeader,
            worldState,
            mock(ProtocolSpec.class),
            mockBlockHashLookup,
            BlockAwareOperationTracer.NO_TRACING,
            Optional.of(balBuilder));
    new SystemCallProcessor(mockTransactionProcessor)
        .processUnchecked(
            CALL_ADDRESS,
            blockProcessingContext,
            Bytes.EMPTY,
            Optional.of(BlockAccessListBuilder.createPreExecutionAccessLocationTracker()));
  }

  private static AccountChanges accountChanges(final BlockAccessListBuilder balBuilder) {
    final List<AccountChanges> accountChanges = balBuilder.build().accountChanges();
    assertThat(accountChanges).hasSize(1);
    assertThat(accountChanges.getFirst().address()).isEqualTo(CALL_ADDRESS);
    return accountChanges.getFirst();
  }

  private void successfulProcess() {
    doAnswer(
            invocation -> {
              MessageFrame messageFrame = invocation.getArgument(0);
              messageFrame.setOutputData(EXPECTED_OUTPUT);
              messageFrame.getMessageFrameStack().pop();
              messageFrame.setState(MessageFrame.State.COMPLETED_SUCCESS);
              return null;
            })
        .when(mockMessageCallProcessor)
        .process(any(), any());
  }

  Bytes processSystemCall(final MutableWorldState worldState) {
    return processSystemCallWithTracer(worldState, BlockAwareOperationTracer.NO_TRACING);
  }

  Bytes processSystemCallWithTracer(
      final MutableWorldState worldState, final BlockAwareOperationTracer tracer) {
    SystemCallProcessor systemCallProcessor = new SystemCallProcessor(mockTransactionProcessor);

    BlockProcessingContext blockProcessingContext =
        new BlockProcessingContext(
            mockBlockHeader,
            worldState,
            mock(ProtocolSpec.class),
            mockBlockHashLookup,
            tracer,
            Optional.empty());

    when(mockBlockHashLookup.apply(any(), any())).thenReturn(Hash.EMPTY);
    return systemCallProcessor.process(
        CALL_ADDRESS, blockProcessingContext, Bytes.EMPTY, Optional.empty());
  }

  private MutableWorldState createWorldState(final Address address) {
    return createWorldState(address, Bytes.fromHexString("0x00"));
  }

  private MutableWorldState createWorldState(final Address address, final Bytes code) {
    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.getOrCreate(address);
    account.setCode(code);
    updater.commit();
    return worldState;
  }
}
