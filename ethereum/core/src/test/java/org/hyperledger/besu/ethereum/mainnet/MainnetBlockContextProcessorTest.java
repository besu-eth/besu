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
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.mainnet.systemcall.SystemCallNoCodeAtAddressException;
import org.hyperledger.besu.ethereum.mainnet.systemcall.SystemCallProcessor;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.processor.AbstractMessageProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    assertThat(exception.getMessage()).isEqualTo("Invalid system call address: " + CALL_ADDRESS);
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

  Bytes processSystemCall(final MutableWorldState worldState) {
    SystemCallProcessor systemCallProcessor = new SystemCallProcessor(mockTransactionProcessor);

    BlockProcessingContext blockProcessingContext =
        new BlockProcessingContext(
            mockBlockHeader,
            worldState,
            mock(ProtocolSpec.class),
            mockBlockHashLookup,
            OperationTracer.NO_TRACING,
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
