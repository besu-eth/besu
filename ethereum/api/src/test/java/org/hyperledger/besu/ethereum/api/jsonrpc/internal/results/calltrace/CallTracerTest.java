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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CallTracerResult;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@DisplayName("CallTracer")
class CallTracerTest {

  @ParameterizedTest
  @CsvSource({
    "100, 0",
    "101, 1",
    "164, 63",
    "1000, 886",
    "10000, 9746",
    "-5, 0",
  })
  @DisplayName("calculatePrecompileGas applies the warm-access cap then the 63/64 rule")
  void calculatePrecompileGasAppliesGethStyleCalculation(final long preOpGas, final long expected) {
    assertThat(CallTracer.calculatePrecompileGas(preOpGas)).isEqualTo(expected);
  }

  private static MessageFrame frame(final Address sender, final Address ownAddress) {
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getDepth()).thenReturn(0);
    when(frame.getSenderAddress()).thenReturn(sender);
    when(frame.getContractAddress()).thenReturn(ownAddress);
    when(frame.getRecipientAddress()).thenReturn(ownAddress);
    when(frame.getValue()).thenReturn(Wei.ZERO);
    when(frame.getApparentValue()).thenReturn(Wei.ZERO);
    when(frame.getInputData()).thenReturn(Bytes.EMPTY);
    when(frame.getOutputData()).thenReturn(Bytes.EMPTY);
    when(frame.getRemainingGas()).thenReturn(21_000L);
    when(frame.getState()).thenReturn(MessageFrame.State.COMPLETED_SUCCESS);
    when(frame.getExceptionalHaltReason()).thenReturn(Optional.<ExceptionalHaltReason>empty());
    when(frame.getRevertReason()).thenReturn(Optional.<Bytes>empty());
    when(frame.getType()).thenReturn(MessageFrame.Type.MESSAGE_CALL);
    return frame;
  }

  private static Transaction mockTransaction() {
    final Transaction tx = mock(Transaction.class);
    when(tx.isContractCreation()).thenReturn(false);
    when(tx.getGasLimit()).thenReturn(21_000L);
    return tx;
  }

  @Test
  @DisplayName("clamps a failed CALL's input read to already-expanded memory instead of throwing")
  void doesNotReadBeyondExpandedMemory() {
    final CallTracer tracer = new CallTracer(false);
    final Address sender = Address.fromHexString("0x00");
    final Address contract = Address.fromHexString("0x01");
    final MessageFrame parent = frame(sender, contract);

    final Transaction tx = mockTransaction();
    tracer.traceStartTransaction(null, tx);
    tracer.traceContextEnter(parent);

    // A CALL whose stack declares an astronomically large argsOffset/argsLength - exactly what
    // Words.clampedToLong produces for a crafted stack value, and exactly the shape that used to
    // reach frame.readMemory(offset, length) unclamped and blow up once length >= 2^31. The call
    // never spawns a child frame (state stays COMPLETED_SUCCESS, not CODE_SUSPENDED), mirroring an
    // INSUFFICIENT_GAS halt that fires before memory is ever expanded.
    final Operation call = mock(Operation.class);
    when(call.getName()).thenReturn("CALL");
    when(parent.getCurrentOperation()).thenReturn(call);
    when(parent.stackSize()).thenReturn(7);
    when(parent.getStackItem(1)).thenReturn(bytes32Address(Address.fromHexString("0x02")));
    when(parent.getStackItem(2)).thenReturn(Bytes.repeat((byte) 0, 32));
    when(parent.getStackItem(3)).thenReturn(Bytes.repeat((byte) 0xff, 32));
    when(parent.getStackItem(4)).thenReturn(Bytes.repeat((byte) 0xff, 32));
    when(parent.memoryByteSize()).thenReturn(0L);

    final OperationResult haltResult =
        new OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_GAS);

    assertThatCode(
            () -> {
              tracer.tracePreExecution(parent);
              tracer.tracePostExecution(parent, haltResult);
            })
        .doesNotThrowAnyException();
    verify(parent, never()).readMemory(anyLong(), anyLong());

    tracer.traceContextExit(parent);
    final CallTracerResult result = tracer.buildResult(tx, mockResult(21_000L, true));
    assertThat(result.getCalls()).hasSize(1);
    assertThat(result.getCalls().get(0).getInput()).isEqualTo("0x");
  }

  @Test
  @DisplayName("reads a memory range clamped to what is actually expanded, not the raw stack value")
  void clampsToAvailableMemoryWhenPartiallyExpanded() {
    final CallTracer tracer = new CallTracer(false);
    final Address sender = Address.fromHexString("0x00");
    final Address contract = Address.fromHexString("0x01");
    final MessageFrame parent = frame(sender, contract);

    final Transaction tx = mockTransaction();
    tracer.traceStartTransaction(null, tx);
    tracer.traceContextEnter(parent);

    final Operation call = mock(Operation.class);
    when(call.getName()).thenReturn("CALL");
    when(parent.getCurrentOperation()).thenReturn(call);
    when(parent.stackSize()).thenReturn(7);
    when(parent.getStackItem(1)).thenReturn(bytes32Address(Address.fromHexString("0x02")));
    when(parent.getStackItem(2)).thenReturn(Bytes.repeat((byte) 0, 32));
    when(parent.getStackItem(3)).thenReturn(bytes32Long(32L)); // argsOffset = 32
    when(parent.getStackItem(4)).thenReturn(Bytes.repeat((byte) 0xff, 32)); // argsLength huge
    when(parent.memoryByteSize()).thenReturn(64L); // only 64 bytes actually expanded
    when(parent.readMemory(32L, 32L)).thenReturn(Bytes.fromHexString("0x" + "aa".repeat(32)));

    tracer.tracePreExecution(parent);
    tracer.tracePostExecution(
        parent, new OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_GAS));

    verify(parent).readMemory(32L, 32L);
    tracer.traceContextExit(parent);
    final CallTracerResult result = tracer.buildResult(tx, mockResult(21_000L, true));
    assertThat(result.getCalls().get(0).getInput()).isEqualTo("0x" + "aa".repeat(32));
  }

  @Test
  @DisplayName("reports the delegating account, not the code address, as `from` for nested calls")
  void nestedCallInsideDelegateCallReportsRecipientAddress() {
    final CallTracer tracer = new CallTracer(false);
    final Address sender = Address.fromHexString("0x00");
    final Address rootContract = Address.fromHexString("0x01");
    final MessageFrame root = frame(sender, rootContract);

    final Transaction tx = mockTransaction();
    tracer.traceStartTransaction(null, tx);
    tracer.traceContextEnter(root);

    final Address delegatingAccount = Address.fromHexString("0x0a");
    final Address codeAddress = Address.fromHexString("0x0c");
    final MessageFrame delegateFrame = mock(MessageFrame.class);
    when(delegateFrame.getDepth()).thenReturn(1);
    when(delegateFrame.getContractAddress()).thenReturn(codeAddress);
    when(delegateFrame.getRecipientAddress()).thenReturn(delegatingAccount);
    when(delegateFrame.getApparentValue()).thenReturn(Wei.ZERO);
    when(delegateFrame.getInputData()).thenReturn(Bytes.EMPTY);
    when(delegateFrame.getOutputData()).thenReturn(Bytes.EMPTY);
    when(delegateFrame.getRemainingGas()).thenReturn(20_000L);
    when(delegateFrame.getState()).thenReturn(MessageFrame.State.COMPLETED_SUCCESS);
    when(delegateFrame.getExceptionalHaltReason())
        .thenReturn(Optional.<ExceptionalHaltReason>empty());
    when(delegateFrame.getRevertReason()).thenReturn(Optional.<Bytes>empty());
    when(delegateFrame.getType()).thenReturn(MessageFrame.Type.MESSAGE_CALL);

    final Operation delegatecall = mock(Operation.class);
    when(delegatecall.getName()).thenReturn("DELEGATECALL");
    when(root.getCurrentOperation()).thenReturn(delegatecall);
    tracer.tracePreExecution(root);
    tracer.traceContextEnter(delegateFrame);

    // A CALL made from inside the delegatecall frame that fails without spawning a child: its
    // `from` must be the delegating account (getRecipientAddress()), not the executing code's own
    // address (getContractAddress()).
    final Operation call = mock(Operation.class);
    when(call.getName()).thenReturn("CALL");
    when(delegateFrame.getCurrentOperation()).thenReturn(call);
    when(delegateFrame.stackSize()).thenReturn(7);
    when(delegateFrame.getStackItem(1)).thenReturn(bytes32Address(Address.fromHexString("0x03")));
    when(delegateFrame.getStackItem(2)).thenReturn(Bytes.repeat((byte) 0, 32));
    when(delegateFrame.getStackItem(3)).thenReturn(Bytes.repeat((byte) 0, 32));
    when(delegateFrame.getStackItem(4)).thenReturn(Bytes.repeat((byte) 0, 32));
    when(delegateFrame.memoryByteSize()).thenReturn(0L);
    tracer.tracePreExecution(delegateFrame);
    tracer.tracePostExecution(
        delegateFrame, new OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_GAS));

    tracer.traceContextExit(delegateFrame);
    tracer.traceContextExit(root);
    final CallTracerResult result = tracer.buildResult(tx, mockResult(21_000L, true));

    final CallTracerResult delegateNode = result.getCalls().get(0);
    assertThat(delegateNode.getCalls()).hasSize(1);
    assertThat(delegateNode.getCalls().get(0).getFrom())
        .isEqualTo(delegatingAccount.getBytes().toHexString());
  }

  @Test
  @DisplayName("reports the parent frame's inherited value for DELEGATECALL, not 0x0")
  void delegateCallReportsInheritedValue() {
    final CallTracer tracer = new CallTracer(false);
    final Address sender = Address.fromHexString("0x00");
    final Address rootContract = Address.fromHexString("0x01");
    final MessageFrame root = frame(sender, rootContract);

    final Transaction tx = mockTransaction();
    tracer.traceStartTransaction(null, tx);
    tracer.traceContextEnter(root);

    final Wei inheritedValue = Wei.of(500);
    final MessageFrame delegateFrame = mock(MessageFrame.class);
    when(delegateFrame.getDepth()).thenReturn(1);
    when(delegateFrame.getContractAddress()).thenReturn(Address.fromHexString("0x0c"));
    when(delegateFrame.getRecipientAddress()).thenReturn(Address.fromHexString("0x0a"));
    when(delegateFrame.getApparentValue()).thenReturn(inheritedValue);
    when(delegateFrame.getInputData()).thenReturn(Bytes.EMPTY);
    when(delegateFrame.getOutputData()).thenReturn(Bytes.EMPTY);
    when(delegateFrame.getRemainingGas()).thenReturn(20_000L);
    when(delegateFrame.getState()).thenReturn(MessageFrame.State.COMPLETED_SUCCESS);
    when(delegateFrame.getExceptionalHaltReason())
        .thenReturn(Optional.<ExceptionalHaltReason>empty());
    when(delegateFrame.getRevertReason()).thenReturn(Optional.<Bytes>empty());
    when(delegateFrame.getType()).thenReturn(MessageFrame.Type.MESSAGE_CALL);

    final Operation delegatecall = mock(Operation.class);
    when(delegatecall.getName()).thenReturn("DELEGATECALL");
    when(root.getCurrentOperation()).thenReturn(delegatecall);
    tracer.tracePreExecution(root);
    tracer.traceContextEnter(delegateFrame);
    tracer.traceContextExit(delegateFrame);
    tracer.traceContextExit(root);

    final CallTracerResult result = tracer.buildResult(tx, mockResult(21_000L, true));
    assertThat(result.getCalls().get(0).getValue()).isEqualTo(inheritedValue.toShortHexString());
  }

  @Test
  @DisplayName("synthesizes a root call from the transaction when it never entered the EVM")
  void buildResultSynthesizesRootForUnexecutedTransaction() {
    final CallTracer tracer = new CallTracer(false);
    final Address sender = Address.fromHexString("0x00");
    final Address to = Address.fromHexString("0x02");

    final Transaction tx = mock(Transaction.class);
    when(tx.isContractCreation()).thenReturn(false);
    when(tx.getSender()).thenReturn(sender);
    when(tx.getTo()).thenReturn(Optional.<Address>of(to));
    when(tx.getValue()).thenReturn(Wei.of(7));
    when(tx.getGasLimit()).thenReturn(21_000L);
    when(tx.getPayload()).thenReturn(Bytes.fromHexString("0x1234"));

    // traceStartTransaction only - no traceContextEnter, as when validation fails before the
    // transaction ever reaches the EVM (e.g. debug_traceBlock replaying an invalid transaction).
    tracer.traceStartTransaction(null, tx);

    final TransactionProcessingResult result = mockResult(0L, false);
    when(result.getExceptionalHaltReason())
        .thenReturn(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
    when(result.getRevertReason()).thenReturn(Optional.<Bytes>empty());

    final CallTracerResult callResult = tracer.buildResult(tx, result);

    assertThat(callResult).isNotNull();
    assertThat(callResult.getType()).isEqualTo("CALL");
    assertThat(callResult.getFrom()).isEqualTo(sender.getBytes().toHexString());
    assertThat(callResult.getTo()).isEqualTo(to.getBytes().toHexString());
    assertThat(callResult.getGasUsed()).isEqualTo("0x5208");
    assertThat(callResult.getError())
        .isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS.getDescription());
  }

  private static TransactionProcessingResult mockResult(
      final long gasRemaining, final boolean successful) {
    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);
    when(result.getGasRemaining()).thenReturn(gasRemaining);
    when(result.isSuccessful()).thenReturn(successful);
    when(result.getOutput()).thenReturn(Bytes.EMPTY);
    when(result.getExceptionalHaltReason()).thenReturn(Optional.<ExceptionalHaltReason>empty());
    when(result.getRevertReason()).thenReturn(Optional.<Bytes>empty());
    return result;
  }

  private static Bytes bytes32Address(final Address address) {
    return Bytes.concatenate(Bytes.repeat((byte) 0, 12), address.getBytes());
  }

  private static Bytes bytes32Long(final long value) {
    return Bytes.concatenate(Bytes.repeat((byte) 0, 24), Bytes.ofUnsignedLong(value));
  }
}
