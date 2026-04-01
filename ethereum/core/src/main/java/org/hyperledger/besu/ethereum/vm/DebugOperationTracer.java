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
package org.hyperledger.besu.ethereum.vm;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.AbstractCallOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.tracing.TraceFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class DebugOperationTracer implements OperationTracer {

  private final OpCodeTracerConfig options;

  /**
   * A flag to indicate if call operations should trace just the operation cost (false, Geth style,
   * debug_ series RPCs) or the operation cost and all gas granted to the child call (true, Parity
   * style, trace_ series RPCs)
   */
  private final boolean recordChildCallGas;

  private List<TraceFrame> traceFrames = new ArrayList<>();
  private TraceFrame lastFrame;

  private Optional<Bytes[]> preExecutionStack;
  private long gasRemaining;
  private Bytes inputData;
  private int pc;
  private int depth;

  // Flags used for implementing traceOpcodes functionality
  private boolean traceOpcode;
  private Operation previousOpcode = null;

  /**
   * Creates the operation tracer.
   *
   * @param options The options, as passed in through the RPC
   * @param recordChildCallGas A flag on whether to produce geth style (true) or parity style
   *     (false) gas amounts for call operations
   */
  public DebugOperationTracer(final OpCodeTracerConfig options, final boolean recordChildCallGas) {
    this.options = options;
    this.recordChildCallGas = recordChildCallGas;
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    final Operation currentOperation = frame.getCurrentOperation();
    if (!(traceOpcode = traceOpcode(currentOperation))) {
      return;
    }
    preExecutionStack = captureStack(frame);
    gasRemaining = frame.getRemainingGas();
    if (lastFrame != null && frame.getDepth() > lastFrame.getDepth())
      inputData = frame.getInputData().copy();
    else inputData = frame.getInputData();
    pc = frame.getPC();
    depth = frame.getDepth();
  }

  private boolean traceOpcode(final Operation currentOpcode) {
    if (options.traceOpcodes().isEmpty()) {
      return true;
    }
    final boolean traceCurrentOpcode =
        options.traceOpcodes().contains(currentOpcode.getName().toLowerCase(Locale.ROOT));
    final boolean tracePreviousOpcode =
        previousOpcode != null
            && options.traceOpcodes().contains(previousOpcode.getName().toLowerCase(Locale.ROOT));

    if (!traceCurrentOpcode && !tracePreviousOpcode) {
      return false;
    }
    previousOpcode = currentOpcode;
    return true;
  }

  @Override
  public void tracePostExecution(final MessageFrame frame, final OperationResult operationResult) {
    final Operation currentOperation = frame.getCurrentOperation();
    final String opcode = currentOperation.getName();
    if (!traceOpcode) {
      return;
    }
    final int opcodeNumber = (opcode != null) ? currentOperation.getOpcode() : Integer.MAX_VALUE;
    final WorldUpdater worldUpdater = frame.getWorldUpdater();
    final Bytes outputData = frame.getOutputData();
    final Optional<Bytes[]> memory = captureMemory(frame);
    final Optional<Bytes[]> stackPostExecution = captureStack(frame);

    if (!traceFrames.isEmpty()) {
      final TraceFrame lastTraceFrame = traceFrames.removeLast();
      final TraceFrame updatedLast =
          TraceFrame.from(lastTraceFrame).setGasRemainingPostExecution(gasRemaining).build();
      traceFrames.add(updatedLast);
    }

    final Optional<Map<UInt256, UInt256>> storage = captureStorage(frame);
    final Optional<Map<Address, Wei>> maybeRefunds =
        frame.getRefunds().isEmpty() ? Optional.empty() : Optional.of(frame.getRefunds());
    long thisGasCost = operationResult.getGasCost();
    if (recordChildCallGas && currentOperation instanceof AbstractCallOperation) {
      thisGasCost += frame.getMessageFrameStack().getFirst().getRemainingGas();
    }

    final Optional<ExceptionalHaltReason> haltReason =
        Optional.ofNullable(operationResult.getHaltReason()).or(frame::getExceptionalHaltReason);

    final Optional<Code> maybeCode =
        Optional.ofNullable(frame.getMessageFrameStack().peek()).map(MessageFrame::getCode);
    lastFrame =
        TraceFrame.builder()
            .setPc(pc)
            .setOpcode(opcode)
            .setOpcodeNumber(opcodeNumber)
            .setGasRemaining(gasRemaining)
            .setGasCost(thisGasCost == 0 ? OptionalLong.empty() : OptionalLong.of(thisGasCost))
            .setGasRefund(frame.getGasRefund())
            .setDepth(depth)
            .setExceptionalHaltReason(haltReason)
            .setRecipient(frame.getRecipientAddress())
            .setValue(frame.getApparentValue())
            .setInputData(inputData)
            .setOutputData(outputData)
            .setStack(preExecutionStack)
            .setMemory(memory)
            .setStorage(storage)
            .setWorldUpdater(worldUpdater)
            .setRevertReason(frame.getRevertReason())
            .setMaybeRefunds(maybeRefunds)
            .setMaybeCode(maybeCode)
            .setStackItemsProduced(frame.getCurrentOperation().getStackItemsProduced())
            .setStackPostExecution(stackPostExecution)
            .setVirtualOperation(currentOperation.isVirtualOperation())
            .setMaybeUpdatedMemory(frame.getMaybeUpdatedMemory())
            .setMaybeUpdatedStorage(frame.getMaybeUpdatedStorage())
            .setSoftFailureReason(operationResult.getSoftFailureReason())
            .setGasAvailableForChildCall(operationResult.getGasAvailableForChildCall())
            .build();

    traceFrames.add(lastFrame);
    frame.reset();
  }

  @Override
  public void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {

    final Address recipient = frame.getRecipientAddress();
    final Bytes inputData = frame.getInputData().copy();

    if (traceFrames.isEmpty()) {
      final TraceFrame traceFrame =
          TraceFrame.builder()
              .setPc(frame.getPC())
              .setOpcodeNumber(Integer.MAX_VALUE)
              .setGasRemaining(frame.getRemainingGas())
              .setGasRefund(frame.getGasRefund())
              .setDepth(frame.getDepth())
              .setRecipient(recipient)
              .setValue(frame.getValue())
              .setInputData(inputData)
              .setOutputData(frame.getOutputData())
              .setWorldUpdater(frame.getWorldUpdater())
              .setMaybeRefunds(Optional.ofNullable(frame.getRefunds()))
              .setMaybeCode(Optional.ofNullable(frame.getCode()))
              .setStackItemsProduced(frame.getMaxStackSize())
              .setVirtualOperation(true)
              .setPrecompiledGasCost(gasRequirement)
              .setPrecompileIOData(recipient, inputData, output)
              .build();
      traceFrames.add(traceFrame);
    } else {
      final TraceFrame lastTraceFrame = traceFrames.removeLast();
      final TraceFrame updatedTraceFrame =
          TraceFrame.from(lastTraceFrame)
              .setExceptionalHaltReason(frame.getExceptionalHaltReason())
              .setRevertReason(frame.getRevertReason())
              .setPrecompiledGasCost(gasRequirement)
              .setPrecompileIOData(recipient, inputData, output)
              .build();
      traceFrames.add(updatedTraceFrame);
    }
  }

  @Override
  public void traceAccountCreationResult(
      final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {
    haltReason.ifPresent(
        exceptionalHaltReason -> {
          if (!traceFrames.isEmpty()) {
            updateFirstNonReturnFrame(exceptionalHaltReason);
          } else {
            addNewTraceFrame(frame, exceptionalHaltReason);
          }
        });
  }

  private void updateFirstNonReturnFrame(final ExceptionalHaltReason exceptionalHaltReason) {
    // Find the last non-RETURN frame
    for (int i = traceFrames.size() - 1; i >= 0; i--) {
      final TraceFrame currentFrame = traceFrames.get(i);
      if (!"RETURN".equals(currentFrame.getOpcode())) {
        // Create updated frame with the exceptional halt reason
        final TraceFrame updatedFrame =
            TraceFrame.from(currentFrame)
                .setExceptionalHaltReason(Optional.of(exceptionalHaltReason))
                .build();
        traceFrames.set(i, updatedFrame);
        break;
      }
    }
  }

  private void addNewTraceFrame(
      final MessageFrame frame, final ExceptionalHaltReason exceptionalHaltReason) {
    final TraceFrame traceFrame =
        TraceFrame.builder()
            .setPc(frame.getPC())
            .setOpcodeNumber(Integer.MAX_VALUE)
            .setGasRemaining(frame.getRemainingGas())
            .setGasRefund(frame.getGasRefund())
            .setDepth(frame.getDepth())
            .setExceptionalHaltReason(Optional.of(exceptionalHaltReason))
            .setRecipient(frame.getRecipientAddress())
            .setValue(frame.getValue())
            .setInputData(frame.getInputData().copy())
            .setOutputData(frame.getOutputData())
            .setWorldUpdater(frame.getWorldUpdater())
            .setMaybeRefunds(Optional.ofNullable(frame.getRefunds()))
            .setMaybeCode(Optional.ofNullable(frame.getCode()))
            .setStackItemsProduced(frame.getMaxStackSize())
            .setVirtualOperation(true)
            .build();
    traceFrames.add(traceFrame);
  }

  private Optional<Map<UInt256, UInt256>> captureStorage(final MessageFrame frame) {
    if (!options.traceStorage()) {
      return Optional.empty();
    }
    try {
      Map<UInt256, UInt256> updatedStorage =
          frame.getWorldUpdater().getAccount(frame.getRecipientAddress()).getUpdatedStorage();
      if (updatedStorage.isEmpty()) return Optional.empty();
      final Map<UInt256, UInt256> storageContents = new TreeMap<>(updatedStorage);

      return Optional.of(storageContents);
    } catch (final ModificationNotAllowedException e) {
      return Optional.of(new TreeMap<>());
    }
  }

  private Optional<Bytes[]> captureMemory(final MessageFrame frame) {
    if (!options.traceMemory() || frame.memoryWordSize() == 0) {
      return Optional.empty();
    } else if (frame.getMaybeUpdatedMemory().isEmpty()
        && lastFrame != null
        && lastFrame.getDepth() == frame.getDepth()
        && lastFrame.getMemory().get().length == frame.memoryWordSize()) {
      return lastFrame.getMemory();
    }
    final Bytes[] memoryContents = new Bytes[frame.memoryWordSize()];
    for (int i = 0; i < memoryContents.length; i++) {
      memoryContents[i] = frame.readMemory(i * 32L, 32);
    }
    return Optional.of(memoryContents);
  }

  private Optional<Bytes[]> captureStack(final MessageFrame frame) {
    if (!options.traceStack()) {
      return Optional.empty();
    }

    final Bytes[] stackContents = new Bytes[frame.stackSize()];
    for (int i = 0; i < stackContents.length; i++) {
      // Record stack contents in reverse
      stackContents[i] = frame.getStackItem(stackContents.length - i - 1);
    }
    return Optional.of(stackContents);
  }

  @Override
  public List<TraceFrame> getTraceFrames() {
    return traceFrames;
  }

  public void reset() {
    traceFrames = new ArrayList<>();
    lastFrame = null;
  }

  public List<TraceFrame> copyTraceFrames() {
    return new ArrayList<>(traceFrames);
  }
}
