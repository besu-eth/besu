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
package org.hyperledger.besu.ethereum.vm;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.AbstractCallOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Locale;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Base class for opcode-level operation tracers. Manages the per-frame state that must be captured
 * in {@link #tracePreExecution} and consumed in {@link #tracePostExecution}, along with the shared
 * opcode-filter and stack-capture logic.
 *
 * <p>Subclasses implement {@link #tracePostExecution} to decide what to do with the captured state
 * (accumulate to a list, or stream directly to a writer). Subclasses that need additional
 * pre-execution state (e.g., input data) can override {@link #capturePreExecutionState}.
 */
public abstract class AbstractDebugOperationTracer implements OperationTracer {

  protected final OpCodeTracerConfig options;

  /**
   * If {@code true}, gas cost for CALL operations includes gas granted to the child call (Parity
   * style, {@code trace_} RPCs). If {@code false}, only the operation cost is reported (Geth style,
   * {@code debug_} RPCs).
   */
  protected final boolean recordChildCallGas;

  /** Pre-execution stack snapshot, set in {@link #tracePreExecution}. */
  protected Optional<Bytes[]> preExecutionStack;

  /** Gas remaining before the operation executed, set in {@link #tracePreExecution}. */
  protected long gasRemaining;

  /** Program counter before the operation executed, set in {@link #tracePreExecution}. */
  protected int pc;

  /** Call depth before the operation executed, set in {@link #tracePreExecution}. */
  protected int depth;

  /**
   * Whether the current opcode should be traced (respects the {@code traceOpcodes} filter). Set by
   * {@link #tracePreExecution}; checked by {@link #tracePostExecution} in subclasses.
   */
  protected boolean traceOpcode;

  private Operation previousOpcode = null;

  protected AbstractDebugOperationTracer(
      final OpCodeTracerConfig options, final boolean recordChildCallGas) {
    this.options = options;
    this.recordChildCallGas = recordChildCallGas;
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    final Operation currentOperation = frame.getCurrentOperation();
    if (!(traceOpcode = shouldTraceOpcode(currentOperation))) {
      return;
    }
    preExecutionStack = captureStack(frame);
    gasRemaining = frame.getRemainingGas();
    pc = frame.getPC();
    depth = frame.getDepth();
    capturePreExecutionState(frame);
  }

  /**
   * Hook for subclasses to capture additional pre-execution state. Called only when {@link
   * #traceOpcode} is {@code true}, after the common fields have been populated.
   */
  protected void capturePreExecutionState(final MessageFrame frame) {}

  /**
   * Computes the effective gas cost for the operation, optionally including child-call gas.
   *
   * @param currentOperation the operation that was executed
   * @param operationResult the result including raw gas cost
   * @param frame the message frame after execution
   * @return the effective gas cost to report
   */
  protected long computeGasCost(
      final Operation currentOperation,
      final OperationResult operationResult,
      final MessageFrame frame) {
    long gasCost = operationResult.getGasCost();
    if (recordChildCallGas && currentOperation instanceof AbstractCallOperation) {
      gasCost += frame.getMessageFrameStack().getFirst().getRemainingGas();
    }
    return gasCost;
  }

  /**
   * Captures the current operand stack as an array ordered from bottom to top, or {@link
   * Optional#empty()} when stack tracing is disabled.
   */
  protected Optional<Bytes[]> captureStack(final MessageFrame frame) {
    if (!options.traceStack()) {
      return Optional.empty();
    }
    final Bytes[] stackContents = new Bytes[frame.stackSize()];
    for (int i = 0; i < stackContents.length; i++) {
      stackContents[i] = frame.getStackItem(stackContents.length - i - 1);
    }
    return Optional.of(stackContents);
  }

  private boolean shouldTraceOpcode(final Operation currentOpcode) {
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
}
