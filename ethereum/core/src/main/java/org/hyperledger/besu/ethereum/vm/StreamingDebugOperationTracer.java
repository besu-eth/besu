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

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;

import org.apache.tuweni.bytes.Bytes;

/**
 * An {@link AbstractDebugOperationTracer} that streams trace data directly to a {@link FrameWriter}
 * callback on each operation, without accumulating frames in memory. Precompile and
 * account-creation callbacks are no-ops because already-emitted frames cannot be retroactively
 * updated.
 */
public class StreamingDebugOperationTracer extends AbstractDebugOperationTracer {

  /**
   * Functional interface for zero-allocation streaming of trace data directly from a MessageFrame.
   */
  @FunctionalInterface
  public interface FrameWriter {
    void writeFrame(
        int pc,
        String opcode,
        long gasRemaining,
        long gasCost,
        int depth,
        Bytes[] preExecutionStack,
        MessageFrame frame,
        ExceptionalHaltReason haltReason,
        Bytes revertReason);
  }

  private final FrameWriter frameWriter;
  private boolean hasEmittedFrame = false;

  /**
   * Creates a streaming operation tracer.
   *
   * @param options The opcode tracer config (stack/memory/storage options, opcode filter)
   * @param recordChildCallGas If true, gas cost for CALL ops includes gas granted to the child call
   *     (Parity style). If false, only the operation cost is reported (Geth style).
   * @param frameWriter Callback invoked once per traced operation with live frame data
   */
  public StreamingDebugOperationTracer(
      final OpCodeTracerConfig options,
      final boolean recordChildCallGas,
      final FrameWriter frameWriter) {
    super(options, recordChildCallGas);
    this.frameWriter = frameWriter;
  }

  @Override
  public void tracePostExecution(final MessageFrame frame, final OperationResult operationResult) {
    if (!traceOpcode) {
      return;
    }
    final Operation currentOperation = frame.getCurrentOperation();
    final String opcode = currentOperation.getName();
    final long thisGasCost = computeGasCost(currentOperation, operationResult, frame);

    final ExceptionalHaltReason haltReason =
        operationResult.getHaltReason() != null
            ? operationResult.getHaltReason()
            : frame.getExceptionalHaltReason().orElse(null);

    final Bytes revertReason = frame.getRevertReason().orElse(null);

    frameWriter.writeFrame(
        pc,
        opcode,
        gasRemaining,
        thisGasCost,
        depth,
        preExecutionStack.orElse(null),
        frame,
        haltReason,
        revertReason);

    hasEmittedFrame = true;
    frame.reset();
  }

  @Override
  public void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {
    if (!hasEmittedFrame) {
      frameWriter.writeFrame(
          frame.getPC(),
          "",
          frame.getRemainingGas(),
          0L,
          frame.getDepth(),
          null,
          frame,
          null,
          frame.getRevertReason().orElse(null));
      hasEmittedFrame = true;
    }
  }
}
