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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.vm;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.Trace;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class VmTraceGenerator {

  private int currentIndex = 0;
  private VmTrace currentTrace;
  private TraceFrame currentTraceFrame;
  private String currentOperation;
  private final TransactionTrace transactionTrace;
  private final VmTrace rootVmTrace = new VmTrace();
  private final Deque<VmTrace> parentTraces = new ArrayDeque<>();
  private int lastDepth = 0;

  public VmTraceGenerator(final TransactionTrace transactionTrace) {
    this.transactionTrace = transactionTrace;
  }

  /**
   * Generate a stream of trace result objects.
   *
   * @return a representation of generated traces.
   */
  public Stream<Trace> generateTraceStream() {
    return Stream.of(generateTrace());
  }

  /**
   * Generate trace representation from the specified transaction trace.
   *
   * @return a representation of the trace.
   */
  private Trace generateTrace() {
    parentTraces.add(rootVmTrace);
    if (transactionTrace != null && !transactionTrace.getTraceFrames().isEmpty()) {
      transactionTrace
          .getTransaction()
          .getInit()
          .map(Bytes::toHexString)
          .ifPresent(rootVmTrace::setCode);
      final Iterator<TraceFrame> iter = transactionTrace.getTraceFrames().iterator();
      Optional<TraceFrame> nextTraceFrame =
          iter.hasNext() ? Optional.of(iter.next()) : Optional.empty();
      while (nextTraceFrame.isPresent()) {
        final TraceFrame traceFrame = nextTraceFrame.get();
        nextTraceFrame = iter.hasNext() ? Optional.of(iter.next()) : Optional.empty();
        addFrame(traceFrame, nextTraceFrame);
      }
    }
    return rootVmTrace;
  }

  /**
   * Add a trace frame to the VmTrace result object.
   *
   * @param frame the current trace frame
   */
  private void addFrame(final TraceFrame frame, final Optional<TraceFrame> nextTraceFrame) {
    handleDepthDecreased(frame);
    if (!mustIgnore(frame)) {
      initStep(frame);
      final VmOperation op = buildVmOperation();
      final VmOperationExecutionReport report = generateExecutionReport();
      generateTracingMemory(report);
      generateTracingPush(report);
      generateTracingStorage(report);
      handleDepthIncreased(op, report, nextTraceFrame);
      completeStep(frame, op, report);
      lastDepth = frame.getDepth();
    }
  }

  private boolean mustIgnore(final TraceFrame frame) {
    if ("STOP".equals(frame.getOpcode()) && transactionTrace.getTraceFrames().size() == 1) {
      return true;
    } else if (frame.isVirtualOperation() && frame.isPrecompile()) {
      // synthetic frame for a transaction sent directly to a precompile: no opcode was executed
      return true;
    } else if (frame.getExceptionalHaltReason().isPresent()) {
      final Optional<ExceptionalHaltReason> haltReason = frame.getExceptionalHaltReason();
      return haltReason.get() != ExceptionalHaltReason.INVALID_JUMP_DESTINATION
          && haltReason.get() != ExceptionalHaltReason.INSUFFICIENT_GAS
          && haltReason.get() != ExceptionalHaltReason.ILLEGAL_STATE_CHANGE;
    } else {
      return frame.isVirtualOperation();
    }
  }

  private void completeStep(
      final TraceFrame frame, final VmOperation op, final VmOperationExecutionReport report) {
    // add the operation representation to the list of traces
    final Optional<ExceptionalHaltReason> exceptionalHaltReason = frame.getExceptionalHaltReason();
    if (frame.getDepth() > 0
        && exceptionalHaltReason.isPresent()
        && exceptionalHaltReason.get() == ExceptionalHaltReason.INSUFFICIENT_GAS) {
      op.setVmOperationExecutionReport(null);
    } else {
      op.setVmOperationExecutionReport(report);
    }
    if (currentTrace != null) {
      currentTrace.add(op);
    }
    currentIndex++;
  }

  private void handleDepthIncreased(
      final VmOperation op,
      final VmOperationExecutionReport report,
      final Optional<TraceFrame> nextTraceFrame) {
    // check if next frame depth has increased i.e the current operation is a call
    switch (currentOperation) {
      case "STATICCALL":
      case "DELEGATECALL":
      case "CALLCODE":
      case "CALL":
      case "CREATE":
      case "CREATE2":
        if (currentOperation.equals("CALL") || currentOperation.equals("DELEGATECALL")) {
          findReturnInCall(currentTraceFrame, currentIndex)
              .map(output -> new Mem(output.getOutputData().toHexString(), 0))
              .ifPresent(report::setMem);
        }

        findLastFrameInCall(currentTraceFrame, currentIndex)
            .ifPresent(
                lastFrameInCall -> {
                  report.setUsed(lastFrameInCall.getGasRemaining());
                  lastFrameInCall
                      .getStack()
                      .filter(stack -> stack.length > 0)
                      .map(stack -> stack[stack.length - 1])
                      .map(last -> Quantity.create(UInt256.fromHexString(last.toHexString())))
                      .ifPresent(report::singlePush);
                  if (!currentOperation.startsWith("CREATE")) {
                    lastFrameInCall
                        .getMaybeUpdatedMemory()
                        .map(mem -> new Mem(mem.getValue().toHexString(), mem.getOffset()))
                        .ifPresent(report::setMem);
                  }
                });

        if (currentTraceFrame.getMaybeCode().map(Code::getSize).orElse(0) > 0) {
          if (nextTraceFrame.map(TraceFrame::getDepth).orElse(0) > currentTraceFrame.getDepth()) {
            op.setCost(currentTraceFrame.getGasRemainingPostExecution() + op.getCost());
            final VmTrace newSubTrace = new VmTrace();
            parentTraces.addLast(newSubTrace);
            op.setSub(newSubTrace);
          } else if (currentTraceFrame.getDepth() == 0) {
            op.setCost(
                currentTraceFrame.getGasRemaining()
                    - currentTraceFrame.getGasRemainingPostExecution());
            currentTraceFrame
                .getMaybeCode()
                .ifPresent(
                    code ->
                        op.setSub(
                            new VmTrace(
                                currentTraceFrame.getMaybeCode().get().getBytes().toHexString())));
          } else {
            op.setCost(op.getCost());
            op.setSub(null);
          }
        } else {
          if (currentTraceFrame.getPrecompiledGasCost().isPresent()) {
            op.setCost(op.getCost() + currentTraceFrame.getPrecompiledGasCost().orElse(0L));
          } else if ((currentOperation.equals("STATICCALL") || currentOperation.equals("CALL"))
              && nextTraceFrame.map(TraceFrame::getDepth).orElse(0)
                  > currentTraceFrame.getDepth()) {
            op.setCost(currentTraceFrame.getGasRemainingPostExecution() + op.getCost());
          }
          op.setSub(new VmTrace());
        }
        break;
      default:
        break;
    }
  }

  private void handleDepthDecreased(final TraceFrame frame) {
    // check if next frame depth has decreased i.e the current operation closes the parent trace
    if (currentTraceFrame != null && frame.getDepth() < lastDepth) {
      currentTrace = parentTraces.removeLast();
    }
  }

  private VmOperation buildVmOperation() {
    final VmOperation op = new VmOperation();
    // set gas cost and program counter
    op.setCost(currentTraceFrame.getGasCost().orElse(0L));
    op.setPc(currentTraceFrame.getPc());
    // op.setOperation(currentOperation);
    return op;
  }

  private VmOperationExecutionReport generateExecutionReport() {
    final VmOperationExecutionReport report = new VmOperationExecutionReport();
    // set gas remaining
    report.setUsed(
        currentTraceFrame.getGasRemaining() - (currentTraceFrame.getGasCost().orElse(0L)));
    return report;
  }

  private void generateTracingMemory(final VmOperationExecutionReport report) {
    switch (currentOperation) {
      case "CALLDATACOPY":
      case "CODECOPY":
      case "EXTCODECOPY":
      case "MLOAD":
      case "MSTORE":
      case "MSTORE8":
      case "RETURNDATACOPY":
        currentTraceFrame
            .getMaybeUpdatedMemory()
            .map(
                updatedMemory ->
                    new Mem(updatedMemory.getValue().toHexString(), updatedMemory.getOffset()))
            .ifPresent(report::setMem);
        break;
      default:
        break;
    }
  }

  private void generateTracingPush(final VmOperationExecutionReport report) {
    // set push from stack elements if some elements have been produced
    if (currentTraceFrame.getStackItemsProduced() > 0) {
      currentTraceFrame
          .getStackPostExecution()
          .filter(stack -> stack.length > 0)
          .ifPresent(
              stack ->
                  IntStream.range(0, currentTraceFrame.getStackItemsProduced())
                      .mapToObj(i -> Bytes.wrap(stack[stack.length - i - 1]).trimLeadingZeros())
                      .map(value -> Quantity.create(UInt256.fromHexString(value.toHexString())))
                      .forEach(report::addPush));
    }
  }

  private void generateTracingStorage(final VmOperationExecutionReport report) {
    // set storage if updated
    currentTraceFrame
        .getMaybeUpdatedStorage()
        .ifPresent(
            entry ->
                report.setStore(
                    new Store(
                        entry.getOffset().toQuantityHexString(),
                        entry.getValue().toQuantityHexString())));
  }

  /**
   * Set current trace from parents queue and retrieve next frame.
   *
   * @param frame the trace frame.
   */
  private void initStep(final TraceFrame frame) {
    this.currentTraceFrame = frame;
    this.currentOperation = frame.getOpcode();
    currentTrace = parentTraces.peekLast();
    // set smart contract code
    if (currentTrace != null && "0x".equals(currentTrace.getCode())) {
      currentTrace.setCode(
          currentTraceFrame.getMaybeCode().orElse(Code.EMPTY_CODE).getBytes().toHexString());
    }
  }

  /**
   * Find the last frame in the call.
   *
   * @param callFrame the CALL frame
   * @param callIndex the CALL frame index
   * @return an {@link Optional} of {@link TraceFrame} containing the last frame in the call.
   */
  private Optional<TraceFrame> findLastFrameInCall(
      final TraceFrame callFrame, final int callIndex) {
    for (int i = callIndex; i < transactionTrace.getTraceFrames().size(); i++) {
      if (i + 1 < transactionTrace.getTraceFrames().size()) {
        final TraceFrame next = transactionTrace.getTraceFrames().get(i + 1);
        if (next.getPc() == (callFrame.getPc() + 1) && next.getDepth() == callFrame.getDepth()) {
          return Optional.of(next);
        }
      }
    }
    return Optional.empty();
  }

  private Optional<TraceFrame> findReturnInCall(final TraceFrame callFrame, final int callIndex) {
    for (int i = callIndex; i < transactionTrace.getTraceFrames().size(); i++) {
      if (i + 1 < transactionTrace.getTraceFrames().size()) {
        final TraceFrame next = transactionTrace.getTraceFrames().get(i + 1);
        if (next.getOpcode().equals("RETURN") && next.getDepth() == callFrame.getDepth()) {
          return Optional.of(next);
        }
      }
    }
    return Optional.empty();
  }
}
