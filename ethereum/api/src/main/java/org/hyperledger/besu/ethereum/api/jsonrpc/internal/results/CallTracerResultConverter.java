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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.CallTracerErrorHandler;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.CallTracerGasCalculator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.OpcodeCategory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.StackExtractor;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;

/**
 * Converts Ethereum transaction traces into a hierarchical call tracer format.
 *
 * <p>This class transforms the flat sequence of trace frames produced during transaction execution
 * into a structured call tree similar to Geth's callTracer output. It captures the relationships
 * between calls, their inputs, outputs, gas usage, and error states.
 *
 * <p>The converter handles various EVM operations including:
 *
 * <ul>
 *   <li>Contract calls (CALL, STATICCALL, DELEGATECALL, CALLCODE)
 *   <li>Contract creation (CREATE, CREATE2)
 *   <li>Precompiled contracts (addresses 0x01-0x0a)
 *   <li>Return operations (RETURN, REVERT, STOP, SELFDESTRUCT)
 * </ul>
 */
public class CallTracerResultConverter {

  // Operation type constants
  private static final String CALL_TYPE = "CALL";
  private static final String CALLCODE_TYPE = "CALLCODE";
  private static final String DELEGATECALL_TYPE = "DELEGATECALL";
  private static final String STATICCALL_TYPE = "STATICCALL";
  private static final String CREATE_TYPE = "CREATE";
  private static final String SELFDESTRUCT_TYPE = "SELFDESTRUCT";

  private static final String ZERO_VALUE = "0x0";

  private CallTracerResultConverter() {
    // Utility class - prevent instantiation
  }

  /**
   * Converts a transaction trace to a call tracer result.
   *
   * @param transactionTrace The transaction trace to convert
   * @return A call tracer result representing the transaction's call hierarchy
   * @throws NullPointerException if transactionTrace or its components are null
   */
  public static CallTracerResult convert(final TransactionTrace transactionTrace) {
    return convert(transactionTrace, false);
  }

  /**
   * Converts a transaction trace to a call tracer result.
   *
   * @param transactionTrace The transaction trace to convert
   * @param onlyTopCall When true, only the top-level call is reported and all nested calls are
   *     omitted, per the execution-apis callTracer {@code onlyTopCall} option
   * @return A call tracer result representing the transaction's call hierarchy
   * @throws NullPointerException if transactionTrace or its components are null
   */
  public static CallTracerResult convert(
      final TransactionTrace transactionTrace, final boolean onlyTopCall) {
    checkNotNull(
        transactionTrace, "CallTracerResultConverter requires a non-null TransactionTrace");
    checkNotNull(
        transactionTrace.getTransaction(),
        "CallTracerResultConverter requires non-null Transaction");
    checkNotNull(
        transactionTrace.getResult(), "CallTracerResultConverter requires non-null Result");

    if (transactionTrace.getTraceFrames() == null || transactionTrace.getTraceFrames().isEmpty()) {
      return createRootCallFromTransaction(transactionTrace);
    }

    return buildCallHierarchyFromFrames(transactionTrace, onlyTopCall);
  }

  private static CallTracerResult buildCallHierarchyFromFrames(
      final TransactionTrace trace, final boolean onlyTopCall) {
    final CallTracerResult.Builder rootBuilder = initializeRootBuilder(trace);

    // onlyTopCall: report just the root frame and omit nested calls entirely. The root's own
    // fields are fully set by initializeRootBuilder from the transaction and its result, so the
    // frame walk (which only adds nested calls) can be skipped.
    if (onlyTopCall) {
      return rootBuilder.build();
    }

    // Only process frames if transaction succeeded OR reverted (not exceptional halt)
    // Exceptional halts (stack underflow, out of gas, etc.) happen before execution really begins
    if (!trace.getResult().isSuccessful()
        && trace.getResult().getExceptionalHaltReason().isPresent()) {
      return rootBuilder.build();
    }

    final List<TraceFrame> frames = trace.getTraceFrames();
    final Map<Integer, CallInfo> depthToCallInfo = new HashMap<>();

    depthToCallInfo.put(0, new CallInfo(rootBuilder, null));

    TraceFrame previousFrame = null;

    for (int i = 0; i < frames.size(); i++) {
      final TraceFrame frame = frames.get(i);
      final TraceFrame nextTrace = (i < frames.size() - 1) ? frames.get(i + 1) : null;

      // Handle implicit returns when depth decreases
      if (shouldHandleImplicitReturn(previousFrame, frame)) {
        handleImplicitReturn(previousFrame, depthToCallInfo);
      }

      processFrame(frame, nextTrace, depthToCallInfo);

      previousFrame = frame;
    }

    processRemainingCalls(depthToCallInfo);
    return depthToCallInfo.get(0).builder.build();
  }

  private static void processFrame(
      final TraceFrame frame,
      final TraceFrame nextTrace,
      final Map<Integer, CallInfo> depthToCallInfo) {

    final String opcode = frame.getOpcode();

    if (OpcodeCategory.isCallOp(opcode) || OpcodeCategory.isCreateOp(opcode)) {
      processCallOrCreate(frame, nextTrace, opcode, depthToCallInfo);
    } else if (OpcodeCategory.isSelfDestructOp(opcode)) {
      handleSelfDestruct(frame, depthToCallInfo.get(frame.getDepth()));
    } else if (OpcodeCategory.isReturnOp(opcode)
        || OpcodeCategory.isRevertOp(opcode)
        || OpcodeCategory.isHaltOp(opcode)) {
      processReturn(frame, opcode, depthToCallInfo);
    }
  }

  private static boolean shouldHandleImplicitReturn(
      final TraceFrame previousFrame, final TraceFrame currentFrame) {
    return previousFrame != null
        && currentFrame.getDepth() < previousFrame.getDepth()
        && !OpcodeCategory.isReturnOp(previousFrame.getOpcode())
        && !OpcodeCategory.isRevertOp(previousFrame.getOpcode())
        && !OpcodeCategory.isHaltOp(previousFrame.getOpcode());
  }

  private static void processCallOrCreate(
      final TraceFrame frame,
      final TraceFrame nextTrace,
      final String opcode,
      final Map<Integer, CallInfo> depthToCallInfo) {

    final CallInfo parentCallInfo = depthToCallInfo.get(frame.getDepth());
    final CallTracerResult.Builder childBuilder =
        createCallBuilder(frame, nextTrace, opcode, parentCallInfo);

    // Handle precompiles
    if (frame.isPrecompile()) {
      finalizePrecompileChild(frame, childBuilder, parentCallInfo);
      return;
    }

    // Check if call entered (depth increased)
    final boolean calleeEntered = nextTrace != null && nextTrace.getDepth() > frame.getDepth();

    if (!calleeEntered) {
      handleNonEnteredCall(frame, opcode, childBuilder, parentCallInfo);
      return;
    }

    // Track child for later processing
    depthToCallInfo.put(frame.getDepth() + 1, new CallInfo(childBuilder, frame));
  }

  private static void processReturn(
      final TraceFrame frame, final String opcode, final Map<Integer, CallInfo> depthToCallInfo) {

    final CallInfo childCallInfo = depthToCallInfo.get(frame.getDepth());
    if (childCallInfo == null || childCallInfo.entryFrame == null) {
      return;
    }

    // Handle CREATE contract address
    // Only set 'to' if CREATE succeeded (no REVERT or exceptional halt)
    if (OpcodeCategory.isCreateOp(childCallInfo.builder.getType())
        && frame.getDepth() > 0
        && childCallInfo.builder.getTo() == null
        && frame.getExceptionalHaltReason().isEmpty()
        && !OpcodeCategory.isRevertOp(opcode)) {
      childCallInfo.builder.to(frame.getRecipient().getBytes().toHexString());
    }

    // Set output and error status
    CallTracerErrorHandler.setOutputAndErrorStatus(childCallInfo.builder, frame, opcode);

    // Calculate and set gas used
    final long gasUsed =
        CallTracerGasCalculator.calculateGasUsed(
            childCallInfo.builder.getGas(),
            childCallInfo.builder.getType(),
            childCallInfo.entryFrame,
            frame);
    childCallInfo.builder.gasUsed(gasUsed);

    // Add to parent's calls
    final CallInfo parentCallInfo = depthToCallInfo.get(frame.getDepth() - 1);
    if (parentCallInfo != null) {
      parentCallInfo.builder.addCall(childCallInfo.builder.build());
    }

    depthToCallInfo.remove(frame.getDepth());
  }

  private static void handleNonEnteredCall(
      final TraceFrame frame,
      final String opcode,
      final CallTracerResult.Builder childBuilder,
      final CallInfo parentCallInfo) {

    // Check for soft failure vs hard failure
    if (frame.getExceptionalHaltReason().isPresent()) {
      CallTracerErrorHandler.handleHardFailure(frame, opcode, childBuilder);
    } else if (frame.getSoftFailureReason().isPresent()) {
      CallTracerErrorHandler.handleSoftFailure(frame, opcode, childBuilder);
    } else {
      // Unknown failure type - no error message
      childBuilder.gasUsed(0L);
      if (OpcodeCategory.isCreateOp(opcode)) {
        childBuilder.to(null);
      }
    }

    // Add to parent immediately
    if (parentCallInfo != null) {
      parentCallInfo.builder.addCall(childBuilder.build());
    }
  }

  private static void processRemainingCalls(final Map<Integer, CallInfo> depthToCallInfo) {
    depthToCallInfo.keySet().stream()
        .filter(depth -> depth > 0)
        .sorted(Comparator.reverseOrder())
        .forEach(
            depth -> {
              CallInfo child = depthToCallInfo.get(depth);
              CallInfo parent = depthToCallInfo.get(depth - 1);
              if (child != null && parent != null) {
                parent.builder.addCall(child.builder.build());
              }
            });
  }

  private static CallTracerResult.Builder createCallBuilder(
      final TraceFrame frame,
      final TraceFrame nextTrace,
      final String opcode,
      final CallInfo parentCallInfo) {

    final String fromAddress =
        (parentCallInfo != null && parentCallInfo.builder != null)
            ? parentCallInfo.builder.getTo()
            : null;

    final String toAddress = resolveToAddress(frame, opcode);
    final long gasProvided = CallTracerGasCalculator.computeGasProvided(frame, nextTrace);
    final boolean isPrecompile = frame.isPrecompile();

    final CallTracerResult.Builder builder =
        CallTracerResult.builder()
            .type(opcode)
            .from(fromAddress)
            .to(toAddress)
            .value(getCallValue(frame, opcode))
            .gas(gasProvided);

    if (!isPrecompile) {
      builder.input(StackExtractor.resolveCallInputData(frame, nextTrace, opcode).toHexString());
    }

    return builder;
  }

  private static CallTracerResult.Builder initializeRootBuilder(final TransactionTrace trace) {
    final Transaction tx = trace.getTransaction();
    final TransactionProcessingResult result = trace.getResult();

    final CallTracerResult.Builder builder =
        CallTracerResult.builder()
            .type(tx.isContractCreation() ? CREATE_TYPE : CALL_TYPE)
            .from(tx.getSender().getBytes().toHexString())
            .to(
                tx.isContractCreation()
                    ? tx.contractAddress().map(a -> a.getBytes().toHexString()).orElse(null)
                    : tx.getTo().map(a -> a.getBytes().toHexString()).orElse(null))
            .value(tx.getValue().toShortHexString())
            .gas(tx.getGasLimit())
            .input(tx.getPayload().toHexString())
            .gasUsed(tx.getGasLimit() - result.getGasRemaining());

    // Set output if present
    if (result.getOutput() != null && !result.getOutput().isEmpty()) {
      builder.output(result.getOutput().toHexString());
    }

    // Handle errors
    if (!result.isSuccessful()) {
      CallTracerErrorHandler.handleRootError(builder, tx, result);
    }

    return builder;
  }

  private static String getCallValue(final TraceFrame frame, final String opcode) {
    if (STATICCALL_TYPE.equals(opcode)) {
      return null;
    }

    if (DELEGATECALL_TYPE.equals(opcode)) {
      return ZERO_VALUE;
    }

    if (CALL_TYPE.equals(opcode) || CALLCODE_TYPE.equals(opcode)) {
      return StackExtractor.extractCallValue(frame);
    }

    if (OpcodeCategory.isCreateOp(opcode)) {
      return StackExtractor.extractCreateValue(frame);
    }

    return frame.getValue().toShortHexString();
  }

  private static String resolveToAddress(final TraceFrame frame, final String opcode) {
    if (OpcodeCategory.isCreateOp(opcode)) {
      return null;
    }

    if (frame.isPrecompile() && frame.getPrecompileRecipient().isPresent()) {
      return frame.getPrecompileRecipient().get().getBytes().toHexString();
    }

    if (OpcodeCategory.isCallOp(opcode)) {
      return StackExtractor.extractCallToAddress(frame);
    }

    return null;
  }

  private static void finalizePrecompileChild(
      final TraceFrame entryFrame,
      final CallTracerResult.Builder childBuilder,
      final CallInfo parentCallInfo) {

    // Calculate gas for precompile (Geth-style)
    final long cap = CallTracerGasCalculator.calculatePrecompileGas(entryFrame);
    childBuilder.gas(cap);

    // Handle precompile failure and calculate gas used
    final boolean hasFailed = entryFrame.getExceptionalHaltReason().isPresent();
    if (hasFailed) {
      CallTracerErrorHandler.handlePrecompileError(entryFrame, childBuilder);
    }

    final long gasUsed =
        CallTracerGasCalculator.calculatePrecompileGasUsed(entryFrame, hasFailed, cap);
    childBuilder.gasUsed(gasUsed);

    // Set I/O data
    childBuilder.input(entryFrame.getPrecompileInputData().map(Bytes::toHexString).orElse(null));
    childBuilder.output(entryFrame.getPrecompileOutputData().map(Bytes::toHexString).orElse(null));

    // Add to parent
    if (parentCallInfo != null) {
      parentCallInfo.builder.addCall(childBuilder.build());
    }
  }

  private static void handleSelfDestruct(final TraceFrame frame, final CallInfo currentCallInfo) {
    if (currentCallInfo == null || frame.getExceptionalHaltReason().isPresent()) {
      return;
    }

    StackExtractor.extractSelfDestructBeneficiary(frame)
        .ifPresent(
            beneficiary -> {
              final String from = frame.getRecipient().getBytes().toHexString();
              final String value = StackExtractor.extractSelfDestructValue(frame, beneficiary);

              final CallTracerResult selfDestructCall =
                  CallTracerResult.builder()
                      .type(SELFDESTRUCT_TYPE)
                      .from(from)
                      .to(beneficiary.getBytes().toHexString())
                      .gas(0L)
                      .gasUsed(0L)
                      .value(value)
                      .input("0x")
                      .build();

              currentCallInfo.builder.addCall(selfDestructCall);
            });
  }

  private static void handleImplicitReturn(
      final TraceFrame lastFrameAtDepth, final Map<Integer, CallInfo> depthToCallInfo) {

    final int failedDepth = lastFrameAtDepth.getDepth();
    final CallInfo childCallInfo = depthToCallInfo.get(failedDepth);

    if (childCallInfo == null) {
      return;
    }

    // Set error if exceptional halt
    lastFrameAtDepth
        .getExceptionalHaltReason()
        .ifPresent(reason -> childCallInfo.builder.error(reason.getDescription()));

    // Calculate gas used
    if (childCallInfo.entryFrame != null) {
      long gasUsed =
          CallTracerGasCalculator.calculateImplicitReturnGasUsed(
              lastFrameAtDepth, childCallInfo.builder.getGas());
      childCallInfo.builder.gasUsed(gasUsed);
    }

    // Add to parent and remove from tracking
    CallInfo parentCallInfo = depthToCallInfo.get(failedDepth - 1);
    if (parentCallInfo != null) {
      parentCallInfo.builder.addCall(childCallInfo.builder.build());
    }

    depthToCallInfo.remove(failedDepth);
  }

  private static CallTracerResult createRootCallFromTransaction(final TransactionTrace trace) {
    final Transaction tx = trace.getTransaction();
    final TransactionProcessingResult result = trace.getResult();

    return initializeRootBuilder(trace)
        .gasUsed(tx.getGasLimit() - result.getGasRemaining())
        .build();
  }

  /** Helper class to track call information during trace processing. */
  private record CallInfo(CallTracerResult.Builder builder, TraceFrame entryFrame) {}
}
