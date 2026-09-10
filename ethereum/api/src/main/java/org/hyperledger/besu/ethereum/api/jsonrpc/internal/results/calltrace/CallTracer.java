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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CallTracerResult;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.SoftFailureReason;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Native {@code callTracer} implementation built directly on {@link OperationTracer} context hooks
 * ({@link #traceContextEnter}, {@link #traceContextExit}) instead of post-processing a full
 * opcode-level trace.
 *
 * <p>Each real {@link MessageFrame} (root transaction, CALL/CALLCODE/DELEGATECALL/STATICCALL,
 * CREATE/CREATE2, and precompile invocations) gets exactly one enter/exit pair, from which the call
 * tree, gas accounting, inputs/outputs and error/revert information are read directly - no
 * opcode-level stack, memory or storage trace is ever allocated.
 *
 * <p>Failed CALL/CREATE attempts with valid operands become synthetic leaves for Geth parity.
 *
 * <p>One instance traces exactly one transaction and is not thread-safe; {@link
 * #traceStartTransaction} resets all per-transaction state so an instance may be reused
 * sequentially, matching {@code DebugOperationTracer.reset()} semantics.
 */
public class CallTracer implements OperationTracer {

  private static final String CALL = "CALL";
  private static final String CALLCODE = "CALLCODE";
  private static final String DELEGATECALL = "DELEGATECALL";
  private static final String STATICCALL = "STATICCALL";
  private static final String CREATE = "CREATE";
  private static final String CREATE2 = "CREATE2";
  private static final String SELFDESTRUCT = "SELFDESTRUCT";

  private static final String EXECUTION_REVERTED = "execution reverted";
  private static final String PRECOMPILE_FAILED = "precompile failed";
  private static final String ZERO_VALUE = "0x0";

  private static final long GAS_CALL_STIPEND_DIVISOR = 64L;

  private final boolean onlyTopCall;
  private final Deque<Node> callStack = new ArrayDeque<>();

  private String rootType;
  private long rootGas;
  private CallTracerResult.Builder rootBuilder;

  // Captured in tracePreExecution for the CALL/CREATE/SELFDESTRUCT opcode about to run on the
  // *current* frame; consumed immediately afterwards by traceContextEnter (real entry),
  // tracePostExecution (soft/hard failure to enter, or self-destruct).
  private CallTracerResult.Builder pendingBuilder;
  private long pendingInOffset;
  private long pendingInLength;

  /**
   * Instantiates a new Call tracer.
   *
   * @param traceOptions the trace options containing the tracer configuration
   */
  public CallTracer(final TraceOptions traceOptions) {
    this.onlyTopCall =
        Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("onlyTopCall", false));
  }

  @Override
  public void traceStartTransaction(final WorldView worldView, final Transaction transaction) {
    this.rootType = transaction.isContractCreation() ? CREATE : CALL;
    this.rootGas = transaction.getGasLimit();
    this.rootBuilder = null;
    this.pendingBuilder = null;
    this.pendingInOffset = 0L;
    this.pendingInLength = 0L;
    this.callStack.clear();
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    if (onlyTopCall) {
      return;
    }
    final Operation op = frame.getCurrentOperation();
    if (op == null) {
      return;
    }
    switch (op.getName()) {
      case CALL, CALLCODE, DELEGATECALL, STATICCALL -> capturePendingCall(frame, op);
      case CREATE, CREATE2 -> capturePendingCreate(frame, op);
      case SELFDESTRUCT -> capturePendingSelfDestruct(frame, op);
      default -> {}
    }
  }

  @Override
  public void tracePostExecution(final MessageFrame frame, final Operation.OperationResult result) {
    if (onlyTopCall) {
      return;
    }
    if (frame.getState() == MessageFrame.State.CODE_SUSPENDED) {
      return; // child frame spawned; traceContextEnter/Exit own it
    }
    final Operation op = frame.getCurrentOperation();
    if (op == null) {
      return;
    }
    switch (op.getName()) {
      case CALL, CALLCODE, DELEGATECALL, STATICCALL, CREATE, CREATE2 ->
          addFailedCallOrCreateLeaf(frame, result, op.getName());
      case SELFDESTRUCT -> handleSelfDestructPostExecution(frame, result);
      default -> {}
    }
  }

  @Override
  public void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {
    if (onlyTopCall || callStack.isEmpty()) {
      return;
    }
    final Node node = callStack.peek();
    node.isPrecompile = true;
    // traceContextEnter already fired for this precompile frame with its exact allocated gas;
    // no approximation needed.
    final long cap = node.builder.getGas().longValueExact();

    if (frame.getExceptionalHaltReason().isPresent()) {
      String message =
          frame
              .getExceptionalHaltReason()
              .map(ExceptionalHaltReason::getDescription)
              .orElse(PRECOMPILE_FAILED);
      if (frame.getRevertReason().isPresent()) {
        message = new String(frame.getRevertReason().get().toArrayUnsafe(), StandardCharsets.UTF_8);
      }
      node.builder.error(message);
      node.builder.gasUsed(cap);
    } else {
      node.builder.gasUsed(gasRequirement);
    }
  }

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    if (onlyTopCall && frame.getDepth() != 0) {
      return;
    }
    final boolean isRoot = callStack.isEmpty();
    final boolean hadPending = pendingBuilder != null;
    final CallTracerResult.Builder callBuilder;
    final String type;
    if (isRoot) {
      type = rootType;
      callBuilder = CallTracerResult.builder().type(type).gas(rootGas);
    } else {
      if (hadPending) {
        callBuilder = pendingBuilder;
        type = callBuilder.getType();
      } else {
        type = frame.getType() == MessageFrame.Type.CONTRACT_CREATION ? CREATE : CALL;
        callBuilder = CallTracerResult.builder().type(type);
      }
      callBuilder.gas(frame.getRemainingGas());
    }
    callBuilder.from(
        isRoot
            ? frame.getSenderAddress().getBytes().toHexString()
            : callStack.peek().ownAddress.getBytes().toHexString());

    final boolean isCreate = isCreateType(type);
    if (!isCreate) {
      if (!hadPending) {
        callBuilder.to(frame.getContractAddress().getBytes().toHexString());

        final String value =
            switch (type) {
              case STATICCALL -> null; // staticcall value is intentionally null
              case DELEGATECALL -> frame.getApparentValue().toShortHexString();
              default -> frame.getValue().toShortHexString();
            };
        callBuilder.value(value);
      }
      callBuilder.input(frame.getInputData().toHexString());
    } else {
      if (!hadPending) {
        callBuilder.value(frame.getValue().toShortHexString());
      }
      callBuilder.input(frame.getCode().getBytes().toHexString());
    }

    if (isRoot) {
      rootBuilder = callBuilder;
    }
    pendingBuilder = null;
    callStack.push(new Node(callBuilder, frame.getRemainingGas(), frame.getRecipientAddress()));
  }

  @Override
  public void traceContextExit(final MessageFrame frame) {
    if (onlyTopCall && frame.getDepth() != 0) {
      return;
    }
    final Node node = callStack.pop();
    finalizeNode(node, frame);
    if (callStack.isEmpty()) {
      rootBuilder = node.builder;
    } else {
      callStack.peek().builder.addCall(node.builder.build());
    }
  }

  /**
   * Finalizes and returns the call tracer result for the root transaction.
   *
   * <p>The root's gas accounting (refunds, EIP-8037 state gas, EIP-7623 floor cost) and final
   * error/revert classification are economics the EVM frame alone cannot express; they are sourced
   * from the authoritative {@link TransactionProcessingResult} instead of being reconstructed here,
   * exactly as the legacy post-processing converter did.
   *
   * @param tx the traced transaction
   * @param result the transaction's authoritative processing result
   * @return the completed call tracer result
   */
  public CallTracerResult buildResult(
      final Transaction tx, final TransactionProcessingResult result) {
    if (rootBuilder == null) {
      // Validation failed before any frame was traced (e.g. debug_traceBlock replaying an
      // invalid transaction) - synthesize the root call from the transaction itself, like the
      // legacy converter did.
      rootBuilder =
          CallTracerResult.builder()
              .type(tx.isContractCreation() ? CREATE : CALL)
              .from(tx.getSender().getBytes().toHexString())
              .to(
                  tx.isContractCreation()
                      ? tx.contractAddress().map(a -> a.getBytes().toHexString()).orElse(null)
                      : tx.getTo().map(a -> a.getBytes().toHexString()).orElse(null))
              .value(tx.getValue().toShortHexString())
              .gas(tx.getGasLimit())
              .input(tx.getPayload().toHexString());
      if (result.getOutput() != null && !result.getOutput().isEmpty()) {
        rootBuilder.output(result.getOutput().toHexString());
      }
    }
    rootBuilder.gasUsed(tx.getGasLimit() - result.getGasRemaining());
    if (!result.isSuccessful()) {
      applyRootError(tx, result);
    }
    return rootBuilder.build();
  }

  private void applyRootError(final Transaction tx, final TransactionProcessingResult result) {
    final String errorMessage =
        result
            .getExceptionalHaltReason()
            .map(ExceptionalHaltReason::getDescription)
            .orElse(EXECUTION_REVERTED);
    rootBuilder.error(errorMessage);
    if (tx.isContractCreation()) {
      rootBuilder.to(null);
      result.getRevertReason().ifPresent(rootBuilder::revertReason);
    } else if (result.getExceptionalHaltReason().isEmpty()
        && result.getRevertReason().isPresent()) {
      rootBuilder.output(result.getRevertReason().get().toHexString());
      JsonRpcErrorResponse.decodeRevertReason(result.getRevertReason().get())
          .ifPresent(rootBuilder::revertReasonDecoded);
    }
  }

  // ------------------------------------------------------------------------------------------
  // Node finalisation
  // ------------------------------------------------------------------------------------------

  private void finalizeNode(final Node node, final MessageFrame frame) {
    final CallTracerResult.Builder callBuilder = node.builder;
    final Bytes output = frame.getOutputData();
    if (output != null && !output.isEmpty()) {
      callBuilder.output(output.toHexString());
    }
    if (node.isPrecompile
        && (frame.getExceptionalHaltReason().isPresent()
            || frame.getState() != MessageFrame.State.COMPLETED_FAILED)) {
      return;
    }

    final Optional<ExceptionalHaltReason> halt = frame.getExceptionalHaltReason();
    if (halt.isPresent()) {
      callBuilder.error(halt.get().getDescription());
      frame.getRevertReason().ifPresent(callBuilder::revertReason);
    } else if (frame.getState() == MessageFrame.State.COMPLETED_FAILED) {
      // Completed-failed without an exceptional halt reason only happens via REVERT.
      callBuilder.error(EXECUTION_REVERTED);
      Bytes revertBytes = frame.getRevertReason().orElse(null);
      if ((revertBytes == null || revertBytes.isEmpty()) && output != null && !output.isEmpty()) {
        revertBytes = output;
      }
      if (revertBytes != null && !revertBytes.isEmpty()) {
        if (output == null || output.isEmpty()) {
          callBuilder.output(revertBytes.toHexString());
        }
        JsonRpcErrorResponse.decodeRevertReason(revertBytes)
            .ifPresent(callBuilder::revertReasonDecoded);
      }
    } else if (isCreateType(callBuilder.getType())) {
      callBuilder.to(frame.getContractAddress().getBytes().toHexString());
    }
    callBuilder.gasUsed(Math.max(0L, node.entryGas - frame.getRemainingGas()));
  }

  /**
   * Only reached when a CALL/CREATE opcode failed before spawning a child frame (soft: insufficient
   * balance / max depth; hard: out of gas, static-context violation, initcode too large). Geth
   * still emits a leaf for these; see specs 30, 32, 34.
   */
  private void addFailedCallOrCreateLeaf(
      final MessageFrame frame, final Operation.OperationResult result, final String opcode) {
    if (result.getHaltReason() == ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS) {
      // Operand validation failed before a child call was attempted.
      pendingBuilder = null;
      return;
    }
    final CallTracerResult.Builder cb =
        pendingBuilder != null ? pendingBuilder : CallTracerResult.builder().type(opcode);
    cb.from(callStack.isEmpty() ? null : callStack.peek().ownAddress.getBytes().toHexString());
    if (pendingBuilder == null) {
      if (STATICCALL.equals(opcode)) {
        // value intentionally omitted (null) for STATICCALL
      } else if (DELEGATECALL.equals(opcode)) {
        cb.value(frame.getApparentValue().toShortHexString());
      } else {
        cb.value(ZERO_VALUE);
      }
      cb.input(frame.getInputData().toHexString());
    } else {
      // Clamp to already-expanded memory: offset/length are unclamped stack values, and the
      // call/create failed before memory was expanded to fit them.
      final long available = frame.memoryByteSize();
      final long len =
          pendingInOffset >= available
              ? 0L
              : Math.min(Math.max(0L, pendingInLength), available - pendingInOffset);
      final Bytes data = len == 0 ? Bytes.EMPTY : frame.readMemory(pendingInOffset, len);
      cb.input(data.toHexString());
    }

    final Optional<SoftFailureReason> soft = result.getSoftFailureReason();
    if (soft.isPresent()) {
      // CALL-family soft failures carry the gas that would have been forwarded; CREATE-family
      // soft failures do not, so fall back to the standard 63/64 approximation like hard failures.
      final long gasAfterSoft = Math.max(0L, frame.getRemainingGas());
      cb.gas(
          result
              .getGasAvailableForChildCall()
              .orElse(Math.max(0L, gasAfterSoft - gasAfterSoft / GAS_CALL_STIPEND_DIVISOR)));
      cb.gasUsed(0L);
      cb.error(soft.get().getDescription());
    } else {
      final ExceptionalHaltReason halt = result.getHaltReason();
      final long gasAfter = Math.max(0L, frame.getRemainingGas());
      cb.gas(Math.max(0L, gasAfter - gasAfter / GAS_CALL_STIPEND_DIVISOR));
      cb.gasUsed(
          halt == ExceptionalHaltReason.INSUFFICIENT_GAS ? Math.max(0L, result.getGasCost()) : 0L);
      cb.error(halt != null ? halt.getDescription() : EXECUTION_REVERTED);
    }
    if (!callStack.isEmpty()) {
      callStack.peek().builder.addCall(cb.build());
    }
    pendingBuilder = null;
  }

  private void handleSelfDestructPostExecution(
      final MessageFrame frame, final Operation.OperationResult result) {
    if (result.getHaltReason() != null || pendingBuilder == null || callStack.isEmpty()) {
      pendingBuilder = null;
      return;
    }
    final Address beneficiary = Address.fromHexString(pendingBuilder.getTo());
    final Address from = frame.getRecipientAddress();
    final Wei value = frame.getRefunds().getOrDefault(beneficiary, Wei.ZERO);
    final CallTracerResult selfDestructCall =
        pendingBuilder
            .from(from.getBytes().toHexString())
            .gas(0L)
            .gasUsed(0L)
            .value(value.toShortHexString())
            .input("0x")
            .build();
    callStack.peek().builder.addCall(selfDestructCall);
    pendingBuilder = null;
  }

  // ------------------------------------------------------------------------------------------
  // Pre-execution snapshotting (stack is about to be consumed by the operation itself)
  // ------------------------------------------------------------------------------------------

  private void capturePendingCall(final MessageFrame frame, final Operation op) {
    if (frame.stackSize() < op.getStackItemsConsumed()) {
      pendingBuilder = null;
      pendingInOffset = 0L;
      pendingInLength = 0L;
      return;
    }
    final String opcode = op.getName();
    final boolean hasValue = CALL.equals(opcode) || CALLCODE.equals(opcode);
    final String toHex = Words.toAddress(frame.getStackItem(1)).getBytes().toHexString();
    pendingBuilder = CallTracerResult.builder().type(opcode).to(toHex);
    if (CALL.equals(opcode) || CALLCODE.equals(opcode)) {
      pendingBuilder.value(Wei.wrap(frame.getStackItem(2)).toShortHexString());
    } else if (DELEGATECALL.equals(opcode)) {
      pendingBuilder.value(frame.getApparentValue().toShortHexString());
    }
    final int offsetIdx = hasValue ? 3 : 2;
    final int lengthIdx = hasValue ? 4 : 3;
    pendingInOffset = Words.clampedToLong(frame.getStackItem(offsetIdx));
    pendingInLength = Words.clampedToLong(frame.getStackItem(lengthIdx));
  }

  private void capturePendingCreate(final MessageFrame frame, final Operation op) {
    if (frame.stackSize() < op.getStackItemsConsumed()) {
      pendingBuilder = null;
      pendingInOffset = 0L;
      pendingInLength = 0L;
      return;
    }
    final String opcode = op.getName();
    pendingBuilder =
        CallTracerResult.builder()
            .type(opcode)
            .value(Wei.wrap(frame.getStackItem(0)).toShortHexString());
    pendingInOffset = Words.clampedToLong(frame.getStackItem(1));
    pendingInLength = Words.clampedToLong(frame.getStackItem(2));
  }

  private void capturePendingSelfDestruct(final MessageFrame frame, final Operation op) {
    if (frame.stackSize() < op.getStackItemsConsumed()) {
      pendingBuilder = null;
      pendingInOffset = 0L;
      pendingInLength = 0L;
      return;
    }
    final Address beneficiary = Words.toAddress(frame.getStackItem(0));
    pendingBuilder =
        CallTracerResult.builder().type(SELFDESTRUCT).to(beneficiary.getBytes().toHexString());
  }

  private static boolean isCreateType(final String type) {
    return CREATE.equals(type) || CREATE2.equals(type);
  }

  // Tracks one in-flight call-tree node, keyed to the MessageFrame it mirrors.
  private static final class Node {
    private final CallTracerResult.Builder builder;
    private final long entryGas;
    private final Address ownAddress;
    private boolean isPrecompile;

    private Node(
        final CallTracerResult.Builder builder, final long entryGas, final Address ownAddress) {
      this.builder = builder;
      this.entryGas = entryGas;
      this.ownAddress = ownAddress;
    }
  }
}
