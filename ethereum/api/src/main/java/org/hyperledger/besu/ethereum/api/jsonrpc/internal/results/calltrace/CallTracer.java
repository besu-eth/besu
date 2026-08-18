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
 * <p>CALL/CREATE attempts that never spawn a child frame (soft failures such as insufficient
 * balance/max depth, and hard failures such as insufficient gas or stack underflow) are detected in
 * {@link #tracePostExecution} and represented as synthetic leaf nodes, mirroring geth's callTracer.
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

  private static final long WARM_ACCESS_GAS = 100L;
  private static final long GAS_CALL_STIPEND_DIVISOR = 64L;

  private final boolean onlyTopCall;
  private final Deque<Node> stack = new ArrayDeque<>();

  private Transaction transaction;
  private CallTracerResult.Builder rootBuilder;

  // Captured in tracePreExecution for the CALL/CREATE/SELFDESTRUCT opcode about to run on the
  // *current* frame; consumed immediately afterwards by traceContextEnter (real entry),
  // tracePostExecution (soft/hard failure to enter, or self-destruct), or tracePrecompileCall.
  private String pendingType;
  private boolean pendingValid;
  private Address pendingTo;
  private String pendingValueHex;
  private long pendingInOffset;
  private long pendingInLength;
  private long pendingPreOpGas;
  private Address pendingBeneficiary;

  /**
   * Instantiates a new Call tracer.
   *
   * @param onlyTopCall when true, only the top-level call is reported and nested calls are never
   *     built, per the execution-apis callTracer {@code onlyTopCall} option
   */
  public CallTracer(final boolean onlyTopCall) {
    this.onlyTopCall = onlyTopCall;
  }

  @Override
  public boolean isExtendedTracing() {
    return false;
  }

  @Override
  public void traceStartTransaction(final WorldView worldView, final Transaction transaction) {
    this.transaction = transaction;
    this.rootBuilder = null;
    this.pendingType = null;
    this.stack.clear();
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
      case CALL, CALLCODE, DELEGATECALL, STATICCALL -> capturePendingCall(frame, op.getName());
      case CREATE, CREATE2 -> capturePendingCreate(frame, op.getName());
      case SELFDESTRUCT -> capturePendingSelfDestruct(frame);
      default -> {}
    }
  }

  @Override
  public void tracePostExecution(final MessageFrame frame, final Operation.OperationResult result) {
    if (onlyTopCall) {
      return;
    }
    final Operation op = frame.getCurrentOperation();
    if (op == null) {
      return;
    }
    switch (op.getName()) {
      case CALL, CALLCODE, DELEGATECALL, STATICCALL, CREATE, CREATE2 ->
          handleCallOrCreatePostExecution(frame, result, op.getName());
      case SELFDESTRUCT -> handleSelfDestructPostExecution(frame, result);
      default -> {}
    }
  }

  @Override
  public void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {
    if (onlyTopCall || stack.isEmpty()) {
      return;
    }
    final Node node = stack.peek();
    node.isPrecompile = true;
    if (stack.size() > 1) {
      // Besu never spawns a real child frame for a precompile, so there is no genuine gas
      // allocation to report; approximate it the way geth does. Not applicable when the
      // transaction itself targets a precompile directly - there is no enclosing CALL opcode
      // budget to approximate from, so the root keeps its tx.gasLimit "gas" field.
      node.builder.gas(calculatePrecompileGas(pendingPreOpGas));
    }
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
    final boolean isRoot = stack.isEmpty();
    final CallTracerResult.Builder b = CallTracerResult.builder();

    final String type;
    if (isRoot) {
      type = transaction.isContractCreation() ? CREATE : CALL;
      b.gas(transaction.getGasLimit());
    } else {
      type =
          pendingType != null
              ? pendingType
              : (frame.getType() == MessageFrame.Type.CONTRACT_CREATION ? CREATE : CALL);
      b.gas(frame.getRemainingGas());
    }
    b.type(type);
    b.from(
        isRoot
            ? frame.getSenderAddress().getBytes().toHexString()
            : stack.peek().ownAddress.getBytes().toHexString());

    final boolean isCreate = isCreateType(type);
    if (!isCreate) {
      b.to(frame.getContractAddress().getBytes().toHexString());
      if (STATICCALL.equals(type)) {
        // value intentionally omitted (null) for STATICCALL
      } else if (DELEGATECALL.equals(type)) {
        b.value(frame.getApparentValue().toShortHexString());
      } else {
        b.value(frame.getValue().toShortHexString());
      }
      b.input(frame.getInputData().toHexString());
    } else {
      b.value(frame.getValue().toShortHexString());
      b.input(frame.getCode().getBytes().toHexString());
    }

    pendingType = null;
    stack.push(new Node(b, frame.getRemainingGas(), frame.getRecipientAddress()));
  }

  @Override
  public void traceContextExit(final MessageFrame frame) {
    if (onlyTopCall && frame.getDepth() != 0) {
      return;
    }
    final Node node = stack.pop();
    finalizeNode(node, frame);
    if (stack.isEmpty()) {
      rootBuilder = node.builder;
    } else {
      stack.peek().builder.addCall(node.builder.build());
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
      if (result.getExceptionalHaltReason().isPresent()) {
        // Whole-transaction exceptional halts happen before any nested call tree can be
        // trusted; only the root's own summary/error is reported, matching the legacy
        // converter behaviour.
        rootBuilder.calls(null);
      }
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
    final CallTracerResult.Builder b = node.builder;
    final Bytes output = frame.getOutputData();
    if (output != null && !output.isEmpty()) {
      b.output(output.toHexString());
    }
    if (node.isPrecompile) {
      return;
    }

    final Optional<ExceptionalHaltReason> halt = frame.getExceptionalHaltReason();
    if (halt.isPresent()) {
      b.error(halt.get().getDescription());
      frame.getRevertReason().ifPresent(b::revertReason);
    } else if (frame.getState() == MessageFrame.State.COMPLETED_FAILED) {
      // Completed-failed without an exceptional halt reason only happens via REVERT.
      b.error(EXECUTION_REVERTED);
      Bytes revertBytes = frame.getRevertReason().orElse(null);
      if ((revertBytes == null || revertBytes.isEmpty()) && output != null && !output.isEmpty()) {
        revertBytes = output;
      }
      if (revertBytes != null && !revertBytes.isEmpty()) {
        if (output == null || output.isEmpty()) {
          b.output(revertBytes.toHexString());
        }
        JsonRpcErrorResponse.decodeRevertReason(revertBytes).ifPresent(b::revertReasonDecoded);
      }
    } else if (isCreateType(b.getType())) {
      b.to(frame.getContractAddress().getBytes().toHexString());
    }
    b.gasUsed(Math.max(0L, node.entryGas - frame.getRemainingGas()));
  }

  private void handleCallOrCreatePostExecution(
      final MessageFrame frame, final Operation.OperationResult result, final String opcode) {
    if (frame.getState() == MessageFrame.State.CODE_SUSPENDED) {
      // Entered a real child frame; traceContextEnter/traceContextExit handle it.
      return;
    }
    final boolean isCreate = isCreateType(opcode);
    final CallTracerResult.Builder cb = CallTracerResult.builder().type(opcode);
    cb.from(stack.isEmpty() ? null : stack.peek().ownAddress.getBytes().toHexString());
    if (!isCreate) {
      cb.to(pendingValid && pendingTo != null ? pendingTo.getBytes().toHexString() : null);
    }
    if (pendingValueHex != null) {
      cb.value(pendingValueHex);
    }
    if (pendingValid) {
      // Clamp to already-expanded memory: offset/length are unclamped stack values, and the
      // call/create failed before memory was expanded to fit them.
      final long available = frame.memoryByteSize();
      final long len =
          pendingInOffset >= available
              ? 0L
              : Math.min(Math.max(0L, pendingInLength), available - pendingInOffset);
      final Bytes data = len == 0 ? Bytes.EMPTY : frame.readMemory(pendingInOffset, len);
      cb.input(data.toHexString());
    } else {
      cb.input(frame.getInputData().toHexString());
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
    if (!stack.isEmpty()) {
      stack.peek().builder.addCall(cb.build());
    }
  }

  private void handleSelfDestructPostExecution(
      final MessageFrame frame, final Operation.OperationResult result) {
    if (result.getHaltReason() != null || !pendingValid || stack.isEmpty()) {
      return;
    }
    final Address beneficiary = pendingBeneficiary;
    final Address from = frame.getRecipientAddress();
    final Wei value = frame.getRefunds().getOrDefault(beneficiary, Wei.ZERO);
    final CallTracerResult selfDestructCall =
        CallTracerResult.builder()
            .type(SELFDESTRUCT)
            .from(from.getBytes().toHexString())
            .to(beneficiary.getBytes().toHexString())
            .gas(0L)
            .gasUsed(0L)
            .value(value.toShortHexString())
            .input("0x")
            .build();
    stack.peek().builder.addCall(selfDestructCall);
  }

  // ------------------------------------------------------------------------------------------
  // Pre-execution snapshotting (stack is about to be consumed by the operation itself)
  // ------------------------------------------------------------------------------------------

  private void capturePendingCall(final MessageFrame frame, final String opcode) {
    pendingType = opcode;
    pendingPreOpGas = frame.getRemainingGas();
    final boolean hasValue = CALL.equals(opcode) || CALLCODE.equals(opcode);
    final int required = hasValue ? 7 : 6;
    pendingValueHex =
        STATICCALL.equals(opcode)
            ? null
            : DELEGATECALL.equals(opcode)
                ? frame.getApparentValue().toShortHexString()
                : ZERO_VALUE;
    pendingTo = null;
    pendingInOffset = 0L;
    pendingInLength = 0L;
    pendingValid = frame.stackSize() >= required;
    if (!pendingValid) {
      return;
    }
    pendingTo = Words.toAddress(frame.getStackItem(1));
    if (hasValue) {
      pendingValueHex = Wei.wrap(frame.getStackItem(2)).toShortHexString();
    }
    final int offsetIdx = hasValue ? 3 : 2;
    final int lengthIdx = hasValue ? 4 : 3;
    pendingInOffset = Words.clampedToLong(frame.getStackItem(offsetIdx));
    pendingInLength = Words.clampedToLong(frame.getStackItem(lengthIdx));
  }

  private void capturePendingCreate(final MessageFrame frame, final String opcode) {
    pendingType = opcode;
    pendingValueHex = ZERO_VALUE;
    pendingTo = null;
    pendingInOffset = 0L;
    pendingInLength = 0L;
    pendingValid = frame.stackSize() >= 3;
    if (!pendingValid) {
      return;
    }
    pendingValueHex = Wei.wrap(frame.getStackItem(0)).toShortHexString();
    pendingInOffset = Words.clampedToLong(frame.getStackItem(1));
    pendingInLength = Words.clampedToLong(frame.getStackItem(2));
  }

  private void capturePendingSelfDestruct(final MessageFrame frame) {
    pendingValid = frame.stackSize() >= 1;
    pendingBeneficiary = pendingValid ? Words.toAddress(frame.getStackItem(0)) : null;
  }

  // ------------------------------------------------------------------------------------------
  // Gas helpers (Geth-style precompile gas cap; Besu does not spawn a child frame for
  // precompiles' gas accounting, so the forwarded amount is approximated the same way geth
  // reports it: warm-access cost subtracted, then the standard 63/64 EIP-150 rule).
  // ------------------------------------------------------------------------------------------

  static long calculatePrecompileGas(final long preOpGas) {
    final long post = Math.max(0L, preOpGas);
    final long base = post > WARM_ACCESS_GAS ? post - WARM_ACCESS_GAS : 0L;
    return base - (base / GAS_CALL_STIPEND_DIVISOR);
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
