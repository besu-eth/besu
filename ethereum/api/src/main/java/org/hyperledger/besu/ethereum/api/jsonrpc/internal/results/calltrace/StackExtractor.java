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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.CallTracerHelper.bytesToInt;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace.CallTracerHelper.extractCallDataFromMemory;
import static org.hyperledger.besu.evm.internal.Words.toAddress;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts data from EVM stack and memory for call tracing.
 *
 * <p>This class encapsulates the logic for extracting various pieces of information from the EVM
 * stack during call operations. It handles the different stack layouts for CALL, CREATE, CREATE2,
 * and SELFDESTRUCT operations.
 *
 * <p>Stack layouts handled:
 *
 * <ul>
 *   <li>CALL/CALLCODE: gas, to, value, inOffset, inSize, outOffset, outSize
 *   <li>DELEGATECALL/STATICCALL: gas, to, inOffset, inSize, outOffset, outSize
 *   <li>CREATE: value, offset, size
 *   <li>CREATE2: value, offset, size, salt
 *   <li>SELFDESTRUCT: beneficiary
 * </ul>
 */
public final class StackExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(StackExtractor.class);

  // Stack indices, counted from the top as the EVM indexes the operand stack.
  // CALL/CALLCODE:            gas, to, value, inOffset, inSize, outOffset, outSize
  // DELEGATECALL/STATICCALL:  gas, to,        inOffset, inSize, outOffset, outSize
  private static final int CALL_TO_INDEX = 1;
  private static final int CALL_VALUE_INDEX = 2;
  private static final int CALL_IN_OFFSET_INDEX = 3;
  private static final int DELEGATECALL_IN_OFFSET_INDEX = 2;

  // CREATE: value, offset, size          CREATE2: value, offset, size, salt
  private static final int CREATE_VALUE_INDEX = 0;
  private static final int CREATE_OFFSET_INDEX = 1;
  private static final int CREATE_SIZE_INDEX = 2;

  private static final String ZERO_VALUE = "0x0";

  private StackExtractor() {
    // Utility class - prevent instantiation
  }

  /**
   * Extracts the value (wei) being transferred in a CALL or CALLCODE operation.
   *
   * @param frame the trace frame containing the stack
   * @return the value as a hex string, or "0x0" if not available
   */
  public static String extractCallValue(final TraceFrame frame) {
    return frame
        .getStack()
        .filter(stack -> stack.length > CALL_VALUE_INDEX)
        .map(stack -> Wei.wrap(stackItem(stack, CALL_VALUE_INDEX)).toShortHexString())
        .orElse(ZERO_VALUE);
  }

  /**
   * Extracts the value (wei) being transferred in a CREATE or CREATE2 operation.
   *
   * <p>Stack layout for CREATE (from top): value, offset, size
   *
   * @param frame the trace frame containing the stack
   * @return the value as a hex string, or "0x0" if not available
   */
  public static String extractCreateValue(final TraceFrame frame) {
    return frame
        .getStack()
        .filter(stack -> stack.length > CREATE_SIZE_INDEX)
        .map(stack -> Wei.wrap(stackItem(stack, CREATE_VALUE_INDEX)).toShortHexString())
        .orElse(ZERO_VALUE);
  }

  /**
   * Extracts the target address from a CALL operation's stack.
   *
   * @param frame the trace frame containing the stack
   * @return the target address as a hex string, or null if not available
   */
  public static String extractCallToAddress(final TraceFrame frame) {
    return frame
        .getStack()
        .filter(stack -> stack.length > CALL_TO_INDEX && stackItem(stack, CALL_TO_INDEX) != null)
        .map(stack -> toAddress(stackItem(stack, CALL_TO_INDEX)).getBytes().toHexString())
        .orElse(null);
  }

  /**
   * Extracts the input data for a CALL operation.
   *
   * @param frame the trace frame containing stack and memory
   * @param opcode the call opcode, which fixes where the arguments sit on the stack
   * @return the input data bytes, or the frame's input data as fallback
   */
  public static Bytes extractCallInputFromMemory(final TraceFrame frame, final String opcode) {
    final Optional<Bytes> callInputData = frame.getCallInputData();
    if (callInputData.isPresent()) {
      return callInputData.get();
    }

    final int offsetIndex =
        OpcodeCategory.hasValueArgument(opcode)
            ? CALL_IN_OFFSET_INDEX
            : DELEGATECALL_IN_OFFSET_INDEX;

    return frame
        .getStack()
        .filter(stack -> stack.length > offsetIndex + 1)
        .map(
            stack -> {
              final int inOffset = bytesToInt(stackItem(stack, offsetIndex));
              final int inSize = bytesToInt(stackItem(stack, offsetIndex + 1));
              return extractFromMemory(frame, inOffset, inSize);
            })
        .orElse(frame.getInputData());
  }

  private static Bytes stackItem(final Bytes[] stack, final int indexFromTop) {
    return stack[stack.length - 1 - indexFromTop];
  }

  /**
   * Extracts the initialization code for a CREATE or CREATE2 operation.
   *
   * @param frame the trace frame containing stack and memory
   * @param entered whether the CREATE operation successfully entered (depth increased)
   * @return the initialization code bytes, or empty if extraction fails
   */
  public static Bytes extractCreateInitCode(final TraceFrame frame, final boolean entered) {
    // Only try getMaybeCode() if the CREATE entered successfully
    // When CREATE doesn't enter (soft failure), getMaybeCode() contains the parent's code, not the
    // init code
    if (entered && frame.getMaybeCode().isPresent()) {
      return frame.getMaybeCode().get().getBytes();
    }

    final Optional<Bytes> callInputData = frame.getCallInputData();
    if (callInputData.isPresent()) {
      return callInputData.get();
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Using memory extraction for CREATE input data at depth {} (entered: {})",
          frame.getDepth(),
          entered);
    }

    return frame
        .getStack()
        .map(stack -> extractCreateInitCodeFromStack(frame, stack))
        .orElse(Bytes.EMPTY);
  }

  /**
   * Extracts the beneficiary address from a SELFDESTRUCT operation's stack.
   *
   * @param frame the trace frame containing the stack
   * @return the beneficiary address, or empty if not available
   */
  public static Optional<Address> extractSelfDestructBeneficiary(final TraceFrame frame) {
    return frame
        .getStack()
        .filter(stack -> stack.length > 0)
        .map(stack -> toAddress(stack[stack.length - 1]));
  }

  /**
   * Extracts the value being transferred in a SELFDESTRUCT operation.
   *
   * <p>The value is determined by looking at the refunds map for the beneficiary address.
   *
   * @param frame the trace frame containing refund information
   * @param beneficiary the address receiving the funds
   * @return the value as a hex string, or "0x0" if not available
   */
  public static String extractSelfDestructValue(final TraceFrame frame, final Address beneficiary) {
    return frame
        .getMaybeRefunds()
        .map(refunds -> refunds.get(beneficiary))
        .map(Wei::toShortHexString)
        .orElse(ZERO_VALUE);
  }

  /**
   * Resolves the input data for a call operation, preferring the callee's input data.
   *
   * @param frame the current trace frame
   * @param nextTrace the next trace frame (callee's first frame), may be null
   * @param opcode the opcode of the operation
   * @return the resolved input data
   */
  public static Bytes resolveCallInputData(
      final TraceFrame frame, final TraceFrame nextTrace, final String opcode) {

    if (OpcodeCategory.isCreateOp(opcode)) {
      // Check if the CREATE entered (depth increased)
      boolean entered = nextTrace != null && nextTrace.getDepth() > frame.getDepth();
      return extractCreateInitCode(frame, entered);
    }

    // Prefer callee frame's input data for calls
    if (nextTrace != null && nextTrace.getDepth() == frame.getDepth() + 1) {
      return nextTrace.getInputData() != null ? nextTrace.getInputData() : Bytes.EMPTY;
    }

    // Fallback to memory extraction for calls
    if (OpcodeCategory.isCallOp(opcode)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Falling back to memory extraction for CALL input data at depth {}", frame.getDepth());
      }
      return extractCallInputFromMemory(frame, opcode);
    }

    return frame.getInputData();
  }

  private static Bytes extractCreateInitCodeFromStack(final TraceFrame frame, final Bytes[] stack) {
    if (stack.length <= CREATE_SIZE_INDEX) {
      return Bytes.EMPTY;
    }

    final int offset = bytesToInt(stackItem(stack, CREATE_OFFSET_INDEX));
    final int length = bytesToInt(stackItem(stack, CREATE_SIZE_INDEX));

    return extractFromMemory(frame, offset, length);
  }

  private static Bytes extractFromMemory(
      final TraceFrame frame, final int offset, final int length) {
    if (length == 0) {
      return Bytes.EMPTY;
    }

    return frame
        .getMemory()
        .map(memory -> extractCallDataFromMemory(memory, offset, length))
        .orElse(Bytes.EMPTY);
  }
}
