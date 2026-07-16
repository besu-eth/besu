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
package org.hyperledger.besu.evm.tracing;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.AbstractCallOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import com.google.common.base.Joiner;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * The Streaming operation tracer. This tracer streams traces in JSON format to the chosen output
 * stream
 */
public class StreamingOperationTracer implements OperationTracer {

  private static final Joiner commaJoiner = Joiner.on(',');
  private final PrintWriter out;
  private final OpCodeTracerConfig opCodeTracerConfig;
  private int pc;
  private List<String> stack;
  private long gas;
  private Bytes memory;
  private int memorySize;
  private int depth;
  private String storageString;

  // Flags used for implementing traceOpcodes functionality
  private boolean traceOpcode;
  private Operation previousOpcode = null;

  /**
   * Instantiates a new StreamingOperationTracer
   *
   * @param out the PrintStream to stream JSON traces to
   * @param opCodeTracerConfig configuration for tracing
   */
  public StreamingOperationTracer(
      final PrintStream out, final OpCodeTracerConfig opCodeTracerConfig) {
    this(new PrintWriter(out, true, StandardCharsets.UTF_8), opCodeTracerConfig);
  }

  /**
   * Instantiates a new StreamingOperationTracer
   *
   * @param out the PrintWriter to stream JSON traces to
   * @param opCodeTracerConfig configuration for tracing
   */
  public StreamingOperationTracer(
      final PrintWriter out, final OpCodeTracerConfig opCodeTracerConfig) {
    this.out = out;
    this.opCodeTracerConfig = opCodeTracerConfig;
  }

  /**
   * Short as hex string.
   *
   * @param number the number
   * @return the string
   */
  public static String shortNumber(final UInt256 number) {
    return number.isZero() ? "0x0" : number.toShortHexString();
  }

  /**
   * Long number as hex string.
   *
   * @param number the number
   * @return the string
   */
  public static String shortNumber(final long number) {
    return "0x" + Long.toHexString(number);
  }

  private static String shortBytes(final Bytes bytes) {
    return bytes.isZero() ? "0x0" : bytes.toShortHexString();
  }

  @Override
  public void tracePreExecution(final MessageFrame messageFrame) {
    final Operation currentOp = messageFrame.getCurrentOperation();
    if (!(traceOpcode = traceOpcode(currentOp))) {
      return;
    }
    stack = new ArrayList<>(messageFrame.stackSize());
    for (int i = messageFrame.stackSize() - 1; i >= 0; i--) {
      stack.add("\"" + shortBytes(messageFrame.getStackItem(i)) + "\"");
    }
    pc = messageFrame.getPC();
    gas = messageFrame.getRemainingGas();
    memorySize = messageFrame.memoryWordSize() * 32;
    if (opCodeTracerConfig.traceMemory() && memorySize > 0) {
      memory = messageFrame.readMemory(0, messageFrame.memoryWordSize() * 32L);
    } else {
      memory = null;
    }
    depth = messageFrame.getMessageStackSize();

    StringBuilder sb = new StringBuilder();
    if (opCodeTracerConfig.traceStorage()) {
      var updater = messageFrame.getWorldUpdater();
      var account = updater.getAccount(messageFrame.getRecipientAddress());
      if (account != null && !account.getUpdatedStorage().isEmpty()) {
        boolean[] shownEntry = {false};
        sb.append(",\"storage\":{");
        account
            .getUpdatedStorage()
            .forEach(
                (k, v) -> {
                  if (shownEntry[0]) {
                    sb.append(",");
                  } else {
                    shownEntry[0] = true;
                  }
                  sb.append("\"")
                      .append(k.toQuantityHexString())
                      .append("\":\"")
                      .append(v.toQuantityHexString())
                      .append("\"");
                });
        sb.append("}");
      }
    }
    storageString = sb.toString();
  }

  private boolean traceOpcode(final Operation currentOpcode) {
    if (opCodeTracerConfig.traceOpcodes().isEmpty()) {
      return true;
    }
    final boolean traceCurrentOpcode =
        opCodeTracerConfig
            .traceOpcodes()
            .contains(currentOpcode.getName().toLowerCase(Locale.ROOT));
    final boolean tracePreviousOpcode =
        previousOpcode != null
            && opCodeTracerConfig
                .traceOpcodes()
                .contains(previousOpcode.getName().toLowerCase(Locale.ROOT));

    if (!traceCurrentOpcode && !tracePreviousOpcode) {
      return false;
    }
    previousOpcode = currentOpcode;
    return true;
  }

  @Override
  public void tracePostExecution(
      final MessageFrame messageFrame, final Operation.OperationResult executeResult) {
    final Operation currentOp = messageFrame.getCurrentOperation();
    if (!traceOpcode) {
      return;
    }
    if (currentOp.isVirtualOperation()) {
      return;
    }
    final int opcode = currentOp.getOpcode();
    final Bytes returnData = messageFrame.getReturnData();
    long thisGasCost = executeResult.getGasCost();
    if (currentOp instanceof AbstractCallOperation) {
      thisGasCost += messageFrame.getMessageFrameStack().getFirst().getRemainingGas();
    }

    final StringBuilder sb = new StringBuilder(1024);
    sb.append("{");
    sb.append("\"pc\":").append(pc).append(",");
    if (opCodeTracerConfig.eip3155Strict()) {
      sb.append("\"op\":").append(opcode).append(",");
    } else {
      sb.append("\"op\":\"").append(fastHexByte(opcode)).append("\",");
    }
    if (opCodeTracerConfig.eip3155Strict()) {
      sb.append("\"gas\":\"").append(shortNumber(gas)).append("\",");
      sb.append("\"gasCost\":\"").append(shortNumber(thisGasCost)).append("\",");
    } else {
      sb.append("\"gas\":").append(Long.toUnsignedString(gas)).append(",");
      sb.append("\"gasCost\":").append(Long.toUnsignedString(thisGasCost)).append(",");
    }
    if (memory != null) {
      sb.append("\"memory\":\"").append(memory.toHexString()).append("\",");
    }
    sb.append("\"memSize\":").append(memorySize).append(",");
    if (opCodeTracerConfig.traceStack()) {
      sb.append("\"stack\":[").append(commaJoiner.join(stack)).append("],");
    }
    if (opCodeTracerConfig.traceReturnData() && !returnData.isEmpty()) {
      sb.append("\"returnData\":\"").append(returnData.toHexString()).append("\",");
    }
    sb.append("\"depth\":").append(depth).append(",");
    sb.append("\"refund\":").append(messageFrame.getGasRefund()).append(",");
    sb.append("\"opName\":\"").append(currentOp.getName()).append("\"");
    if (executeResult.getHaltReason() != null) {
      sb.append(",\"error\":\"")
          .append(executeResult.getHaltReason().getDescription())
          .append("\"");
    } else if (messageFrame.getRevertReason().isPresent()) {
      sb.append(",\"error\":\"")
          .append(messageFrame.getRevertReason().get().toHexString())
          .append("\"");
    }

    sb.append(storageString).append("}");
    out.println(sb);
  }

  @Override
  public void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {
    // precompile calls are not part of the standard trace
  }

  @Override
  public void traceAccountCreationResult(
      final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {
    // precompile calls are not part of the standard trace
  }

  private String fastHexByte(final int b) {
    char[] result = new char[] {'0', 'x', '-', '-'};
    result[2] = "0123456789abcdef".charAt(b >> 4 & 15);
    result[3] = "0123456789abcdef".charAt(b & 15);

    return new String(result);
  }
}
