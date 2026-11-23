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
package org.hyperledger.besu.evm.tracing;

import static org.apache.tuweni.bytes.Bytes32.leftPad;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.operation.Operation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Tracer used by pre-Amsterdam {@code eth_simulateV1} when {@code traceTransfers} is enabled.
 *
 * <p>Records synthetic ETH-transfer logs (value transfers and self-destructs) and contract {@code
 * LOG0}–{@code LOG4} emissions in execution order. Failed frames drop the logs they added, matching
 * geth's simulate log tracer.
 */
public class EthTransferLogOperationTracer implements OperationTracer {
  /** Execution-ordered logs: synthetic transfers and contract LOG emissions */
  private final List<Log> logs = new ArrayList<>();

  /** Snapshot stack tracking {@link #logs} size at each frame entry */
  private final Deque<Integer> logSnapshots = new ArrayDeque<>();

  /** The constant address for transfer logs */
  public static final Address SIMULATION_TRANSFER_ADDRESS =
      Address.fromHexString("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

  /** The constant topic for transfer logs */
  public static final Bytes32 SIMULATION_TRANSFER_TOPIC =
      Bytes32.fromHexString("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

  private static final int LOG0 = 0xA0;
  private static final int LOG4 = 0xA4;

  /** Default constructor. */
  public EthTransferLogOperationTracer() {}

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    logSnapshots.push(logs.size());
    if (frame.getValue().compareTo(Wei.ZERO) > 0
        && !frame.getRecipientAddress().equals(frame.getSenderAddress())) {
      emitTransferLogs(frame.getSenderAddress(), frame.getRecipientAddress(), frame.getValue());
    }
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    // Emit log if self-destruct
    if (frame.getCurrentOperation().getOpcode() == 0xFF) {
      emitSelfDestructLog(frame);
    }
  }

  @Override
  public void tracePostExecution(
      final MessageFrame frame, final Operation.OperationResult operationResult) {
    if (operationResult.getHaltReason() != null) {
      return;
    }
    final int opcode = frame.getCurrentOperation().getOpcode();
    if (opcode >= LOG0 && opcode <= LOG4) {
      final List<Log> frameLogs = frame.getLogs();
      if (!frameLogs.isEmpty()) {
        logs.add(frameLogs.getLast());
      }
    }
  }

  @Override
  public void traceContextExit(final MessageFrame frame) {
    final int snapshot = logSnapshots.pop();
    if (frame.getState() == MessageFrame.State.COMPLETED_FAILED) {
      // Remove only logs added during this frame (and its children)
      while (logs.size() > snapshot) {
        logs.removeLast();
      }
    }
  }

  private void emitSelfDestructLog(final MessageFrame frame) {
    final Address beneficiaryAddress = Words.toAddress(TracedStack.item(frame, 0));
    final Address originatorAddress = frame.getRecipientAddress();
    final MutableAccount originatorAccount = frame.getWorldUpdater().getAccount(originatorAddress);
    final Wei originatorBalance = originatorAccount.getBalance();
    emitTransferLogs(frame.getRecipientAddress(), beneficiaryAddress, originatorBalance);
  }

  private void emitTransferLogs(final Address sender, final Address recipient, final Wei value) {
    final ImmutableList.Builder<LogTopic> builder = ImmutableList.builderWithExpectedSize(3);
    builder.add(LogTopic.create(SIMULATION_TRANSFER_TOPIC));
    builder.add(LogTopic.create(leftPad(sender.getBytes())));
    builder.add(LogTopic.create(leftPad(recipient.getBytes())));
    logs.add(new Log(SIMULATION_TRANSFER_ADDRESS, value, builder.build()));
  }

  /**
   * Returns the logs recorded by this tracer in execution order (synthetic transfers and contract
   * logs).
   *
   * @return the recorded logs
   */
  public List<Log> getLogs() {
    return logs;
  }
}
