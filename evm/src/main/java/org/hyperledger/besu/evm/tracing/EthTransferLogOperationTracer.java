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
 * Tracer that emits logs for all transfers that occur during the execution of a transaction.
 *
 * <p>Logs are emitted for all transfers that occur during the execution of a transaction. This
 * includes both value transfers and self-destructs.
 */
public class EthTransferLogOperationTracer implements OperationTracer {
  /** The list of logs emitted by this tracer */
  private final List<Log> traceTransfers = new ArrayList<>();

  /** Snapshot stack tracking traceTransfers size at each frame entry */
  private final Deque<Integer> logSnapshots = new ArrayDeque<>();

  /** The constant address for transfer logs */
  public static final Address SIMULATION_TRANSFER_ADDRESS =
      Address.fromHexString("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

  /** The constant topic for transfer logs */
  public static final Bytes32 SIMULATION_TRANSFER_TOPIC =
      Bytes32.fromHexString("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

  /** Default constructor. */
  public EthTransferLogOperationTracer() {}

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    logSnapshots.push(traceTransfers.size());
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
    // do nothing for now
  }

  @Override
  public void traceContextExit(final MessageFrame frame) {
    final int snapshot = logSnapshots.pop();
    if (frame.getState() == MessageFrame.State.COMPLETED_FAILED) {
      // Remove only logs added during this frame (and its children)
      while (traceTransfers.size() > snapshot) {
        traceTransfers.removeLast();
      }
    }
  }

  private void emitSelfDestructLog(final MessageFrame frame) {
    final Address beneficiaryAddress = Words.toAddress(frame.getStackItem(0));
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
    traceTransfers.add(new Log(SIMULATION_TRANSFER_ADDRESS, value, builder.build()));
  }

  /**
   * Returns the logs emitted by this tracer.
   *
   * @return the logs emitted by this tracer
   */
  public List<Log> getLogs() {
    return traceTransfers;
  }
}
