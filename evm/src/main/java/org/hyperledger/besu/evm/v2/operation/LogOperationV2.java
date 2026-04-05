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
package org.hyperledger.besu.evm.v2.operation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import com.google.common.collect.ImmutableList;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * EVM v2 LOG operation (LOG0-LOG4) using long[] stack representation. Parameterized by topic count.
 */
public class LogOperationV2 extends AbstractOperationV2 {

  private final int numTopics;

  /**
   * Instantiates a new Log operation.
   *
   * @param numTopics the num topics (0-4)
   * @param gasCalculator the gas calculator
   */
  public LogOperationV2(final int numTopics, final GasCalculator gasCalculator) {
    super(0xA0 + numTopics, "LOG" + numTopics, numTopics + 2, 0, gasCalculator);
    this.numTopics = numTopics;
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2(), numTopics, gasCalculator());
  }

  /**
   * Performs LOG operation.
   *
   * @param frame the frame
   * @param s the stack data array
   * @param numTopics the number of topics to pop
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame,
      final long[] s,
      final int numTopics,
      final GasCalculator gasCalculator) {
    if (!frame.stackHasItems(2 + numTopics)) {
      return new OperationResult(0, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    final int top = frame.stackTopV2();
    final long dataLocation = StackArithmetic.clampedToLong(s, top, 0);
    final long numBytes = StackArithmetic.clampedToLong(s, top, 1);

    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }

    final long cost = gasCalculator.logOperationGasCost(frame, dataLocation, numBytes, numTopics);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Address address = frame.getRecipientAddress();
    final Bytes data = frame.readMemory(dataLocation, numBytes);

    final ImmutableList.Builder<LogTopic> builder =
        ImmutableList.builderWithExpectedSize(numTopics);
    for (int i = 0; i < numTopics; i++) {
      final byte[] buf = new byte[32];
      StackArithmetic.toBytesAt(s, top, 2 + i, buf);
      builder.add(LogTopic.create(Bytes32.wrap(buf)));
    }
    frame.setTopV2(top - 2 - numTopics);

    frame.addLog(new Log(address, data, builder.build()));
    return new OperationResult(cost, null);
  }
}
