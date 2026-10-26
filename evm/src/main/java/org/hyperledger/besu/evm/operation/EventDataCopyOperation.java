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
package org.hyperledger.besu.evm.operation;

import static org.hyperledger.besu.evm.internal.Words.clampedToLong;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.TransactionDiff;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-7906 EVENTDATACOPY operation (0xb8): copies a range of one event's non-indexed data into
 * memory, with CALLDATACOPY pricing.
 *
 * <p>Stack: {@code eventIndex} on top, then {@code memOffset}, {@code dataOffset} and {@code
 * length}. Unlike CALLDATACOPY, a range reaching past the end of the data is not zero-padded but
 * halts exceptionally, even when the length is zero, as does an event index past the end of the
 * events table, or execution outside a POST_TX frame.
 */
public class EventDataCopyOperation extends AbstractOperation {

  /** The EVENTDATACOPY opcode. */
  public static final int OPCODE = 0xb8;

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public EventDataCopyOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "EVENTDATACOPY", 4, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final Optional<FrameTransactionContext> maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      // Not a frame transaction.
      return new OperationResult(
          gasCalculator().getVeryLowTierGasCost(), ExceptionalHaltReason.INVALID_OPERATION);
    }
    final long eventIndex = clampedToLong(frame.popStackItem());
    final long memOffset = clampedToLong(frame.popStackItem());
    final long dataOffset = clampedToLong(frame.popStackItem());
    final long length = clampedToLong(frame.popStackItem());

    final long cost = gasCalculator().dataCopyOperationGasCost(frame, memOffset, length);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!context.isCurrentFramePostTx()) {
      return new OperationResult(cost, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final AssertionResult result = eventData(context, eventIndex, dataOffset, length);
    if (result.halt() != null) {
      return new OperationResult(cost, result.halt());
    }
    if (length > 0) {
      frame.writeMemory(memOffset, dataOffset, length, result.value(), true);
    }
    return new OperationResult(cost, null);
  }

  /**
   * Selects the data an EVENTDATACOPY copies from, after the operation charged its gas and checked
   * that a POST_TX frame executes.
   *
   * <p>The range check cannot wrap: with clamped operands, an offset of 2^63 or more always lies
   * past the end of the data.
   *
   * @param context the frame transaction context
   * @param eventIndex the event index, clamped to {@link Long#MAX_VALUE}
   * @param dataOffset the offset into the event's data, clamped to {@link Long#MAX_VALUE}
   * @param length the number of bytes to copy, clamped to {@link Long#MAX_VALUE}
   * @return the event's whole data, which holds the range, or the halt
   */
  public static AssertionResult eventData(
      final FrameTransactionContext context,
      final long eventIndex,
      final long dataOffset,
      final long length) {
    final Optional<TransactionDiff> maybeDiff = context.transactionDiff();
    if (maybeDiff.isEmpty()) {
      return AssertionResult.INVALID;
    }
    final TransactionDiff diff = maybeDiff.get();
    if (eventIndex >= diff.events().size()) {
      return AssertionResult.OUT_OF_BOUNDS;
    }
    final Bytes data = diff.events().get((int) eventIndex).data();
    if (dataOffset > data.size() || length > data.size() - dataOffset) {
      return AssertionResult.OUT_OF_BOUNDS;
    }
    return AssertionResult.of(data);
  }
}
