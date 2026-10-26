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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.isZeroAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedIntAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedLongAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setBytes;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.AssertionResult;
import org.hyperledger.besu.evm.operation.TxTraceOperation;

import java.util.Optional;

/**
 * The EIP-7906 TXTRACE operation (0xb6): reads one value of the transaction's state diff. The value
 * itself is {@link TxTraceOperation#lookup}, shared with the v1 operation.
 */
public class TxTraceOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxTraceOperationV2(final GasCalculator gasCalculator) {
    super(TxTraceOperation.OPCODE, "TXTRACE", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final long cost = gasCalculator().getWarmStorageReadCost();
    final Optional<FrameTransactionContext> maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return new OperationResult(cost, ExceptionalHaltReason.INVALID_OPERATION);
    }
    if (!frame.stackHasItemsV2(2)) return haltPoppingAll(frame, UNDERFLOW_RESPONSE);
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int param = readClampedIntAt(stack, top, 0);
    final long index = readClampedLongAt(stack, top, 1);
    final boolean indexIsZero = isZeroAt(stack, top, 1);

    if (frame.getRemainingGas() < cost) {
      return haltPopping(
          frame, 2, new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS));
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!context.isCurrentFramePostTx()) {
      return haltPopping(
          frame, 2, new OperationResult(cost, ExceptionalHaltReason.INVALID_OPERATION));
    }
    final AssertionResult result = TxTraceOperation.lookup(context, param, index, indexIsZero);
    if (result.halt() != null) {
      return haltPopping(frame, 2, new OperationResult(cost, result.halt()));
    }
    // the value replaces the deeper operand
    setBytes(result.value(), stack, top - 2);
    frame.setTopV2(top - 1);
    return new OperationResult(cost, null);
  }
}
