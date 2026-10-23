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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedIntAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setBytes32;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setZero;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.FrameDataLoadOperation;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes32;

/**
 * The EIP-8141 FRAMEDATALOAD operation (0xb1): loads one word of a chosen frame's data, with
 * CALLDATALOAD semantics.
 */
public class FrameDataLoadOperationV2 extends AbstractOperationV2 {

  private static final long GAS_COST = 3L;
  private static final OperationResult SUCCESS = new OperationResult(GAS_COST, null);
  private static final OperationResult INVALID =
      new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
  private static final OperationResult OUT_OF_BOUNDS =
      new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public FrameDataLoadOperationV2(final GasCalculator gasCalculator) {
    super(FrameDataLoadOperation.OPCODE, "FRAMEDATALOAD", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return INVALID;
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!frame.stackHasItemsV2(2)) return haltPoppingAll(frame, UNDERFLOW_RESPONSE);
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = readClampedIntAt(stack, top, 0);
    final int frameIndex = readClampedIntAt(stack, top, 1);

    if (frameIndex >= context.frames().size()) {
      return haltPopping(frame, 2, OUT_OF_BOUNDS);
    }
    final Bytes data = context.frames().get(frameIndex).data();
    frame.setTopV2(top - 1);
    if (offset >= data.size()) {
      setZero(stack, top - 2);
      return SUCCESS;
    }
    final MutableBytes32 word = MutableBytes32.create();
    data.slice(offset, Math.min(Bytes32.SIZE, data.size() - offset)).copyTo(word, 0);
    setBytes32(word, stack, top - 2);
    return SUCCESS;
  }
}
