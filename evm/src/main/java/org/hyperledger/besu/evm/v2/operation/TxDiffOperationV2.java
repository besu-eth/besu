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
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readBytes32At;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedIntAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedLongAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setBytes;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.AssertionResult;
import org.hyperledger.besu.evm.operation.TxDiffOperation;

import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * The EIP-7906 TXDIFF operation (0xb7): one keyed lookup into the transaction's state diff. The
 * pricing and warming ({@link TxDiffOperation#chargeAndWarm}), the state lookups ({@link
 * TxDiffOperation#lookup}) and the views ({@link TxDiffOperation#view}) are shared with the v1
 * operation.
 */
public class TxDiffOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxDiffOperationV2(final GasCalculator gasCalculator) {
    super(TxDiffOperation.OPCODE, "TXDIFF", 3, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final Optional<FrameTransactionContext> maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return new OperationResult(
          gasCalculator().getWarmStorageReadCost(), ExceptionalHaltReason.INVALID_OPERATION);
    }
    if (!frame.stackHasItemsV2(3)) return haltPoppingAll(frame, UNDERFLOW_RESPONSE);
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int param = readClampedIntAt(stack, top, 0);
    final Address address = readAddressAt(stack, top, 1);
    // the same key object SLOAD warms with
    final UInt256 key = UInt256.fromBytes(readBytes32At(stack, top, 2));

    final long cost = TxDiffOperation.chargeAndWarm(frame, gasCalculator(), param, address, key);
    if (frame.getRemainingGas() < cost) {
      return haltPopping(
          frame, 3, new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS));
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!context.isCurrentFramePostTx()) {
      return haltPopping(
          frame, 3, new OperationResult(cost, ExceptionalHaltReason.INVALID_OPERATION));
    }
    final boolean in3IsZero = isZeroAt(stack, top, 2);
    final AssertionResult result =
        TxDiffOperation.isStateLookup(param)
            ? TxDiffOperation.lookup(frame, context, param, address, key, in3IsZero)
            : TxDiffOperation.view(
                context,
                param,
                address,
                readBytes32At(stack, top, 1),
                readClampedLongAt(stack, top, 2),
                in3IsZero);
    if (result.halt() != null) {
      return haltPopping(frame, 3, new OperationResult(cost, result.halt()));
    }
    // the value replaces the deepest operand
    setBytes(result.value(), stack, top - 3);
    frame.setTopV2(top - 2);
    return new OperationResult(cost, null);
  }
}
