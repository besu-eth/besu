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

import static org.hyperledger.besu.evm.v2.operation.AbstractOperationV2.UNDERFLOW_RESPONSE;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushLong;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readWeiAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.PayOperation;

/**
 * The Pay operation (EIP-5920) on the v2 long[] stack. The payment itself is the one of {@link
 * PayOperation}.
 */
public class PayOperationV2 extends PayOperation {

  /**
   * Instantiates a new Pay operation.
   *
   * @param gasCalculator the gas calculator
   */
  public PayOperationV2(final GasCalculator gasCalculator) {
    super(gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int toOffset = (top - 1) << 2;
    // an address is 20 bytes, so the 12 most significant bytes of the word must be zero
    if (stack[toOffset] != 0 || (stack[toOffset + 1] >>> 32) != 0) {
      return new OperationResult(0, ExceptionalHaltReason.ADDRESS_OUT_OF_RANGE);
    }
    final Address to = readAddressAt(stack, top, 0);
    if (!frame.stackHasItemsV2(2)) return UNDERFLOW_RESPONSE;
    final Wei value = readWeiAt(stack, top, 1);
    return pay(frame, to, value);
  }

  @Override
  protected void popArgumentsAndPushResult(final MessageFrame frame, final boolean success) {
    final int resultSlot = frame.stackTopV2() - getStackItemsConsumed();
    pushLong(success ? 1L : 0L, frame.stackDataV2(), resultSlot);
    frame.setTopV2(resultSlot + 1);
  }
}
