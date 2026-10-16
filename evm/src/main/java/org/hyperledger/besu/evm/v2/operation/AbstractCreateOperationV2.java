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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.clampedToLong;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushAddress;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushZero;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readWeiAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.AbstractCreateOperation;

import org.apache.tuweni.bytes.Bytes;

/**
 * A skeleton class for implementing create operations on the v2 long[] stack.
 *
 * <p>The create itself is the one of {@link AbstractCreateOperation}; the v2 operations only read
 * their arguments from, and write their result to, the v2 stack.
 */
public abstract class AbstractCreateOperationV2 extends AbstractCreateOperation {

  /**
   * Instantiates a new Abstract create operation.
   *
   * @param opcode the opcode
   * @param name the name
   * @param stackItemsConsumed the stack items consumed
   * @param stackItemsProduced the stack items produced
   * @param gasCalculator the gas calculator
   */
  protected AbstractCreateOperationV2(
      final int opcode,
      final String name,
      final int stackItemsConsumed,
      final int stackItemsProduced,
      final GasCalculator gasCalculator) {
    super(opcode, name, stackItemsConsumed, stackItemsProduced, gasCalculator);
  }

  @Override
  protected Code getInitCode(final MessageFrame frame, final EVM evm) {
    final Bytes inputData = frame.readMemory(getInputOffset(frame), getInputSize(frame));
    // Never cache CREATEx initcode. The amount of reuse is very low, and caching mostly
    // addresses disk loading delay, and we already have the code.
    return new Code(inputData);
  }

  @Override
  protected Wei value(final MessageFrame frame) {
    return readWeiAt(frame.stackDataV2(), frame.stackTopV2(), 0);
  }

  @Override
  protected long getInputOffset(final MessageFrame frame) {
    return clampedToLong(frame.stackDataV2(), frame.stackTopV2(), 1);
  }

  @Override
  public long getInputSize(final MessageFrame frame) {
    return clampedToLong(frame.stackDataV2(), frame.stackTopV2(), 2);
  }

  @Override
  protected void popArguments(final MessageFrame frame) {
    frame.setTopV2(frame.stackTopV2() - getStackItemsConsumed());
  }

  @Override
  protected void popArgumentsAndPushResult(final MessageFrame frame, final Address createdAddress) {
    final int resultSlot = frame.stackTopV2() - getStackItemsConsumed();
    if (createdAddress == null) {
      pushZero(frame.stackDataV2(), resultSlot);
    } else {
      pushAddress(createdAddress, frame.stackDataV2(), resultSlot);
    }
    frame.setTopV2(resultSlot + 1);
  }
}
