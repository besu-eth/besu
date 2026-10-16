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
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushLong;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.AbstractCallOperation;

/**
 * A skeleton class for implementing call operations on the v2 long[] stack.
 *
 * <p>The call itself is the one of {@link AbstractCallOperation}; the v2 operations only read their
 * arguments from, and write their result to, the v2 stack.
 */
public abstract class AbstractCallOperationV2 extends AbstractCallOperation {

  /**
   * Instantiates a new Abstract call operation.
   *
   * @param opcode the opcode
   * @param name the name
   * @param stackItemsConsumed the stack items consumed
   * @param stackItemsProduced the stack items produced
   * @param gasCalculator the gas calculator
   */
  AbstractCallOperationV2(
      final int opcode,
      final String name,
      final int stackItemsConsumed,
      final int stackItemsProduced,
      final GasCalculator gasCalculator) {
    super(opcode, name, stackItemsConsumed, stackItemsProduced, gasCalculator);
  }

  @Override
  protected long gas(final MessageFrame frame) {
    return clampedToLong(frame.stackDataV2(), frame.stackTopV2(), 0);
  }

  @Override
  protected void popArgumentsAndPushResult(final MessageFrame frame, final boolean success) {
    final int resultSlot = frame.stackTopV2() - getStackItemsConsumed();
    pushLong(success ? 1L : 0L, frame.stackDataV2(), resultSlot);
    frame.setTopV2(resultSlot + 1);
  }
}
