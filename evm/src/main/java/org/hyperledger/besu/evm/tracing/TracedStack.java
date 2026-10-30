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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.UnderflowException;

import org.apache.tuweni.bytes.Bytes;

/**
 * Reads the operand stack of a frame for tracers, from whichever stack the interpreter running the
 * frame uses. {@link MessageFrame#getStackItem} and {@link MessageFrame#stackSize} only read the
 * EVM v1 stack, and throw a {@link NullPointerException} on a frame EVM v2 runs, which has none.
 */
public final class TracedStack {

  private TracedStack() {}

  /**
   * Returns the number of items on the stack of the frame.
   *
   * @param frame the frame
   * @return the stack size
   */
  public static int size(final MessageFrame frame) {
    return frame.runsOnEvmV2() ? frame.stackTopV2() : frame.stackSize();
  }

  /**
   * Returns the item at the specified offset in the stack of the frame.
   *
   * @param frame the frame
   * @param offset the item's position relative to the top of the stack
   * @return the item at the specified offset in the stack
   * @throws UnderflowException if the offset is out of range
   */
  public static Bytes item(final MessageFrame frame, final int offset) {
    return frame.runsOnEvmV2() ? frame.stackItemV2(offset) : frame.getStackItem(offset);
  }
}
