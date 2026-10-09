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

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;

/**
 * Leaves the stack of an operation that halts as v1 leaves it. The v1 operations pop their
 * arguments one at a time, so a stack underflow pops everything there is, and many pop their
 * arguments before checking gas or the static flag. EVM v2 operations check before they touch the
 * stack. Tracers show the stack after the halting operation, so EVM v2 pops what v1 pops.
 */
public final class HaltedStackV2 {

  /** The operations whose v1 version empties the stack on an underflow. */
  private static final boolean[] EMPTIES_ON_UNDERFLOW = new boolean[256];

  /**
   * The arguments the v1 operations have popped when they halt for gas, a state change in a static
   * frame or a read beyond the return data.
   */
  private static final int[] POPPED_BEFORE_HALT = new int[256];

  static {
    for (final int opcode :
        new int[] {
          0x01,
          0x02,
          0x03,
          0x04,
          0x05,
          0x06,
          0x07,
          0x08,
          0x09,
          0x0a,
          0x0b, // ADD to SIGNEXTEND
          0x10,
          0x11,
          0x12,
          0x13,
          0x14,
          0x16,
          0x17,
          0x18,
          0x1a,
          0x1b,
          0x1c,
          0x1d, // LT to SAR
          0x20, // KECCAK256
          0x37,
          0x39,
          0x3c,
          0x3e, // CALLDATACOPY, CODECOPY, EXTCODECOPY, RETURNDATACOPY
          0x52,
          0x53,
          0x55,
          0x57,
          0x5d,
          0x5e, // MSTORE, MSTORE8, SSTORE, JUMPI, TSTORE, MCOPY
          0xa0,
          0xa1,
          0xa2,
          0xa3,
          0xa4, // LOG0 to LOG4
          0xf3,
          0xfd // RETURN, REVERT
        }) {
      EMPTIES_ON_UNDERFLOW[opcode] = true;
    }
    POPPED_BEFORE_HALT[0x0a] = 2; // EXP
    POPPED_BEFORE_HALT[0x20] = 2; // KECCAK256
    POPPED_BEFORE_HALT[0x31] = 1; // BALANCE
    POPPED_BEFORE_HALT[0x37] = 3; // CALLDATACOPY
    POPPED_BEFORE_HALT[0x39] = 3; // CODECOPY
    POPPED_BEFORE_HALT[0x3b] = 1; // EXTCODESIZE
    POPPED_BEFORE_HALT[0x3c] = 4; // EXTCODECOPY
    POPPED_BEFORE_HALT[0x3e] = 3; // RETURNDATACOPY
    POPPED_BEFORE_HALT[0x3f] = 1; // EXTCODEHASH
    POPPED_BEFORE_HALT[0x51] = 1; // MLOAD
    POPPED_BEFORE_HALT[0x52] = 2; // MSTORE
    POPPED_BEFORE_HALT[0x53] = 2; // MSTORE8
    POPPED_BEFORE_HALT[0x54] = 1; // SLOAD
    POPPED_BEFORE_HALT[0x5c] = 1; // TLOAD
    POPPED_BEFORE_HALT[0x5d] = 2; // TSTORE
    POPPED_BEFORE_HALT[0x5e] = 3; // MCOPY
    for (int opcode = 0xa0; opcode <= 0xa4; opcode++) {
      POPPED_BEFORE_HALT[opcode] = 2; // LOG0 to LOG4 pop the memory range before the topics
    }
    POPPED_BEFORE_HALT[0xf3] = 2; // RETURN
    POPPED_BEFORE_HALT[0xfd] = 2; // REVERT
  }

  private HaltedStackV2() {}

  /**
   * Pops what the v1 version of the operation has popped when it halts for the reason. Undefined
   * opcodes and overflows leave the stack as it is, on both.
   *
   * @param frame the frame the operation halted in
   * @param opcode the operation's opcode
   * @param haltReason why it halted
   */
  public static void popAsV1(
      final MessageFrame frame, final int opcode, final ExceptionalHaltReason haltReason) {
    final int top = frame.stackTopV2();
    if (haltReason == ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS) {
      if (EMPTIES_ON_UNDERFLOW[opcode]) {
        frame.setTopV2(0);
      }
    } else if (haltReason == ExceptionalHaltReason.INSUFFICIENT_GAS
        || haltReason == ExceptionalHaltReason.ILLEGAL_STATE_CHANGE
        || haltReason == ExceptionalHaltReason.INVALID_RETURN_DATA_BUFFER_ACCESS
        || haltReason == ExceptionalHaltReason.OUT_OF_BOUNDS) {
      frame.setTopV2(Math.max(0, top - POPPED_BEFORE_HALT[opcode]));
    }
  }
}
