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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Unit tests for {@link DupNOperation}. */
class DupNOperationTest {

  private final DupNOperation operation = new DupNOperation(new PragueGasCalculator());

  @Test
  void testDecodeSingle_validLowRange() {
    // n = (x + 145) % 256
    // x = 0 -> n = 145
    assertThat(DupNOperation.decodeSingle(0)).isEqualTo(145);
    // x = 90 -> n = 235
    assertThat(DupNOperation.decodeSingle(90)).isEqualTo(235);
    // x = 45 -> n = 190
    assertThat(DupNOperation.decodeSingle(45)).isEqualTo(190);
  }

  @Test
  void testDecodeSingle_validHighRange() {
    // n = (x + 145) % 256
    // x = 128 -> n = 17
    assertThat(DupNOperation.decodeSingle(128)).isEqualTo(17);
    // x = 255 -> n = 144
    assertThat(DupNOperation.decodeSingle(255)).isEqualTo(144);
    // x = 200 -> n = 89
    assertThat(DupNOperation.decodeSingle(200)).isEqualTo(89);
  }

  @ParameterizedTest
  @ValueSource(ints = {91, 100, 110, 127})
  void testDecodeSingle_invalidRange(final int imm) {
    // Invalid range: 91-127
    assertThat(DupNOperation.decodeSingle(imm)).isEqualTo(-1);
  }

  @Test
  void testDupN_basicOperation() {
    // DUPN with immediate 0x80 -> n=(128+145)%256=17, duplicates 17th item
    // Set up stack with 17 items, verify the 17th (bottom) item is duplicated
    final Bytes code = Bytes.fromHexString("e680"); // DUPN 0x80 -> n=17
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 17 items onto stack (first pushed = bottom = item 17)
    for (int i = 17; i >= 1; i--) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(3);
    assertThat(result.getPcIncrement()).isEqualTo(2);
    // Top of stack should now be a copy of item 17 (value 17)
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(17));
    // Stack should have 18 items now
    assertThat(frame.stackSize()).isEqualTo(18);
  }

  @Test
  void testDupN_immediate90() {
    // DUPN with immediate 90 -> n=(90+145)%256=235
    // Set up minimal test to verify decode works correctly
    final Bytes code = Bytes.of(0xe6, 90); // DUPN -> n=235
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 235 items
    for (int i = 235; i >= 1; i--) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isEqualTo(2);
    // Should duplicate item 235 (value 235)
    assertThat(frame.getStackItem(0).toInt()).isEqualTo(235);
  }

  @Test
  void testDupN_immediateFF() {
    // DUPN with immediate 0xFF -> n=(255+145)&0xFF=144
    final Bytes code = Bytes.fromHexString("e6ff"); // DUPN 0xFF -> n=144
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 144 items
    for (int i = 144; i >= 1; i--) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // Should duplicate item 144 (value 144)
    assertThat(frame.getStackItem(0).toInt()).isEqualTo(144);
    assertThat(frame.stackSize()).isEqualTo(145);
  }

  @ParameterizedTest
  @ValueSource(ints = {91, 0x5b, 0x60, 0x7f, 100, 127})
  void testDupN_invalidImmediate(final int imm) {
    // Invalid immediate range: 91-127 (includes JUMPDEST 0x5b and PUSH ops 0x60-0x7f)
    final Bytes code = Bytes.wrap(new byte[] {(byte) 0xe6, (byte) imm});
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Add enough stack items that underflow won't be the issue
    for (int i = 0; i < 250; i++) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  @Test
  void testDupN_stackUnderflow() {
    // DUPN with immediate 0x80 -> n=17, but only 10 items on stack
    final Bytes code = Bytes.fromHexString("e680"); // DUPN -> n=17
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push only 10 items (need 17)
    for (int i = 0; i < 10; i++) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  @Test
  void testDupN_endOfCode() {
    // Code ends right after opcode (no immediate byte)
    // Immediate should be treated as 0 -> n=(0+145)%256=145
    // With only 17 items on stack, this causes underflow
    final Bytes code = Bytes.of(0xe6); // Just DUPN, no immediate
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 17 items (not enough for n=145)
    for (int i = 17; i >= 1; i--) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    // Immediate 0 is valid but n=145 causes underflow with only 17 items
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  @Test
  void testDupN_gasCost() {
    final Bytes code = Bytes.fromHexString("e680"); // DUPN 0x80 -> n=17
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0).initialGas(100);

    for (int i = 17; i >= 1; i--) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(3);
  }

  @Test
  void testDupN_stackOverflow() {
    // Fill stack to maximum capacity (1024 items), then try to duplicate
    final Bytes code = Bytes.fromHexString("e680"); // DUPN 0x80 -> n=17
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 1024 items (max stack size)
    for (int i = 0; i < 1024; i++) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    assertThat(frame.stackSize()).isEqualTo(1024);

    final OperationResult result = operation.execute(frame, null);

    // Should fail with stack overflow since duplicating would exceed 1024
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  // ==================== EIP-8024 Spec Test Vectors ====================

  /**
   * Spec test vector: DUPN with immediate 0x80 (n=17) duplicating 17th item from 18-item stack.
   * Bytecode: 60016000808080808080808080808080808080e680 Stack setup: PUSH1 1, PUSH1 0, 16xDUP1 ->
   * 18 items with value 1 at bottom.
   */
  @Test
  void testSpecVector_dupn17With18Items() {
    // Set up stack as if: PUSH1 1, PUSH1 0, 16xDUP1 were executed
    // Result: 18 items, stack[0-16]=0, stack[17]=1
    final Bytes code = Bytes.fromHexString("e680"); // DUPN 0x80 -> n=17
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Bottom of stack (pushed first) = 1, then 0, then 16 copies of 0 (via DUP1)
    builder.pushStackItem(Bytes.of(1)); // stack[17] = 1
    for (int i = 0; i < 17; i++) {
      builder.pushStackItem(Bytes.of(0)); // stack[16..0] = 0
    }
    final MessageFrame frame = builder.build();

    assertThat(frame.stackSize()).isEqualTo(18);
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(0)); // top
    assertThat(frame.getStackItem(17)).isEqualTo(Bytes.of(1)); // bottom

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isEqualTo(2);
    // DUPN 0x80 -> n=17, duplicates stack[16] which is 0
    assertThat(frame.stackSize()).isEqualTo(19);
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(0)); // duplicated value
  }

  /**
   * Spec test vector: DUPN at end of code (implicit immediate 0). Bytecode:
   * 60016000808080808080808080808080808080e6 Immediate defaults to 0 when code ends, which now maps
   * to n=145 and causes underflow with only 18 items.
   */
  @Test
  void testSpecVector_dupnEndOfCode() {
    // Code ends right after opcode, immediate treated as 0 -> n=145
    final Bytes code = Bytes.of(0xe6); // Just DUPN, no immediate
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Same stack setup: 18 items, but n=145 requires 145 items
    builder.pushStackItem(Bytes.of(1)); // stack[17] = 1
    for (int i = 0; i < 17; i++) {
      builder.pushStackItem(Bytes.of(0)); // stack[16..0] = 0
    }
    final MessageFrame frame = builder.build();

    assertThat(frame.stackSize()).isEqualTo(18);

    final OperationResult result = operation.execute(frame, null);

    // Implicit immediate 0 is valid but n=145 causes underflow
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  /**
   * Spec test vector: Invalid immediate 0x5f (PUSH0 opcode). Bytecode: e65f 0x5f (95) is in invalid
   * range 91-127, should return INVALID_OPERATION.
   */
  @Test
  void testSpecVector_invalidImmediate0x5f() {
    final Bytes code = Bytes.of(0xe6, 0x5f); // DUPN with invalid immediate 0x5f
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Add sufficient stack items
    for (int i = 0; i < 250; i++) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  /**
   * Spec test vector: Stack underflow with 16 items (needs 17). Bytecode:
   * 6000808080808080808080808080808080e680 Stack has 16 items from PUSH1 0 + 15xDUP1, but DUPN 0x80
   * (n=17) needs 17.
   */
  @Test
  void testSpecVector_stackUnderflow() {
    final Bytes code = Bytes.fromHexString("e680"); // DUPN 0x80 -> n=17
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Only 16 items (simulating PUSH1 0 + 15xDUP1)
    for (int i = 0; i < 16; i++) {
      builder.pushStackItem(Bytes.of(0));
    }
    final MessageFrame frame = builder.build();

    assertThat(frame.stackSize()).isEqualTo(16);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }
}
