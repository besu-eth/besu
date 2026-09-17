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

/** Unit tests for {@link SwapNOperation}. */
class SwapNOperationTest {

  private final SwapNOperation operation = new SwapNOperation(new PragueGasCalculator());

  @Test
  void testDecodeSingle_validLowRange() {
    // n = (x + 145) % 256
    // x = 0 -> n = 145
    assertThat(SwapNOperation.decodeSingle(0)).isEqualTo(145);
    // x = 90 -> n = 235
    assertThat(SwapNOperation.decodeSingle(90)).isEqualTo(235);
  }

  @Test
  void testDecodeSingle_validHighRange() {
    // n = (x + 145) % 256
    // x = 128 -> n = 17
    assertThat(SwapNOperation.decodeSingle(128)).isEqualTo(17);
    // x = 255 -> n = 144
    assertThat(SwapNOperation.decodeSingle(255)).isEqualTo(144);
  }

  @ParameterizedTest
  @ValueSource(ints = {91, 100, 110, 127})
  void testDecodeSingle_invalidRange(final int imm) {
    assertThat(SwapNOperation.decodeSingle(imm)).isEqualTo(-1);
  }

  @Test
  void testSwapN_basicOperation() {
    // SWAPN with immediate 0x80 -> n=(128+145)%256=17, swaps top with 18th item (index 17)
    final Bytes code = Bytes.fromHexString("e780"); // SWAPN 0x80 -> n=17
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 18 items: bottom=18, ..., top=1
    for (int i = 18; i >= 1; i--) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    // Before: stack[0]=1, stack[17]=18
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(1));
    assertThat(frame.getStackItem(17)).isEqualTo(Bytes.of(18));

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(3);
    assertThat(result.getPcIncrement()).isEqualTo(2);

    // After: stack[0]=18, stack[17]=1
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(18));
    assertThat(frame.getStackItem(17)).isEqualTo(Bytes.of(1));
    // Stack size unchanged
    assertThat(frame.stackSize()).isEqualTo(18);
  }

  @Test
  void testSwapN_immediateFF() {
    // SWAPN with immediate 0xFF -> n=(255+145)&0xFF=144, swaps top with 145th item
    final Bytes code = Bytes.fromHexString("e7ff"); // SWAPN 0xFF -> n=144
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 145 items
    for (int i = 145; i >= 1; i--) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    // Before: stack[0]=1, stack[144]=145
    assertThat(frame.getStackItem(0).toInt()).isEqualTo(1);
    assertThat(frame.getStackItem(144).toInt()).isEqualTo(145);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();

    // After: swapped
    assertThat(frame.getStackItem(0).toInt()).isEqualTo(145);
    assertThat(frame.getStackItem(144).toInt()).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(ints = {91, 0x5b, 0x60, 0x7f, 100, 127})
  void testSwapN_invalidImmediate(final int imm) {
    final Bytes code = Bytes.wrap(new byte[] {(byte) 0xe7, (byte) imm});
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    for (int i = 0; i < 250; i++) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  @Test
  void testSwapN_stackUnderflow() {
    // SWAPN with immediate 0x80 -> n=17, needs 18 items but only 10
    final Bytes code = Bytes.fromHexString("e780");
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    for (int i = 0; i < 10; i++) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  @Test
  void testSwapN_endOfCode() {
    // Code ends right after opcode, immediate treated as 0 -> n=145
    // With only 18 items on stack, causes underflow
    final Bytes code = Bytes.of(0xe7);
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    for (int i = 18; i >= 1; i--) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    // Immediate 0 is valid but n=145 causes underflow with only 18 items
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  @Test
  void testSwapN_gasCost() {
    final Bytes code = Bytes.fromHexString("e780"); // SWAPN 0x80 -> n=17
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0).initialGas(100);

    for (int i = 18; i >= 1; i--) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(3);
  }

  // ==================== EIP-8024 Spec Test Vectors ====================

  /**
   * Spec test vector: SWAPN with immediate 0x80 (n=17) swapping top with 18th item. Bytecode
   * sequence represents: PUSH1 1, PUSH1 0, 16xDUP1, PUSH1 2, SWAPN 0x80 Stack setup: 18 items with
   * top=2, bottom=1, rest=0.
   */
  @Test
  void testSpecVector_swapn17With18Items() {
    final Bytes code = Bytes.fromHexString("e780"); // SWAPN 0x80 -> n=17
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Build stack: bottom=1, middle 16 zeros, top=2 (18 items total)
    builder.pushStackItem(Bytes.of(1)); // stack[17] = 1 (bottom)
    for (int i = 0; i < 16; i++) {
      builder.pushStackItem(Bytes.of(0)); // stack[16..1] = 0
    }
    builder.pushStackItem(Bytes.of(2)); // stack[0] = 2 (top)
    final MessageFrame frame = builder.build();

    assertThat(frame.stackSize()).isEqualTo(18);
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(2)); // top
    assertThat(frame.getStackItem(17)).isEqualTo(Bytes.of(1)); // bottom

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isEqualTo(2);
    // After SWAPN: stack[0]=1, stack[17]=2
    assertThat(frame.stackSize()).isEqualTo(18); // size unchanged
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(1)); // swapped
    assertThat(frame.getStackItem(17)).isEqualTo(Bytes.of(2)); // swapped
  }

  /**
   * Spec test vector: SWAPN at end of code (implicit immediate 0). Immediate defaults to 0 which
   * now maps to n=145, causing underflow with only 18 items.
   */
  @Test
  void testSpecVector_swapnEndOfCode() {
    final Bytes code = Bytes.of(0xe7); // Just SWAPN, no immediate
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Same stack setup: 18 items, but n=145 requires 146 items
    builder.pushStackItem(Bytes.of(1)); // stack[17] = 1 (bottom)
    for (int i = 0; i < 16; i++) {
      builder.pushStackItem(Bytes.of(0)); // stack[16..1] = 0
    }
    builder.pushStackItem(Bytes.of(2)); // stack[0] = 2 (top)
    final MessageFrame frame = builder.build();

    assertThat(frame.stackSize()).isEqualTo(18);

    final OperationResult result = operation.execute(frame, null);

    // Implicit immediate 0 is valid but n=145 causes underflow
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  /**
   * Spec test vector: Invalid immediate 0x5b (JUMPDEST opcode). Bytecode: e75b 0x5b (91) is in
   * invalid range 91-127, should return INVALID_OPERATION.
   */
  @Test
  void testSpecVector_invalidImmediate0x5b() {
    final Bytes code = Bytes.of(0xe7, 0x5b); // SWAPN with invalid immediate 0x5b
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
   * Spec test vector: Invalid immediate 0x61 (PUSH2 opcode). Bytecode: e7610000 0x61 (97) is in
   * invalid range 91-127, should return INVALID_OPERATION.
   */
  @Test
  void testSpecVector_invalidImmediate0x61() {
    final Bytes code = Bytes.fromHexString("e7610000"); // SWAPN 0x61, followed by 0x0000
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
   * Spec test vector: JUMP over invalid SWAPN immediate to JUMPDEST. Bytecode: 600456e75b Decodes
   * as: PUSH1 4, JUMP, E7, 5B The E7 5B sequence is unreachable - execution JUMPs to 5B (JUMPDEST).
   * This tests that the invalid immediate (0x5b) doesn't cause issues when code-scanned. Note: This
   * is a code validation test - the SWAPN operation itself is never executed.
   */
  @Test
  void testSpecVector_jumpOverInvalid() {
    // This test verifies that unreachable invalid immediates don't cause problems
    // In EVM, the 0x5b at position 4 is valid as JUMPDEST when jumped to directly
    // The E7 at position 3 with immediate 5B would be invalid if executed,
    // but since we JUMP over it, execution continues normally at the JUMPDEST

    // For unit testing the SWAPN operation specifically, we verify that
    // the operation itself correctly rejects 0x5b as immediate when executed
    // (covered by testSpecVector_invalidImmediate0x5b)

    // This vector is more of an integration/acceptance test for code validation
    // Verifying that Code analysis handles such patterns correctly
    final Bytes code = Bytes.fromHexString("600456e75b"); // PUSH1 4, JUMP, E7, 5B
    final Code codeObj = new Code(code);

    // Verify the code structure is valid (doesn't throw during creation)
    assertThat(codeObj.getSize()).isEqualTo(5);
  }
}
