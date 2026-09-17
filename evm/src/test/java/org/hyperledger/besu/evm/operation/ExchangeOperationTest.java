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

/** Unit tests for {@link ExchangeOperation}. */
class ExchangeOperationTest {

  private final ExchangeOperation operation = new ExchangeOperation(new PragueGasCalculator());

  @Test
  void testDecodePair_immediate0x9d() {
    // k = x ^ 143 = 0x9d ^ 0x8f = 0x12 = 18
    // q=18/16=1, r=18%16=2, q < r, so return (q+1, r+1) = (2, 3)
    int[] result = ExchangeOperation.decodePair(0x9d);
    assertThat(result).isNotNull();
    assertThat(result[0]).isEqualTo(2);
    assertThat(result[1]).isEqualTo(3);
  }

  @Test
  void testDecodePair_immediate0x2f() {
    // k = 0x2f ^ 0x8f = 0xa0 = 160
    // q=160/16=10, r=160%16=0, q >= r, so return (r+1, 29-q) = (1, 19)
    int[] result = ExchangeOperation.decodePair(0x2f);
    assertThat(result).isNotNull();
    assertThat(result[0]).isEqualTo(1);
    assertThat(result[1]).isEqualTo(19);
  }

  @Test
  void testDecodePair_immediate0() {
    // x=0, k=0^143=143, q=143/16=8, r=143%16=15
    // q < r, so return (q+1, r+1) = (9, 16)
    int[] result = ExchangeOperation.decodePair(0);
    assertThat(result).isNotNull();
    assertThat(result[0]).isEqualTo(9);
    assertThat(result[1]).isEqualTo(16);
  }

  @Test
  void testDecodePair_immediate79() {
    // x=79=0x4f, k=0x4f^0x8f=0xc0=192, q=192/16=12, r=0
    // q >= r, so return (r+1, 29-q) = (1, 17)
    int[] result = ExchangeOperation.decodePair(79);
    assertThat(result).isNotNull();
    assertThat(result[0]).isEqualTo(1);
    assertThat(result[1]).isEqualTo(17);
  }

  @Test
  void testDecodePair_newPair14_15() {
    // x=81=0x51, k=0x51^0x8f=0xde=222, q=222/16=13, r=222%16=14
    // q < r, so return (q+1, r+1) = (14, 15)
    int[] result = ExchangeOperation.decodePair(81);
    assertThat(result).isNotNull();
    assertThat(result[0]).isEqualTo(14);
    assertThat(result[1]).isEqualTo(15);
  }

  @Test
  void testDecodePair_newPair14_16() {
    // x=80=0x50, k=0x50^0x8f=0xdf=223, q=223/16=13, r=223%16=15
    // q < r, so return (q+1, r+1) = (14, 16)
    int[] result = ExchangeOperation.decodePair(80);
    assertThat(result).isNotNull();
    assertThat(result[0]).isEqualTo(14);
    assertThat(result[1]).isEqualTo(16);
  }

  @ParameterizedTest
  @ValueSource(ints = {82, 91, 100, 127})
  void testDecodePair_invalidRange(final int imm) {
    // Invalid range: 82-127
    assertThat(ExchangeOperation.decodePair(imm)).isNull();
  }

  @Test
  void testExchange_basicOperation() {
    // EXCHANGE with immediate 0x9d -> k=0x9d^0x8f=0x12=18, (n=2, m=3)
    // Swaps stack[2] with stack[3]
    final Bytes code = Bytes.fromHexString("e89d");
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 4 items: stack[0]=1, stack[1]=2, stack[2]=3, stack[3]=4
    builder.pushStackItem(Bytes.of(4)); // becomes stack[3]
    builder.pushStackItem(Bytes.of(3)); // becomes stack[2]
    builder.pushStackItem(Bytes.of(2)); // becomes stack[1]
    builder.pushStackItem(Bytes.of(1)); // becomes stack[0]
    final MessageFrame frame = builder.build();

    // Before
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(1));
    assertThat(frame.getStackItem(1)).isEqualTo(Bytes.of(2));
    assertThat(frame.getStackItem(2)).isEqualTo(Bytes.of(3));
    assertThat(frame.getStackItem(3)).isEqualTo(Bytes.of(4));

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(3);
    assertThat(result.getPcIncrement()).isEqualTo(2);

    // After: stack[2] and stack[3] swapped
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(1));
    assertThat(frame.getStackItem(1)).isEqualTo(Bytes.of(2));
    assertThat(frame.getStackItem(2)).isEqualTo(Bytes.of(4)); // was 3
    assertThat(frame.getStackItem(3)).isEqualTo(Bytes.of(3)); // was 4
    assertThat(frame.stackSize()).isEqualTo(4);
  }

  @Test
  void testExchange_immediate0x2f() {
    // EXCHANGE with immediate 0x2f -> k=0x2f^0x8f=0xa0=160, (n=1, m=19)
    // Swaps stack[1] with stack[19]
    final Bytes code = Bytes.fromHexString("e82f");
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 20 items
    for (int i = 20; i >= 1; i--) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    // Before: stack[1]=2, stack[19]=20
    assertThat(frame.getStackItem(1).toInt()).isEqualTo(2);
    assertThat(frame.getStackItem(19).toInt()).isEqualTo(20);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();

    // After: swapped
    assertThat(frame.getStackItem(1).toInt()).isEqualTo(20);
    assertThat(frame.getStackItem(19).toInt()).isEqualTo(2);
  }

  @ParameterizedTest
  @ValueSource(ints = {82, 91, 0x5b, 0x60, 0x7f, 100, 127})
  void testExchange_invalidImmediate(final int imm) {
    final Bytes code = Bytes.wrap(new byte[] {(byte) 0xe8, (byte) imm});
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    for (int i = 0; i < 50; i++) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  @Test
  void testExchange_stackUnderflow() {
    // EXCHANGE with immediate 0x9d -> (n=2, m=3), needs 4 items but only 2
    final Bytes code = Bytes.fromHexString("e89d");
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    builder.pushStackItem(Bytes.of(1));
    builder.pushStackItem(Bytes.of(2));
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }

  @Test
  void testExchange_endOfCode() {
    // Code ends right after opcode, immediate treated as 0
    // x=0 -> k=0^143=143, q=8, r=15, q<r -> (n=9, m=16), needs 17 items
    final Bytes code = Bytes.of(0xe8);
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    for (int i = 17; i >= 1; i--) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    // Before: stack[9]=10, stack[16]=17
    assertThat(frame.getStackItem(9)).isEqualTo(Bytes.of(10));
    assertThat(frame.getStackItem(16)).isEqualTo(Bytes.of(17));

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // Verify swap: stack[9] and stack[16] swapped
    assertThat(frame.getStackItem(9)).isEqualTo(Bytes.of(17));
    assertThat(frame.getStackItem(16)).isEqualTo(Bytes.of(10));
  }

  @Test
  void testExchange_gasCost() {
    final Bytes code = Bytes.fromHexString("e89d"); // EXCHANGE 0x9d -> (2, 3)
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0).initialGas(100);

    for (int i = 0; i < 10; i++) {
      builder.pushStackItem(Bytes.of(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(3);
  }

  // ==================== EIP-8024 Spec Test Vectors ====================

  /**
   * Spec test vector: EXCHANGE with immediate 0x8e. Bytecode sequence: 5f60016002e88e (PUSH0, PUSH1
   * 1, PUSH1 2, EXCHANGE 0x8e) Stack before: [2, 1, 0] (top to bottom) Expected after: [2, 0, 1]
   * (swaps stack[1] with stack[2])
   *
   * <p>For immediate 0x8e: k=0x8e^0x8f=1, q=0, r=1, q < r so (n,m) = (1, 2)
   */
  @Test
  void testSpecVector_exchange_e88e() {
    final Bytes code = Bytes.fromHexString("e88e"); // EXCHANGE 0x8e -> (1, 2)
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Stack setup: PUSH0, PUSH1 1, PUSH1 2 -> [2, 1, 0]
    builder.pushStackItem(Bytes.of(0)); // stack[2] = 0
    builder.pushStackItem(Bytes.of(1)); // stack[1] = 1
    builder.pushStackItem(Bytes.of(2)); // stack[0] = 2
    final MessageFrame frame = builder.build();

    // Verify initial state
    assertThat(frame.stackSize()).isEqualTo(3);
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(2)); // top
    assertThat(frame.getStackItem(1)).isEqualTo(Bytes.of(1));
    assertThat(frame.getStackItem(2)).isEqualTo(Bytes.of(0)); // bottom

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isEqualTo(2);

    // After EXCHANGE 0x8e: stack[1] and stack[2] swapped
    assertThat(frame.stackSize()).isEqualTo(3); // size unchanged
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(2)); // unchanged
    assertThat(frame.getStackItem(1)).isEqualTo(Bytes.of(0)); // swapped
    assertThat(frame.getStackItem(2)).isEqualTo(Bytes.of(1)); // swapped
  }

  /**
   * Spec test vector: EXCHANGE with 30 items on stack. Uses immediate 0x8f which decodes to (n=1,
   * m=29) via k=0x8f^0x8f=0, q=0, r=0, q>=r -> (1, 29). Swaps stack[1] with stack[29].
   */
  @Test
  void testSpecVector_exchange30Items() {
    final Bytes code = Bytes.fromHexString("e88f"); // EXCHANGE 0x8f -> (1, 29)
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Push 30 items: stack[29]=1, stack[28..1]=values, stack[0]=2
    // Using distinct values at positions we care about
    builder.pushStackItem(Bytes.of(1)); // stack[29] = 1 (bottom)
    for (int i = 28; i >= 2; i--) {
      builder.pushStackItem(Bytes.of(0)); // stack[28..2] = 0
    }
    builder.pushStackItem(Bytes.of(99)); // stack[1] = 99
    builder.pushStackItem(Bytes.of(2)); // stack[0] = 2 (top)
    final MessageFrame frame = builder.build();

    assertThat(frame.stackSize()).isEqualTo(30);
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(2)); // top
    assertThat(frame.getStackItem(1)).isEqualTo(Bytes.of(99));
    assertThat(frame.getStackItem(29)).isEqualTo(Bytes.of(1)); // bottom

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();

    // After EXCHANGE: stack[1] and stack[29] swapped
    assertThat(frame.stackSize()).isEqualTo(30);
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.of(2)); // unchanged
    assertThat(frame.getStackItem(1)).isEqualTo(Bytes.of(1)); // was 99, now 1
    assertThat(frame.getStackItem(29)).isEqualTo(Bytes.of(99)); // was 1, now 99
  }

  /**
   * Spec test vector: Invalid immediate 0x52 (82). Bytecode: e852 0x52 (82) is in invalid range
   * 82-127, should return INVALID_OPERATION. Note: 0x50 (80) and 0x51 (81) are now valid,
   * supporting new pairs (14,16) and (14,15).
   */
  @Test
  void testSpecVector_invalidImmediate0x52() {
    final Bytes code = Bytes.of(0xe8, 0x52); // EXCHANGE with invalid immediate 0x52
    final TestMessageFrameBuilder builder =
        new TestMessageFrameBuilder().code(new Code(code)).pc(0);

    // Add sufficient stack items
    for (int i = 0; i < 50; i++) {
      builder.pushStackItem(Bytes.ofUnsignedInt(i));
    }
    final MessageFrame frame = builder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(result.getPcIncrement()).isEqualTo(2);
  }
}
