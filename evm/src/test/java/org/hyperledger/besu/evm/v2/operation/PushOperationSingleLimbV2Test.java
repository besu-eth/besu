/*
 * Copyright ConsenSys AG.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.v2.operation.PushOperationV2.SingleLimb.staticOperation;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.OverflowException;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link PushOperationV2.SingleLimb}, covering PUSH2–PUSH8 (pushSize 2..8). All values
 * fit in a single 64-bit limb.
 */
public class PushOperationSingleLimbV2Test {

  private MessageFrame frame;

  @BeforeEach
  public void setUp() {
    frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(new ToyWorld())
            .originator(Address.ZERO)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(new ToyBlockValues())
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .initialGas(1)
            .address(Address.ZERO)
            .contract(Address.ZERO)
            .inputData(Bytes32.ZERO)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .code(Code.EMPTY_CODE)
            .build();
  }

  // --- Boundary: each supported pushSize in the middle of a long code array ---

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8})
  void pushInMiddleOfLongCode(final int pushSize) {
    final byte[] code = generateCode(100);
    final int pc = 10;
    staticOperation(frame, code, pc, pushSize);
    assertThat(frame.getStackItemV2(0))
        .as("pushSize=%d", pushSize)
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, pc + 1, pc + 1 + pushSize)));
  }

  // --- PUSH2: explicit values ---

  @Test
  void push2Normal() {
    final byte[] code = {0x00, 0x12, 0x34};
    staticOperation(frame, code, 0, 2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0x1234L));
  }

  @Test
  void push2WithHighBits_noSignExtension() {
    final byte[] code = {0x00, (byte) 0xFF, (byte) 0xFF};
    staticOperation(frame, code, 0, 2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0xFFFFL));
  }

  @Test
  void push2TruncatedOneByteAvailable() {
    // only the first byte of a 2-byte push is within bounds; second is zero-padded
    final byte[] code = {0x00, (byte) 0xAB}; // code.length == 2, end = 3 > 2
    staticOperation(frame, code, 0, 2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0xAB00L));
  }

  @Test
  void push2TruncatedNoBytesAvailable() {
    // start = pc + 1 >= code.length
    final byte[] code = {0x00};
    staticOperation(frame, code, 0, 2); // start = 1 == code.length
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  // --- PUSH8: max single-limb push ---

  @Test
  void push8Normal() {
    final byte[] code = new byte[10];
    // fill with a known pattern
    for (int i = 0; i < code.length; i++) code[i] = (byte) (i + 1);
    staticOperation(frame, code, 0, 8);
    // bytes at positions 1..8
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, 1, 9)));
  }

  @Test
  void push8AllOnes_noSignExtension() {
    final byte[] code = new byte[10];
    Arrays.fill(code, (byte) 0xFF);
    staticOperation(frame, code, 0, 8);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(-1L)); // 0xFFFF_FFFF_FFFF_FFFFL
  }

  @Test
  void push8TruncatedFourBytesAvailable() {
    // only 4 of 8 bytes are within bounds; remaining 4 are zero-padded on the right
    final byte[] code = new byte[6]; // pc=0 → start=1, end=9, available=5 bytes (code[1..5])
    for (int i = 0; i < code.length; i++) code[i] = (byte) (i + 1);
    staticOperation(frame, code, 0, 8);
    // expected: bytes 1..5 of code, then 3 zero bytes (shift left 3*8=24)
    final byte[] expected = new byte[8];
    System.arraycopy(code, 1, expected, 0, 5);
    // remaining 3 bytes of expected remain zero
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromBytesBE(expected));
  }

  @Test
  void push8TruncatedNoBytesAvailable() {
    // start is out of bounds entirely
    final byte[] code = {0x00};
    staticOperation(frame, code, 0, 8); // start = 1 == code.length
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  // --- Truncation: last byte of various push sizes ---

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8})
  void pushTruncatedOneByteAtEndOfCode(final int pushSize) {
    // exactly one data byte available; rest must be zero-padded on the right
    final byte[] code = new byte[2]; // pc=0 → start=1, only code[1] available
    code[0] = 0x00;
    code[1] = (byte) 0xAB;
    staticOperation(frame, code, 0, pushSize);
    // expected: 0xAB shifted left by (pushSize-1)*8 bits
    final long expected = 0xABL << ((pushSize - 1) * 8);
    assertThat(frame.getStackItemV2(0))
        .as("pushSize=%d", pushSize)
        .isEqualTo(UInt256.fromLong(expected));
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8})
  void pushOobStartEqualsCodeLength_pushesZero(final int pushSize) {
    final byte[] code = {0x00}; // length = 1, start = 1 >= code.length
    staticOperation(frame, code, 0, pushSize);
    assertThat(frame.getStackItemV2(0)).as("pushSize=%d", pushSize).isEqualTo(UInt256.ZERO);
  }

  // --- Stack interaction ---

  @Test
  void push2ThenPush3GivesCorrectStackOrder() {
    final byte[] code = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66};
    staticOperation(frame, code, 0, 2); // pushes 0x1122
    staticOperation(frame, code, 2, 3); // pushes 0x334455
    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0x334455L));
    assertThat(frame.getStackItemV2(1)).isEqualTo(UInt256.fromLong(0x1122L));
  }

  @Test
  void pushOntoFullStackOverflows() {
    final byte[] code = generateCode(20);
    while (frame.stackHasSpaceV2(1)) {
      frame.setTopV2(frame.stackTopV2() + 1);
    }
    final int top = frame.stackTopV2();
    assertThrows(OverflowException.class, () -> staticOperation(frame, code, 0, 4));
    assertThat(frame.stackTopV2()).isEqualTo(top);
  }

  // --- Exhaustive: all pushSize 2..8 at all pc positions ---

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8})
  void exhaustivePushSizesAndPositions(final int pushSize) {
    final byte[] code = new byte[60];
    for (int i = 0; i < code.length; i++) {
      code[i] = (byte) (i * 31 + 7); // varied bytes with high bits set
    }
    for (int pc = 0; pc < code.length; pc++) {
      frame.setTopV2(0);
      staticOperation(frame, code, pc, pushSize);
      final byte[] expected = new byte[pushSize];
      final int available = Math.max(0, Math.min(pushSize, code.length - pc - 1));
      System.arraycopy(code, Math.min(pc + 1, code.length), expected, 0, available);
      assertThat(frame.getStackItemV2(0))
          .as("pushSize=%d pc=%d", pushSize, pc)
          .isEqualTo(UInt256.fromBytesBE(expected));
    }
  }

  private static byte[] generateCode(final int numBytes) {
    final byte[] code = new byte[numBytes];
    for (int i = 0; i < code.length; i++) {
      code[i] = (byte) i;
    }
    return code;
  }
}
