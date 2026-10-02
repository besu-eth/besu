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
import static org.hyperledger.besu.evm.v2.operation.PushOperationV2.MultiLimb.staticOperation;
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
 * Tests for {@link PushOperationV2.MultiLimb}, covering PUSH9–PUSH32 (pushSize 9..32). Values span
 * multiple 64-bit limbs of the UInt256 representation.
 */
public class PushOperationMultiLimbV2Test {

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

  // --- Each supported pushSize, normal (no truncation) ---

  @ParameterizedTest
  @ValueSource(
      ints = {
        9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        32
      })
  void pushInMiddleOfLongCode(final int pushSize) {
    final byte[] code = generateCode(100);
    final int pc = 5;
    staticOperation(frame, code, pc, pushSize);
    assertThat(frame.getStackItemV2(0))
        .as("pushSize=%d", pushSize)
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, pc + 1, pc + 1 + pushSize)));
  }

  // --- Limb boundaries ---

  @Test
  void push9_crossesFirstLimbBoundary() {
    // 9 bytes = 8-byte limb + 1 extra byte
    final byte[] code = new byte[12];
    for (int i = 0; i < code.length; i++) code[i] = (byte) (i + 1);
    staticOperation(frame, code, 0, 9);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, 1, 10)));
  }

  @Test
  void push16_twoFullLimbs() {
    final byte[] code = new byte[18];
    for (int i = 0; i < code.length; i++) code[i] = (byte) (i + 1);
    staticOperation(frame, code, 0, 16);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, 1, 17)));
  }

  @Test
  void push24_threeFullLimbs() {
    final byte[] code = new byte[26];
    for (int i = 0; i < code.length; i++) code[i] = (byte) (i + 1);
    staticOperation(frame, code, 0, 24);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, 1, 25)));
  }

  @Test
  void push32_fullWord() {
    // PUSH32 fills all four limbs of a UInt256
    final byte[] code = new byte[34];
    for (int i = 0; i < code.length; i++) code[i] = (byte) (i + 1);
    staticOperation(frame, code, 0, 32);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, 1, 33)));
  }

  @Test
  void push32MaxValue_allOnes() {
    // All 0xFF bytes: result should be UInt256.MAX_VALUE
    final byte[] code = new byte[34];
    Arrays.fill(code, (byte) 0xFF);
    staticOperation(frame, code, 0, 32);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.MAX);
  }

  @Test
  void push32Zero_allZeroBytes() {
    final byte[] code = new byte[34];
    staticOperation(frame, code, 0, 32);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  // --- Truncation: push extends past end of code (zero-padding on the right) ---

  @Test
  void push9TruncatedNoBytesAvailable() {
    // start == code.length: no bytes at all
    final byte[] code = {0x00};
    staticOperation(frame, code, 0, 9); // start = 1 == code.length
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void push32TruncatedNoBytesAvailable() {
    final byte[] code = {0x00};
    staticOperation(frame, code, 0, 32);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void push32TruncatedHalfAvailable() {
    // 16 of 32 bytes available; trailing 16 should be zero
    final byte[] code = new byte[17]; // pc=0 → start=1, 16 bytes available (code[1..16])
    for (int i = 0; i < code.length; i++) code[i] = (byte) (i + 1);
    staticOperation(frame, code, 0, 32);
    final byte[] expected = new byte[32];
    System.arraycopy(code, 1, expected, 0, 16); // first 16 bytes from code
    // expected[16..31] remain zero
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromBytesBE(expected));
  }

  @Test
  void push32TruncatedOneByteAvailable() {
    // only the very first byte of 32 is within bounds
    final byte[] code = {0x00, (byte) 0xBE}; // start = 1, only code[1] available
    staticOperation(frame, code, 0, 32);
    // 0xBE shifted left 31 bytes (248 bits)
    final byte[] expected = new byte[32];
    expected[0] = (byte) 0xBE;
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromBytesBE(expected));
  }

  @Test
  void push32TruncatedAtLimbBoundary_8BytesAvailable() {
    // exactly 8 bytes (one limb) available; remaining 24 are zero-padded
    final byte[] code = new byte[10]; // pc=0 → start=1, 9 bytes (code[1..9])
    for (int i = 0; i < code.length; i++) code[i] = (byte) (0xA0 + i);
    staticOperation(frame, code, 0, 32);
    final byte[] expected = new byte[32];
    System.arraycopy(code, 1, expected, 0, 9);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromBytesBE(expected));
  }

  @ParameterizedTest
  @ValueSource(ints = {9, 16, 20, 32})
  void pushTruncatedOneByteAtEndOfCode(final int pushSize) {
    // only one data byte is within bounds; rest must be zero-padded on the right
    final byte[] code = {0x00, (byte) 0xCD}; // start=1, only code[1] available
    staticOperation(frame, code, 0, pushSize);
    final byte[] expected = new byte[pushSize];
    expected[0] = (byte) 0xCD;
    assertThat(frame.getStackItemV2(0))
        .as("pushSize=%d", pushSize)
        .isEqualTo(UInt256.fromBytesBE(expected));
  }

  // --- No sign-extension for high-bit bytes ---

  @Test
  void push9WithHighBitBytes_noSignExtension() {
    final byte[] code = new byte[11];
    Arrays.fill(code, (byte) 0x80);
    staticOperation(frame, code, 0, 9);
    final byte[] expected = new byte[9];
    Arrays.fill(expected, (byte) 0x80);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromBytesBE(expected));
  }

  @Test
  void push20InMiddleOfCode_highBitBytes() {
    final byte[] code = new byte[100];
    for (int i = 0; i < code.length; i++) code[i] = (byte) (i | 0x80); // all high-bit set
    final int pc = 10;
    staticOperation(frame, code, pc, 20);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, pc + 1, pc + 21)));
  }

  // --- Near-end-of-code tests ---

  @Test
  void push32NearEndOfLongCode_20BytesPadded() {
    // 20 bytes of code remain after pc; last 12 of the 32 are zero-padded
    final byte[] code = generateCode(100);
    final int pc = code.length - 21;
    staticOperation(frame, code, pc, 32);
    final byte[] expected = new byte[32];
    System.arraycopy(code, pc + 1, expected, 0, 20);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromBytesBE(expected));
  }

  @Test
  void push9NearEndOfCode_3BytesPadded() {
    // 6 bytes available; 3 are zero-padded
    final byte[] code = generateCode(30);
    final int pc = code.length - 7; // start = pc+1, 6 bytes available
    staticOperation(frame, code, pc, 9);
    final byte[] expected = new byte[9];
    System.arraycopy(code, pc + 1, expected, 0, 6);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromBytesBE(expected));
  }

  // --- Stack interaction ---

  @Test
  void push9ThenPush32GivesCorrectStackOrder() {
    final byte[] code = generateCode(100);
    staticOperation(frame, code, 0, 9); // pushes code[1..9]
    staticOperation(frame, code, 10, 32); // pushes code[11..42]
    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, 11, 43)));
    assertThat(frame.getStackItemV2(1))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, 1, 10)));
  }

  @Test
  void pushOntoFullStackOverflows() {
    final byte[] code = generateCode(50);
    while (frame.stackHasSpaceV2(1)) {
      frame.setTopV2(frame.stackTopV2() + 1);
    }
    final int top = frame.stackTopV2();
    assertThrows(OverflowException.class, () -> staticOperation(frame, code, 0, 32));
    assertThat(frame.stackTopV2()).isEqualTo(top);
  }

  // --- Exhaustive: all pushSizes 9..32 at various pc positions ---

  @Test
  void exhaustivePushSizesAndPositions() {
    final byte[] code = new byte[90];
    for (int i = 0; i < code.length; i++) {
      code[i] = (byte) (i * 37 + 101); // varied bytes with high bits set
    }
    for (int pushSize = 9; pushSize <= 32; pushSize++) {
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
  }

  private static byte[] generateCode(final int numBytes) {
    final byte[] code = new byte[numBytes];
    for (int i = 0; i < code.length; i++) {
      code[i] = (byte) i;
    }
    return code;
  }
}
