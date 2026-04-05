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
package org.hyperledger.besu.evm.frame;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for EVM v2 tracing compatibility — getStackItem() and stackSize() with long[] stack. */
class MessageFrameV2TracingTest {

  private MessageFrame frame;

  @BeforeEach
  void setUp() {
    frame =
        MessageFrame.builder()
            .enableEvmV2(true)
            .worldUpdater(new ToyWorld())
            .originator(Address.ZERO)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(new ToyBlockValues())
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(Long.MAX_VALUE)
            .address(Address.ZERO)
            .contract(Address.ZERO)
            .inputData(Bytes32.ZERO)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(Code.EMPTY_CODE)
            .completer(__ -> {})
            .build();
  }

  @Test
  void stackSizeReturnsZeroForEmptyV2Stack() {
    assertThat(frame.stackSize()).isZero();
  }

  @Test
  void stackSizeReflectsV2StackTop() {
    pushLongs(0L, 0L, 0L, 42L);
    assertThat(frame.stackSize()).isEqualTo(1);

    pushLongs(0L, 0L, 0L, 99L);
    assertThat(frame.stackSize()).isEqualTo(2);
  }

  @Test
  void getStackItemReturnsCorrectBytesForSimpleValue() {
    // Push value 5 onto v2 stack
    pushLongs(0L, 0L, 0L, 5L);

    Bytes result = frame.getStackItem(0);
    assertThat(result).isEqualTo(Bytes32.fromHexStringLenient("0x05"));
  }

  @Test
  void getStackItemReturnsCorrectBytesForLargeValue() {
    // Push 0xFFFFFFFFFFFFFFFF_0000000000000001_0000000000000002_0000000000000003
    pushLongs(0xFFFFFFFFFFFFFFFFL, 1L, 2L, 3L);

    Bytes result = frame.getStackItem(0);
    // u3=FFFFFFFFFFFFFFFF, u2=0000000000000001, u1=0000000000000002, u0=0000000000000003
    assertThat(result)
        .isEqualTo(
            Bytes32.fromHexString(
                "0xFFFFFFFFFFFFFFFF000000000000000100000000000000020000000000000003"));
  }

  @Test
  void getStackItemReturnsCorrectBytesForZero() {
    pushLongs(0L, 0L, 0L, 0L);

    Bytes result = frame.getStackItem(0);
    assertThat(result).isEqualTo(Bytes32.ZERO);
  }

  @Test
  void getStackItemReturnsCorrectBytesForMaxValue() {
    // All bits set = MAX_UINT256
    pushLongs(-1L, -1L, -1L, -1L);

    Bytes result = frame.getStackItem(0);
    byte[] expected = new byte[32];
    java.util.Arrays.fill(expected, (byte) 0xFF);
    assertThat(result).isEqualTo(Bytes32.wrap(expected));
  }

  @Test
  void getStackItemWithOffsetReturnsCorrectValues() {
    // Push two values: first=10, second=20
    pushLongs(0L, 0L, 0L, 10L);
    pushLongs(0L, 0L, 0L, 20L);

    // offset 0 = top of stack = 20
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes32.fromHexStringLenient("0x14"));
    // offset 1 = second from top = 10
    assertThat(frame.getStackItem(1)).isEqualTo(Bytes32.fromHexStringLenient("0x0a"));
  }

  @Test
  void v1StackSizeStillWorksWhenV2Disabled() {
    // Create a v1 frame (no enableEvmV2)
    MessageFrame v1Frame =
        MessageFrame.builder()
            .worldUpdater(new ToyWorld())
            .originator(Address.ZERO)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(new ToyBlockValues())
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(1)
            .address(Address.ZERO)
            .contract(Address.ZERO)
            .inputData(Bytes32.ZERO)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(Code.EMPTY_CODE)
            .completer(__ -> {})
            .build();

    assertThat(v1Frame.stackSize()).isZero();
    v1Frame.pushStackItem(Bytes32.ZERO);
    assertThat(v1Frame.stackSize()).isEqualTo(1);
    assertThat(v1Frame.getStackItem(0)).isEqualTo(Bytes32.ZERO);
  }

  /** Helper to push 4 longs onto the v2 stack. */
  private void pushLongs(final long u3, final long u2, final long u1, final long u0) {
    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int idx = top * 4;
    s[idx] = u3;
    s[idx + 1] = u2;
    s[idx + 2] = u1;
    s[idx + 3] = u0;
    frame.setTopV2(top + 1);
  }
}
