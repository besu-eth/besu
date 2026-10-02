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
import static org.hyperledger.besu.evm.v2.operation.PushOperationV2.SingleByte.staticOperation;
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

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PushOperationV2.SingleByte}, covering PUSH0 (pushSize=0) and PUSH1 (pushSize=1).
 */
public class PushOperationSingleByteV2Test {

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

  // --- PUSH0 tests (pushSize = 0) ---

  @Test
  void push0AlwaysPushesZero() {
    final byte[] code = {0x01, 0x02, 0x03};
    staticOperation(frame, code, 0, 0);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void push0OnEmptyCodePushesZero() {
    staticOperation(frame, new byte[0], 0, 0);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void push0TwiceGivesTwoZeros() {
    final byte[] code = new byte[4];
    staticOperation(frame, code, 0, 0);
    staticOperation(frame, code, 0, 0);
    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
    assertThat(frame.getStackItemV2(1)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void push0OntoFullStackOverflows() {
    final byte[] code = new byte[2];
    while (frame.stackHasSpaceV2(1)) {
      frame.setTopV2(frame.stackTopV2() + 1);
    }
    final int top = frame.stackTopV2();
    assertThrows(OverflowException.class, () -> staticOperation(frame, code, 0, 0));
    assertThat(frame.stackTopV2()).isEqualTo(top);
  }

  // --- PUSH1 tests (pushSize = 1) ---

  @Test
  void push1NormalByte() {
    final byte[] code = {0x00, 0x42};
    staticOperation(frame, code, 0, 1);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0x42));
  }

  @Test
  void push1ZeroByte() {
    final byte[] code = {0x00, 0x00};
    staticOperation(frame, code, 0, 1);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void push1MaxByte0xFF_noSignExtension() {
    // 0xFF as signed byte is -1; must be treated as unsigned 255
    final byte[] code = {0x00, (byte) 0xFF};
    staticOperation(frame, code, 0, 1);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(255L));
  }

  @Test
  void push1HighBit0x80_noSignExtension() {
    final byte[] code = {0x00, (byte) 0x80};
    staticOperation(frame, code, 0, 1);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(128L));
  }

  @Test
  void push1AtNonZeroPc() {
    final byte[] code = {0x00, 0x00, 0x00, 0x7E};
    staticOperation(frame, code, 2, 1);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0x7E));
  }

  @Test
  void push1OobStartEqualsCodeLength_pushesZero() {
    // pc + 1 == code.length: start is out of bounds
    final byte[] code = {0x00, 0x42};
    staticOperation(frame, code, 1, 1); // start = 2 == code.length
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void push1OobStartBeyondCodeLength_pushesZero() {
    final byte[] code = {0x00, 0x42};
    staticOperation(frame, code, 5, 1); // start = 6, well past end
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void push1LastValidByte() {
    // start == code.length - 1: exactly the last byte
    final byte[] code = {0x00, 0x00, (byte) 0xAB};
    staticOperation(frame, code, 1, 1); // start = 2, code.length - 1 = 2
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0xABL));
  }

  @Test
  void push1ThenPush0GivesCorrectStackOrder() {
    final byte[] code = {0x00, 0x55};
    staticOperation(frame, code, 0, 1); // pushes 0x55
    staticOperation(frame, code, 0, 0); // pushes 0
    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
    assertThat(frame.getStackItemV2(1)).isEqualTo(UInt256.fromLong(0x55));
  }

  @Test
  void push1OntoNonEmptyStack() {
    final byte[] code = {0x00, 0x11, 0x00, 0x22};
    staticOperation(frame, code, 0, 1); // pushes 0x11
    staticOperation(frame, code, 2, 1); // pushes 0x22
    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0x22));
    assertThat(frame.getStackItemV2(1)).isEqualTo(UInt256.fromLong(0x11));
  }

  @Test
  void push1OntoFullStackOverflows() {
    final byte[] code = {0x00, 0x01};
    while (frame.stackHasSpaceV2(1)) {
      frame.setTopV2(frame.stackTopV2() + 1);
    }
    final int top = frame.stackTopV2();
    assertThrows(OverflowException.class, () -> staticOperation(frame, code, 0, 1));
    assertThat(frame.stackTopV2()).isEqualTo(top);
  }
}
