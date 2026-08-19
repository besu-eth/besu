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
import static org.hyperledger.besu.evm.v2.operation.PushOperationV2.staticOperation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PushOperationV2Test {
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

  @Test
  void unpaddedPushDoesntReachEndCode() {
    final byte[] code = generateCode(4);
    staticOperation(frame, code, 0, code.length - 2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0x0102));
  }

  @Test
  void unpaddedPushUpReachesEndCode() {
    final byte[] code = generateCode(4);
    staticOperation(frame, code, 0, code.length - 1);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0x010203));
  }

  @Test
  void paddedPush() {
    final byte[] code = generateCode(4);
    staticOperation(frame, code, 1, code.length - 1);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0x020300));
  }

  @Test
  void oobPush() {
    final byte[] code = generateCode(4);
    staticOperation(frame, code, code.length - 1, code.length - 1);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void pushOntoNonEmptyStack() {
    final byte[] code = generateCode(4);
    staticOperation(frame, code, 0, 2);
    staticOperation(frame, code, 1, 2);
    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromLong(0x0203));
    assertThat(frame.getStackItemV2(1)).isEqualTo(UInt256.fromLong(0x0102));
  }

  @Test
  void push32InMiddleOfLongCode() {
    final byte[] code = generateCode(100);
    final int pc = 10;
    staticOperation(frame, code, pc, 32);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, pc + 1, pc + 1 + 32)));
  }

  @Test
  void push8InMiddleOfLongCode() {
    final byte[] code = generateCode(100);
    final int pc = 10;
    staticOperation(frame, code, pc, 8);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, pc + 1, pc + 1 + 8)));
  }

  @Test
  void push9InMiddleOfLongCode() {
    final byte[] code = generateCode(100);
    final int pc = 10;
    staticOperation(frame, code, pc, 9);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, pc + 1, pc + 1 + 9)));
  }

  @Test
  void push20InMiddleOfLongCode() {
    final byte[] code = generateCode(100);
    final int pc = 10;
    staticOperation(frame, code, pc, 20);
    assertThat(frame.getStackItemV2(0))
        .isEqualTo(UInt256.fromBytesBE(Arrays.copyOfRange(code, pc + 1, pc + 1 + 20)));
  }

  @Test
  void paddedPush32NearEndOfLongCode() {
    final byte[] code = generateCode(100);
    final int pc = code.length - 21;
    staticOperation(frame, code, pc, 32);
    final byte[] expected = new byte[32];
    System.arraycopy(code, pc + 1, expected, 0, 20);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromBytesBE(expected));
  }

  @Test
  void pushOntoFullStackOverflows() {
    final byte[] code = generateCode(4);
    while (frame.stackHasSpaceV2(1)) {
      frame.setTopV2(frame.stackTopV2() + 1);
    }
    final int top = frame.stackTopV2();
    final var result = staticOperation(frame, code, 0, 1);
    assertThat(result.getHaltReason())
        .isEqualTo(org.hyperledger.besu.evm.frame.ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
    assertThat(frame.stackTopV2()).isEqualTo(top);
  }

  @Test
  void exhaustivePushSizesAndPositions() {
    // varied bytes with high bits set, to catch sign-extension mistakes
    final byte[] code = new byte[90];
    for (int i = 0; i < code.length; i++) {
      code[i] = (byte) (i * 37 + 101);
    }
    for (int pushSize = 1; pushSize <= 32; pushSize++) {
      for (int pc = 0; pc < code.length; pc++) {
        frame.setTopV2(0);
        staticOperation(frame, code, pc, pushSize);
        // reference: copy available bytes, right-pad with zeros to pushSize
        final byte[] expected = new byte[pushSize];
        final int available = Math.max(0, Math.min(pushSize, code.length - pc - 1));
        System.arraycopy(code, Math.min(pc + 1, code.length), expected, 0, available);
        assertThat(frame.getStackItemV2(0))
            .as("pushSize=%s pc=%s", pushSize, pc)
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
