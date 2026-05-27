/*
 * Copyright contributors to Hyperledger Besu.
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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;

import java.util.Arrays;
import java.util.Collection;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MCopyOperationTest {

  @Mock EVM evm;

  static Collection<Object[]> mcopyTestVector() {
    return Arrays.asList(
        new Object[][] {
          {
            "Copy after, no overlap",
            Bytes.fromHexString("0123456789abcdef000000000000000000000000000000000000000000000000"),
            32,
            0,
            8,
            Bytes.fromHexString(
                "0123456789abcdef0000000000000000000000000000000000000000000000000123456789abcdef"),
            9L
          },
          {
            "copy from uninitialized memory",
            Bytes.EMPTY,
            0,
            24,
            16,
            Bytes.fromHexString(
                "0x000000000000000000000000000000000000000000000000000000000000000000"),
            12L
          },
          {
            "copy from initialized + uninitialized memory",
            Bytes.fromHexString(
                "0x0000000000000000000000000000000000000000000000000123456789abcdef"),
            64,
            24,
            16,
            Bytes.fromHexString(
                "0x0000000000000000000000000000000000000000000000000123456789abcdef00000000000000000000000000000000000000000000000000000000000000000123456789abcdef000000000000000000000000000000000000000000000000"),
            12L
          },
          {
            "overlapping src < dst",
            Bytes.fromHexString(
                "0x0123456789abcdef000000000000000000000000000000000000000000000000"),
            4,
            0,
            8,
            Bytes.fromHexString(
                "0x012345670123456789abcdef0000000000000000000000000000000000000000"),
            6L
          },
          {
            "overlapping src > dst",
            Bytes.fromHexString(
                "0x00112233445566778899aabbccddeeff00000000000000000000000000000000"),
            0,
            4,
            8,
            Bytes.fromHexString(
                "0x445566778899aabb8899aabbccddeeff00000000000000000000000000000000"),
            6L
          },
          {
            "overlapping src == dst",
            Bytes.fromHexString(
                "0x00112233445566778899aabbccddeeff00000000000000000000000000000000"),
            4,
            4,
            8,
            Bytes.fromHexString(
                "0x00112233445566778899aabbccddeeff00000000000000000000000000000000"),
            6L
          }
        });
  }

  @SuppressWarnings("unused")
  @ParameterizedTest(name = "{0}")
  @MethodSource("mcopyTestVector")
  void testMCopy(
      final String name,
      final Bytes memory,
      final long dst,
      final long src,
      final long len,
      final Bytes expected,
      final long gasCost) {
    MCopyOperation subject = new MCopyOperation(new CancunGasCalculator());
    MessageFrame frame =
        new TestMessageFrameBuilder()
            .pushStackItem(Bytes.ofUnsignedLong(len))
            .pushStackItem(Bytes.ofUnsignedLong(src))
            .pushStackItem(Bytes.ofUnsignedLong(dst))
            .memory(memory)
            .build();

    Operation.OperationResult result = subject.execute(frame, evm);

    assertThat(frame.memoryWordSize()).isEqualTo((expected.size() + 31) / 32);
    assertThat(frame.readMemory(0, expected.size())).isEqualTo(expected);
    assertThat(result.getGasCost()).isEqualTo(gasCost);
  }
}
