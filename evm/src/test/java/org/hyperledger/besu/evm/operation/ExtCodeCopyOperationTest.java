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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Arrays;
import java.util.Collection;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExtCodeCopyOperationTest {

  private static final Address REQUESTED_ADDRESS = Address.fromHexString("0x22222222");

  private final ToyWorld toyWorld = new ToyWorld();
  private final WorldUpdater worldStateUpdater = toyWorld.updater();

  @Mock EVM evm;

  static Collection<Object[]> extCodeCopyTestVector() {
    return Arrays.asList(
        new Object[][] {
          {
            "Copy after, no overlap",
            Bytes.fromHexString("0123456789abcdef000000000000000000000000000000000000000000000000"),
            32,
            0,
            8,
            Bytes.fromHexString(
                "00000000000000000000000000000000000000000000000000000000000000000123456789abcdef"),
            2609L
          },
          {
            "copy from uninitialized memory",
            Bytes.EMPTY,
            0,
            24,
            16,
            Bytes.fromHexString(
                "0x000000000000000000000000000000000000000000000000000000000000000000"),
            2606L
          },
          {
            "copy from initialized + uninitialized memory",
            Bytes.fromHexString(
                "0x0000000000000000000000000000000000000000000000000123456789abcdef"),
            64,
            24,
            16,
            Bytes.fromHexString(
                "0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000123456789abcdef000000000000000000000000000000000000000000000000"),
            2612L
          },
          {
            "overlapping src < dst",
            Bytes.fromHexString(
                "0x0123456789abcdef000000000000000000000000000000000000000000000000"),
            4,
            0,
            8,
            Bytes.fromHexString(
                "0x000000000123456789abcdef0000000000000000000000000000000000000000"),
            2606L
          },
          {
            "overlapping src > dst",
            Bytes.fromHexString(
                "0x00112233445566778899aabbccddeeff00000000000000000000000000000000"),
            0,
            4,
            8,
            Bytes.fromHexString(
                "0x445566778899aabb000000000000000000000000000000000000000000000000"),
            2606L
          },
          {
            "overlapping src == dst",
            Bytes.fromHexString(
                "0x00112233445566778899aabbccddeeff00000000000000000000000000000000"),
            4,
            4,
            8,
            Bytes.fromHexString(
                "0x00000000445566778899aabb0000000000000000000000000000000000000000"),
            2606L
          },
          {
            "Code starting with 0xEF00",
            Bytes.fromHexString("0xEF009f918bf09f9fa9"),
            0,
            0,
            9,
            Bytes.fromHexString("0xEF009f918bf09f9fa9"),
            2606L
          },
          {
            "Code starting with 0xEFF0",
            Bytes.fromHexString("0xEFF09f918bf09f9fa9"),
            0,
            0,
            9,
            Bytes.fromHexString("0xEFF09f918bf09f9fa9"),
            2606L
          }
        });
  }

  @SuppressWarnings("unused")
  @ParameterizedTest(name = "{0}")
  @MethodSource("extCodeCopyTestVector")
  void testExtCodeCopy(
      final String name,
      final Bytes code,
      final long dst,
      final long src,
      final long len,
      final Bytes expected,
      final long gasCost) {
    final MutableAccount account = worldStateUpdater.getOrCreate(REQUESTED_ADDRESS);
    account.setCode(code);

    ExtCodeCopyOperation subject = new ExtCodeCopyOperation(new PragueGasCalculator());
    MessageFrame frame =
        new TestMessageFrameBuilder()
            .worldUpdater(worldStateUpdater)
            .pushStackItem(Bytes.ofUnsignedLong(len))
            .pushStackItem(Bytes.ofUnsignedLong(src))
            .pushStackItem(Bytes.ofUnsignedLong(dst))
            .pushStackItem(REQUESTED_ADDRESS.getBytes())
            .build();

    Operation.OperationResult result = subject.execute(frame, evm);

    assertThat(frame.readMemory(0, expected.size())).isEqualTo(expected);
    assertThat(frame.memoryWordSize()).isEqualTo((expected.size() + 31) / 32);
    assertThat(result.getGasCost()).isEqualTo(gasCost);
  }

  @Test
  void testExtCodeCopyCold() {
    final MutableAccount account = worldStateUpdater.getOrCreate(REQUESTED_ADDRESS);
    Bytes code = Bytes.fromHexString("0xEFF09f918bf09f9fa9");
    account.setCode(code);

    ExtCodeCopyOperation subject = new ExtCodeCopyOperation(new PragueGasCalculator());
    MessageFrame frame =
        new TestMessageFrameBuilder()
            .worldUpdater(worldStateUpdater)
            .pushStackItem(Bytes.ofUnsignedLong(9))
            .pushStackItem(Bytes.ofUnsignedLong(0))
            .pushStackItem(Bytes.ofUnsignedLong(0))
            .pushStackItem(REQUESTED_ADDRESS.getBytes())
            .build();
    frame.warmUpAddress(REQUESTED_ADDRESS);

    Operation.OperationResult result = subject.execute(frame, evm);

    assertThat(frame.readMemory(0, 9)).isEqualTo(code);
    assertThat(frame.memoryWordSize()).isEqualTo(1);
    assertThat(result.getGasCost()).isEqualTo(106);
    assertThat(frame.getMaybeUpdatedMemory()).isPresent();
    assertThat(frame.getMaybeUpdatedMemory().get().getOffset()).isZero();
    assertThat(frame.getMaybeUpdatedMemory().get().getValue()).isEqualTo(code);
  }
}
