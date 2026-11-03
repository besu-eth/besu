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
package org.hyperledger.besu.evm.v2.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2.getV2StackItem;

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ByteOperationV2Test extends BinaryOperationV2Test {

  // byte i of this word is 0xa0 + i, so every byte position reads back a different value
  private static final String WORD =
      "0xa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf";

  public ByteOperationV2Test() {
    super(new ByteOperationV2(new FrontierGasCalculator()));
  }

  @ParameterizedTest(name = "byte {0} = {1}")
  @CsvSource({
    "0x00, 0xa0",
    "0x07, 0xa7",
    "0x08, 0xa8",
    "0x0f, 0xaf",
    "0x10, 0xb0",
    "0x17, 0xb7",
    "0x18, 0xb8",
    "0x1f, 0xbf",
    // from index 32 on there is no such byte
    "0x20, 0x00",
    "0xff, 0x00",
    "0x0100000000000000000000000000000000000000000000000000000000000000, 0x00",
    // an index whose least significant limb alone would be in range
    "0x0000000000000000000000000000000100000000000000000000000000000003, 0x00"
  })
  void byteOperation(final String index, final String expected) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(WORD))
            .pushStackItem(Bytes32.fromHexString(index))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(UInt256.fromBytesBE(Bytes32.fromHexString(expected).toArrayUnsafe()));
  }

  @Test
  void gasCostIsVeryLowTier() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(WORD))
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(new FrontierGasCalculator().getVeryLowTierGasCost());
  }
}
