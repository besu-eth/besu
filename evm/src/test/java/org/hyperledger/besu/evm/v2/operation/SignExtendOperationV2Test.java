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

class SignExtendOperationV2Test extends BinaryOperationV2Test {

  public SignExtendOperationV2Test() {
    super(new SignExtendOperationV2(new FrontierGasCalculator()));
  }

  @ParameterizedTest(name = "signextend({0}, {1}) = {2}")
  @CsvSource({
    // byte 0, positive and negative
    "0x00, 0x7f, 0x7f",
    "0x00, 0x80, 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff80",
    "0x00, 0x1234567880, 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff80",
    "0x00, 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f12, 0x12",
    // the sign bit is the top bit of a limb
    "0x07, 0x8000000000000000, 0xffffffffffffffffffffffffffffffffffffffffffffffff8000000000000000",
    "0x07, 0xff7fffffffffffffff, 0x7fffffffffffffff",
    // the sign bit is the bottom bit of a byte in the next limb
    "0x08, 0x800000000000000000, 0xffffffffffffffffffffffffffffffffffffffffffffff800000000000000000",
    "0x0f, 0x80000000000000000000000000000000, 0xffffffffffffffffffffffffffffffff80000000000000000000000000000000",
    "0x10, 0x7f00000000000000000000000000000000, 0x7f00000000000000000000000000000000",
    "0x17, 0x800000000000000000000000000000000000000000000000, 0xffffffffffffffff800000000000000000000000000000000000000000000000",
    "0x1e, 0x00800000000000000000000000000000000000000000000000000000000000ab, 0xff800000000000000000000000000000000000000000000000000000000000ab",
    "0x1e, 0xff7f0000000000000000000000000000000000000000000000000000000000ab, 0x007f0000000000000000000000000000000000000000000000000000000000ab",
    // from byte 31 on the sign is already in place
    "0x1f, 0x8000000000000000000000000000000000000000000000000000000000000001, 0x8000000000000000000000000000000000000000000000000000000000000001",
    "0xff, 0x80, 0x80",
    "0x0100000000000000000000000000000000000000000000000000000000000000, 0x80, 0x80"
  })
  void signExtendOperation(final String byteIndex, final String value, final String expected) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(value))
            .pushStackItem(Bytes32.fromHexString(byteIndex))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(UInt256.fromBytesBE(Bytes32.fromHexString(expected).toArrayUnsafe()));
  }

  @Test
  void gasCostIsLowTier() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x80"))
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(new FrontierGasCalculator().getLowTierGasCost());
  }
}
