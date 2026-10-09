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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CallDataLoadOperationV2Test extends UnaryOperationV2Test {

  // 40 bytes, byte i being i + 1
  private static final Bytes CALL_DATA =
      Bytes.fromHexString(
          "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728");

  public CallDataLoadOperationV2Test() {
    super(new CallDataLoadOperationV2(new FrontierGasCalculator()));
  }

  @ParameterizedTest(name = "calldataload({0}) = {1}")
  @CsvSource({
    "0x00, 0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
    "0x08, 0x090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728",
    // past the end of the call data the word is padded with zeros
    "0x09, 0x0a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20212223242526272800",
    "0x27, 0x2800000000000000000000000000000000000000000000000000000000000000",
    "0x28, 0x00",
    "0x7fffffff, 0x00",
    "0x80000000, 0x00",
    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff, 0x00",
    // an offset whose least significant limb alone would be in range
    "0x0100000000000000000000000000000000000000000000000000000000000000, 0x00"
  })
  void loadsAWordOfCallData(final String offset, final String expected) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .inputData(CALL_DATA)
            .pushStackItem(Bytes32.fromHexString(offset))
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
            .inputData(CALL_DATA)
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(new FrontierGasCalculator().getVeryLowTierGasCost());
  }
}
