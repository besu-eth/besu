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
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CountLeadingZerosOperationV2Test extends UnaryOperationV2Test {
  private final GasCalculator gasCalculator = new OsakaGasCalculator();

  public CountLeadingZerosOperationV2Test() {
    super(new CountLeadingZerosOperationV2(new OsakaGasCalculator()));
  }

  /** Test data for CLZ(a) = expected, including the EIP-7939 vectors. */
  static Iterable<Arguments> data() {
    return List.of(
        // (a, expected)
        Arguments.of("0x00", 256),
        Arguments.of("0x8000000000000000000000000000000000000000000000000000000000000000", 0),
        Arguments.of("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 0),
        Arguments.of("0x4000000000000000000000000000000000000000000000000000000000000000", 1),
        Arguments.of("0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 1),
        Arguments.of("0x01", 255),
        Arguments.of("0xff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff", 8),
        // Highest bit of each lower limb: catches limb selection and offset mistakes.
        Arguments.of("0x800000000000000000000000000000000000000000000000", 64),
        Arguments.of("0x80000000000000000000000000000000", 128),
        Arguments.of("0x8000000000000000", 192),
        // A lower limb set as well must not change the count.
        Arguments.of("0x010000000000000000000000000000000000000000000000ffffffffffffffff", 7));
  }

  @ParameterizedTest(name = "{index}: clz({0}) = {1}")
  @MethodSource("data")
  void clzOperation(final String a, final int expectedLeadingZeros) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().pushStackItem(Bytes32.fromHexString(a)).build();
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // CLZ consumes 1 item and produces 1: the stack size is unchanged.
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.fromInt(expectedLeadingZeros));
  }

  @Test
  void gasCostIsLowTier() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().pushStackItem(Bytes32.fromHexString("0x01")).build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getLowTierGasCost());
  }
}
