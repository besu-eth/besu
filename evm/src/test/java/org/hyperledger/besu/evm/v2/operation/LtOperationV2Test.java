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

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class LtOperationV2Test extends BinaryOperationV2Test {

  public LtOperationV2Test() {
    super(new LtOperationV2(new FrontierGasCalculator()));
  }

  /** (a, b, lt(a, b), case); a is on top of the stack. */
  static Iterable<Arguments> data() {
    return List.of(
        Arguments.of("0x01", "0x02", "0x01", "smaller"),
        Arguments.of("0x02", "0x01", "0x00", "larger"),
        Arguments.of("0x05", "0x05", "0x00", "equal"),
        Arguments.of(
            "0x0123456789abcdeffedcba98765432100f0f0f0f0f0f0f0ff0f0f0f0f0f0f0f0",
            "0x0123456789abcdeffedcba98765432100f0f0f0f0f0f0f0ff0f0f0f0f0f0f0f1",
            "0x01",
            "differs in the least significant limb only"),
        Arguments.of(
            "0x0123456789abcdf0fedcba98765432100f0f0f0f0f0f0f0ff0f0f0f0f0f0f0f0",
            "0x0123456789abcdeffedcba98765432100f0f0f0f0f0f0f0ff0f0f0f0f0f0f0f1",
            "0x00",
            "the most significant limb decides"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000000",
            "0x01",
            "0x00",
            "unsigned, so the top bit makes a value large"),
        Arguments.of(
            "0x00",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x01",
            "zero against the largest value"));
  }

  @ParameterizedTest(name = "{index}: {3}")
  @MethodSource("data")
  void ltOperation(
      final String a, final String b, final String expected, final String description) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(b))
            .pushStackItem(Bytes32.fromHexString(a))
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
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .pushStackItem(Bytes32.fromHexString("0x02"))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(new FrontierGasCalculator().getVeryLowTierGasCost());
  }
}
