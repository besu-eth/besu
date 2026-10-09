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
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CountLeadingZerosOperationV2Test extends UnaryOperationV2Test {

  public CountLeadingZerosOperationV2Test() {
    super(new CountLeadingZerosOperationV2(new OsakaGasCalculator()));
  }

  @ParameterizedTest(name = "clz({0}) = {1}")
  @CsvSource({
    "0x00, 256",
    "0x01, 255",
    "0xff, 248",
    "0x8000000000000000, 192",
    "0x010000000000000000, 191",
    "0x80000000000000000000000000000000, 128",
    "0x0100000000000000000000000000000000, 127",
    "0x800000000000000000000000000000000000000000000000, 64",
    "0x01000000000000000000000000000000000000000000000000, 63",
    "0x0000000000000001000000000000000000000000000000000000000000000000, 63",
    "0x4000000000000000000000000000000000000000000000000000000000000000, 1",
    "0x8000000000000000000000000000000000000000000000000000000000000000, 0",
    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff, 0"
  })
  void countLeadingZeros(final String value, final long expected) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().pushStackItem(Bytes32.fromHexString(value)).build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.fromLong(expected));
  }

  @Test
  void gasCostIsLowTier() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().pushStackItem(Bytes32.fromHexString("0x01")).build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(new OsakaGasCalculator().getLowTierGasCost());
  }
}
