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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class EqOperationTest {
  private EqOperation operation;

  private enum Condition {
    NOT_EQUAL,
    EQUAL
  }

  @BeforeEach
  void setUp() {
    operation = new EqOperation(mock(GasCalculator.class));
  }

  static Collection<Object[]> provideEqTestCases() {
    return Arrays.stream(
            new Object[][] {
              {"0x", "0x", Condition.EQUAL},
              {"0x000001", "0x01", Condition.EQUAL},
              {"0x00", "0x01", Condition.NOT_EQUAL},
              {"0x0002", "0x0003", Condition.NOT_EQUAL},
              {"0x000001", "0x", Condition.NOT_EQUAL},
              {"0x000000", "0x", Condition.EQUAL},
              {"0x321312", "0x321312", Condition.EQUAL},
              {"0x321312", "0x0000321312", Condition.EQUAL},
              {"0x00321312", "0x000000321312", Condition.EQUAL},
              {
                "0x3213124332321312433232131243323213124332321312433232131243324332",
                "0x3213124332321312433232131243323213124332321312433232131243324332",
                Condition.EQUAL
              },
              {
                "0x2313124332321312433232131243323213124332321312433232131243324332",
                "0x3213124332321312433232131243323213124332321312433232131243324332",
                Condition.NOT_EQUAL
              },
              {
                "0x3213124332321312433232131243123213124332321312433232131243324332",
                "0x3213124332321312433232131243323213124332321312433232131243324332",
                Condition.NOT_EQUAL
              },
              {"0x321312", "0x320000321312", Condition.NOT_EQUAL},
              {"0x00321312", "0x03213120", Condition.NOT_EQUAL}
            })
        .flatMap(
            inputs -> {
              if (inputs[0].equals(inputs[1])) {
                return Stream.<Object[]>of(inputs);
              }
              Object[] invert = Arrays.copyOf(inputs, inputs.length);
              invert[0] = inputs[1];
              invert[1] = inputs[0];
              return Stream.of(inputs, invert);
            })
        .collect(Collectors.toList());
  }

  @ParameterizedTest
  @MethodSource("provideEqTestCases")
  void testEqOperation(final String value0, final String value1, final Condition expectedResult) {
    MessageFrame frame =
        new TestMessageFrameBuilder()
            .pushStackItem(Bytes.fromHexString(value1))
            .pushStackItem(Bytes.fromHexString(value0))
            .build();

    operation.executeFixedCostOperation(frame);

    assertThat(UInt256.valueOf(expectedResult.ordinal())).isEqualTo(frame.popStackItem());
  }
}
