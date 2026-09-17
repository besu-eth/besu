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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;

import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CountLeadingZerosOperationTest {

  private CountLeadingZerosOperation operation;
  private MessageFrame frame;

  @BeforeEach
  void setUp() {
    operation = new CountLeadingZerosOperation(mock(GasCalculator.class));
    frame = mock(MessageFrame.class);
  }

  static Stream<Arguments> provideClzTestCases() {
    return Stream.of(
        Arguments.of("0x0000000000000000000000000000000000000000000000000000000000000000", 256),
        Arguments.of("0x8000000000000000000000000000000000000000000000000000000000000000", 0),
        Arguments.of("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 0),
        Arguments.of("0x4000000000000000000000000000000000000000000000000000000000000000", 1),
        Arguments.of("0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 1),
        Arguments.of(
            "0x00ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f", 0),
        Arguments.of("0x01", 255),
        Arguments.of("0xff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff", 8));
  }

  @ParameterizedTest
  @MethodSource("provideClzTestCases")
  void testClzOperation(final String value, final int expectedLeadingZeros) {
    Bytes input = Bytes.fromHexString(value);
    Bytes expected = Words.intBytes(expectedLeadingZeros);

    when(frame.popStackItem()).thenReturn(input);

    operation.executeFixedCostOperation(frame);
    verify(frame).pushStackItem(expected);
  }
}
