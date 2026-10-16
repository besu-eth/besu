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
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.SpuriousDragonGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.math.BigInteger;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ExpOperationV2Test extends BinaryOperationV2Test {

  private static final GasCalculator GAS_CALCULATOR = new SpuriousDragonGasCalculator();
  private static final BigInteger MODULUS = BigInteger.TWO.pow(256);

  public ExpOperationV2Test() {
    super(new ExpOperationV2(GAS_CALCULATOR));
  }

  @ParameterizedTest(name = "{0} ** {1} = {2}")
  @CsvSource({
    "0x02, 0x0a, 0x0400",
    "0x00, 0x00, 0x01",
    "0x00, 0x05, 0x00",
    "0x07, 0x00, 0x01",
    "0x01, 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff, 0x01",
    "0x02, 0xff, 0x8000000000000000000000000000000000000000000000000000000000000000",
    // wraps modulo 2^256
    "0x02, 0x0100, 0x00",
    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff, 0x02, 0x01",
    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff, 0x03, 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
  })
  void expOperation(final String base, final String exponent, final String expected) {
    final MessageFrame frame = frame(base, exponent);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(UInt256.fromBytesBE(Bytes32.fromHexString(expected).toArrayUnsafe()));
  }

  @Test
  void matchesBigIntegerModPow() {
    final Random random = new Random(42);
    for (int i = 0; i < 500; i++) {
      final byte[] base = new byte[1 + random.nextInt(32)];
      final byte[] exponent = new byte[1 + random.nextInt(32)];
      random.nextBytes(base);
      random.nextBytes(exponent);
      final BigInteger expected =
          new BigInteger(1, base).modPow(new BigInteger(1, exponent), MODULUS);

      final MessageFrame frame =
          new TestMessageFrameBuilderV2()
              .pushStackItem(Bytes32.leftPad(org.apache.tuweni.bytes.Bytes.wrap(exponent)))
              .pushStackItem(Bytes32.leftPad(org.apache.tuweni.bytes.Bytes.wrap(base)))
              .build();
      operation.execute(frame, null);

      assertThat(getV2StackItem(frame, 0).toBigInteger()).isEqualTo(expected);
    }
  }

  @ParameterizedTest(name = "exponent {0} costs {1}")
  @CsvSource({
    "0x00, 10",
    "0x01, 60",
    "0xff, 60",
    "0x0100, 110",
    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff, 1610"
  })
  void chargesForEveryByteOfTheExponent(final String exponent, final long expectedCost) {
    final MessageFrame frame = frame("0x03", exponent);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(expectedCost);
  }

  @Test
  void haltsWhenTheGasDoesNotCoverTheExponent() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x0100"))
            .pushStackItem(Bytes32.fromHexString("0x03"))
            .initialGas(109)
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(result.getGasCost()).isEqualTo(110);
  }

  private static MessageFrame frame(final String base, final String exponent) {
    return new TestMessageFrameBuilderV2()
        .pushStackItem(Bytes32.fromHexString(exponent))
        .pushStackItem(Bytes32.fromHexString(base))
        .build();
  }
}
