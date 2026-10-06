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

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for EXCHANGE (EIP-8024). Equivalence with the original implementation for every
 * immediate and at the stack limits is covered by StackOperationsV2ComparisonTest.
 */
class ExchangeOperationV2Test {
  private final GasCalculator gasCalculator = new FrontierGasCalculator();
  private final ExchangeOperationV2 operation = new ExchangeOperationV2(gasCalculator);

  /** Spec vector: EXCHANGE 0x8e decodes to (n, m) = (1, 2), the 2nd and 3rd items. */
  @Test
  void exchange8e() {
    final MessageFrame frame = frame("e88e", 3);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isEqualTo(2);
    assertThat(frame.stackTopV2()).isEqualTo(3);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(word(1));
    assertThat(getV2StackItem(frame, 1)).isEqualTo(word(3));
    assertThat(getV2StackItem(frame, 2)).isEqualTo(word(2));
  }

  /** EXCHANGE 0x2f decodes to (n, m) = (1, 19): the deeper index needs 20 items. */
  @Test
  void exchange2f() {
    final MessageFrame frame = frame("e82f", 20);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    for (int depth = 1; depth <= 20; depth++) {
      final int expected = depth == 2 ? 20 : depth == 20 ? 2 : depth;
      assertThat(getV2StackItem(frame, depth - 1)).isEqualTo(word(expected));
    }
  }

  @Test
  void shouldHaltOnStackUnderflow() {
    final MessageFrame frame = frame("e82f", 19);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(frame.stackTopV2()).isEqualTo(19);
  }

  @Test
  void shouldHaltOnInvalidImmediate() {
    final MessageFrame frame = frame("e85b", 3);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(result.getPcIncrement()).isEqualTo(2);
    assertThat(frame.stackTopV2()).isEqualTo(3);
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .code(new Code(Bytes.fromHexString("e88e")))
            .initialGas(1)
            .build();
    frame.setTopV2(3);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(frame.stackTopV2()).isEqualTo(3);
  }

  @Test
  void gasCostIsVeryLowTier() {
    final Operation.OperationResult result = operation.execute(frame("e88e", 3), null);

    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getVeryLowTierGasCost());
  }

  /** A frame running the given code at pc 0, whose item at depth k (1 = top) is word(k). */
  private static MessageFrame frame(final String code, final int count) {
    final TestMessageFrameBuilderV2 builder =
        new TestMessageFrameBuilderV2().code(new Code(Bytes.fromHexString(code)));
    for (int depth = count; depth >= 1; depth--) {
      builder.pushStackItem(Bytes32.wrap(word(depth).toBytesBE()));
    }
    return builder.build();
  }

  /** A value with four distinct limbs, so any misplaced limb is detected. */
  private static UInt256 word(final int depth) {
    final long tag = (long) depth << 32;
    return new UInt256(tag | 0xa, tag | 0xb, tag | 0xc, tag | 0xd);
  }
}
