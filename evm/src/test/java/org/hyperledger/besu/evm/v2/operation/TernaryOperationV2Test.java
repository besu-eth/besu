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
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.List;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

abstract class TernaryOperationV2Test {

  protected final Operation operation;

  public TernaryOperationV2Test(final Operation operation) {
    this.operation = operation;
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.ZERO)
            .pushStackItem(Bytes32.ZERO)
            .pushStackItem(Bytes32.ZERO)
            .initialGas(1)
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(3);
    final Operation.OperationResult result = operation.execute(frame, null);
    assertThat(frame.stackTopV2()).isEqualTo(3);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  private static Stream<List<String>> operandStacks() {
    return Stream.of(List.of(), List.of("0x01"), List.of("0x02"));
  }

  @ParameterizedTest(name = "stack {0}")
  @MethodSource("operandStacks")
  void shouldHaltOnStackUnderflow(final List<String> stackItems) {
    final TestMessageFrameBuilderV2 frameBuilder = new TestMessageFrameBuilderV2();
    for (String stackItem : stackItems) {
      frameBuilder.pushStackItem(Bytes32.fromHexString(stackItem));
    }
    final MessageFrame frame = frameBuilder.build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  @Test
  void shouldPreserveDeeperStackItems() {
    // Use a distinctive 4-limb value for the untouched deep slot so any accidental write is
    // detectable in any limb.
    final Bytes32 untouched =
        Bytes32.fromHexString("0xdeadbeefdeadbeefcafebabecafebabe0123456789abcdeff1e2d3c4b5a69788");
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(untouched) // top-4 (untouched by operation)
            .pushStackItem(Bytes32.fromHexString("0x09")) // top-3 (c)
            .pushStackItem(Bytes32.fromHexString("0x07")) // top-2 (b)
            .pushStackItem(Bytes32.fromHexString("0x05")) // top-1 (a)
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(4);

    operation.execute(frame, null);

    assertThat(frame.stackTopV2()).isEqualTo(2); // consumes 3 produces 1
    assertThat(getV2StackItem(frame, 1)).isEqualTo(UInt256.fromBytesBE(untouched.toArrayUnsafe()));
  }
}
