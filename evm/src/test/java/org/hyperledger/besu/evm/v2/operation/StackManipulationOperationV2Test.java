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

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.function.IntFunction;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Shared tests for the indexed stack manipulation operations DUP1-16 and SWAP1-16. */
abstract class StackManipulationOperationV2Test {

  protected final GasCalculator gasCalculator = new FrontierGasCalculator();
  private final IntFunction<Operation> operationForIndex;

  StackManipulationOperationV2Test(final IntFunction<Operation> operationForIndex) {
    this.operationForIndex = operationForIndex;
  }

  /**
   * The operation with index n, e.g. DUPn.
   *
   * @param n the index, 1-16
   * @return the operation
   */
  protected Operation operation(final int n) {
    return operationForIndex.apply(n);
  }

  /**
   * The number of stack items the operation with index n reads.
   *
   * @param n the index, 1-16
   * @return the number of items it needs
   */
  protected abstract int itemsNeeded(int n);

  @ParameterizedTest(name = "n = {0}")
  @ValueSource(ints = {1, 2, 15, 16})
  void shouldHaltOnStackUnderflow(final int n) {
    final MessageFrame frame = frameWithItems(itemsNeeded(n) - 1);

    final Operation.OperationResult result = operation(n).execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(frame.stackTopV2()).isEqualTo(itemsNeeded(n) - 1);
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().initialGas(1).build();
    frame.setTopV2(17);

    final Operation.OperationResult result = operation(16).execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(frame.stackTopV2()).isEqualTo(17);
  }

  @Test
  void gasCostIsVeryLowTier() {
    final Operation.OperationResult result = operation(1).execute(frameWithItems(17), null);

    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getVeryLowTierGasCost());
  }

  /**
   * A frame whose item at depth k (1 = top) is word(k).
   *
   * @param count the number of items
   * @return the frame
   */
  protected static MessageFrame frameWithItems(final int count) {
    final TestMessageFrameBuilderV2 builder = new TestMessageFrameBuilderV2();
    for (int depth = count; depth >= 1; depth--) {
      builder.pushStackItem(Bytes32.wrap(word(depth).toBytesBE()));
    }
    return builder.build();
  }

  /**
   * A value with four distinct limbs, so any misplaced limb is detected.
   *
   * @param depth the depth the value is pushed at
   * @return the value
   */
  protected static UInt256 word(final int depth) {
    final long tag = (long) depth << 32;
    return new UInt256(tag | 0xa, tag | 0xb, tag | 0xc, tag | 0xd);
  }
}
