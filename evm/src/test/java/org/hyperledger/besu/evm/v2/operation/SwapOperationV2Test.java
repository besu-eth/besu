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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Structural tests for SWAP1-16. Equivalence with the original implementation at every index and at
 * the stack limits is covered by StackOperationsV2ComparisonTest.
 */
class SwapOperationV2Test extends StackManipulationOperationV2Test {

  public SwapOperationV2Test() {
    super(n -> new SwapOperationV2(n, new FrontierGasCalculator()));
  }

  @Override
  protected int itemsNeeded(final int n) {
    return n + 1;
  }

  /** SWAPn exchanges the top with the item at depth n + 1; all other items stay in place. */
  @ParameterizedTest(name = "SWAP{0}")
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
  void swapOperation(final int n) {
    final MessageFrame frame = frameWithItems(17);

    final Operation.OperationResult result = operation(n).execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isEqualTo(1);
    assertThat(frame.stackTopV2()).isEqualTo(17);
    for (int depth = 1; depth <= 17; depth++) {
      final int expected = depth == 1 ? n + 1 : depth == n + 1 ? 1 : depth;
      assertThat(getV2StackItem(frame, depth - 1)).isEqualTo(word(expected));
    }
  }
}
