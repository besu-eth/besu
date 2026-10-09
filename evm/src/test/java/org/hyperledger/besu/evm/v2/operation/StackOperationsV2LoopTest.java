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
import static org.hyperledger.besu.evm.v2.operation.StackManipulationOperationV2Test.word;
import static org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2.getV2StackItem;

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Runs the stack manipulation opcodes through the v2 interpreter loop to check their dispatch. */
class StackOperationsV2LoopTest {

  private static final int STOP = 0x00;
  private static final int ITEMS = 17;

  private final EVM evm =
      MainnetEVMs.osaka(
          BigInteger.ONE,
          new EvmConfiguration(32_000L, EvmConfiguration.WorldUpdaterMode.STACKED, false, true));

  @ParameterizedTest(name = "DUP{0}")
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
  void dupOpcodeRunsDupN(final int n) {
    final MessageFrame frame = run(DupOperationV2.DUP_BASE + n);

    assertThat(frame.stackTopV2()).isEqualTo(ITEMS + 1);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(word(n));
    for (int depth = 1; depth <= ITEMS; depth++) {
      assertThat(getV2StackItem(frame, depth)).isEqualTo(word(depth));
    }
  }

  @ParameterizedTest(name = "SWAP{0}")
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
  void swapOpcodeRunsSwapN(final int n) {
    final MessageFrame frame = run(SwapOperationV2.SWAP_BASE + n);

    assertThat(frame.stackTopV2()).isEqualTo(ITEMS);
    for (int depth = 1; depth <= ITEMS; depth++) {
      final int expected = depth == 1 ? n + 1 : depth == n + 1 ? 1 : depth;
      assertThat(getV2StackItem(frame, depth - 1)).isEqualTo(word(expected));
    }
  }

  /** Runs the opcode followed by STOP on a stack whose item at depth k (1 = top) is word(k). */
  private MessageFrame run(final int opcode) {
    final TestMessageFrameBuilderV2 builder =
        new TestMessageFrameBuilderV2().code(new Code(Bytes.of(opcode, STOP)));
    for (int depth = ITEMS; depth >= 1; depth--) {
      builder.pushStackItem(Bytes32.wrap(word(depth).toBytesBE()));
    }
    final MessageFrame frame = builder.build();
    frame.setState(MessageFrame.State.CODE_EXECUTING);

    evm.runToHalt(frame, OperationTracer.NO_TRACING);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.CODE_SUCCESS);
    assertThat(frame.getPC()).isEqualTo(1);
    return frame;
  }
}
