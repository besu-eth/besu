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
package org.hyperledger.besu.evm.v2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.v2.ProgramRun.Fork;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

/**
 * Runs every opcode on a full stack, which the programs of the other differential tests never
 * reach, and requires a tracer to see the same on v1 and v2: the gas cost and halt reason the
 * operation reports, the gas left and the stack.
 */
class FullStackV1V2DifferentialTest {

  static final List<Fork> FORKS = ProgramRun.FORKS;

  private static final int FULL = MessageFrame.DEFAULT_MAX_STACK_SIZE;

  @ParameterizedTest(name = "{0}")
  @FieldSource("FORKS")
  void everyOpcodeOnAFullStackLooksTheSameToATracer(final Fork fork) {
    final EVM v1 = fork.factory().apply(ProgramRun.V1);
    final EVM v2 = fork.factory().apply(ProgramRun.V2);
    final List<String> differences = new ArrayList<>();
    for (int opcode = 0; opcode < 0x100; opcode++) {
      if (v1.getOperationsUnsafe()[opcode] instanceof InvalidOperation) {
        continue;
      }
      // 0x00 as the immediate, for the operations that take one, then STOP
      final Code code = new Code(Bytes.of(opcode, 0x00, 0x00));
      final String onV1 = firstStep(v1, code, false);
      final String onV2 = firstStep(v2, code, true);
      if (!onV1.equals(onV2)) {
        differences.add(String.format("opcode 0x%02x:%n  v1 %s%n  v2 %s", opcode, onV1, onV2));
      }
    }
    assertThat(differences).isEmpty();
  }

  private static String firstStep(final EVM evm, final Code code, final boolean isV2) {
    // the frame's own account, which SLOAD, SELFBALANCE and others read
    final WorldUpdater world = new ToyWorld().updater();
    world.getOrCreate(TestMessageFrameBuilder.DEFAULT_ADDRESS).setBalance(Wei.of(1_000));
    final MessageFrame frame;
    if (isV2) {
      final TestMessageFrameBuilderV2 builder =
          new TestMessageFrameBuilderV2().worldUpdater(world).code(code).initialGas(1_000_000L);
      for (int i = 0; i < FULL; i++) {
        builder.pushStackItem(Bytes32.leftPad(Bytes.ofUnsignedInt(i)));
      }
      frame = builder.build();
    } else {
      final TestMessageFrameBuilder builder =
          new TestMessageFrameBuilder().worldUpdater(world).code(code).initialGas(1_000_000L);
      for (int i = 0; i < FULL; i++) {
        builder.pushStackItem(Bytes32.leftPad(Bytes.ofUnsignedInt(i)));
      }
      frame = builder.build();
    }
    frame.setState(MessageFrame.State.CODE_EXECUTING);
    final List<String> steps = new ArrayList<>();
    evm.runToHalt(
        frame,
        new OperationTracer() {
          @Override
          public void tracePostExecution(final MessageFrame frame, final OperationResult result) {
            steps.add(
                String.format(
                    "%s cost %d halt %s gas %d stack %d top %s",
                    frame.getCurrentOperation().getName(),
                    result.getGasCost(),
                    result.getHaltReason(),
                    frame.getRemainingGas(),
                    frame.stackSize(),
                    frame.stackSize() == 0
                        ? "none"
                        : Bytes.wrap(Bytes32.leftPad(frame.getStackItem(0)).toArray())
                            .toShortHexString()));
          }
        });
    return steps.isEmpty() ? "no step" : steps.getFirst();
  }
}
