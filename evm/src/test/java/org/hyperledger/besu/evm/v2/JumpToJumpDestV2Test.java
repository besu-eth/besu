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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.v2.ProgramRun.Execution;
import org.hyperledger.besu.evm.v2.ProgramRun.Fork;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

/**
 * The untraced v2 loop runs the JUMPDEST a JUMP or JUMPI lands on together with the jump, when the
 * gas covers both. The traced loop runs them one at a time. This runs jumps with every gas limit
 * around where the gas runs out, and checks that both loops end with the same gas, pc and halt
 * reason.
 */
class JumpToJumpDestV2Test {

  static final List<Fork> FORKS = ProgramRun.FORKS;

  private static final List<String> PROGRAMS =
      List.of(
          // PUSH1 4 JUMP INVALID JUMPDEST STOP
          "0x600456fe5b00",
          // PUSH1 1 PUSH1 6 JUMPI INVALID JUMPDEST STOP
          "0x6001600657fe5b00",
          // PUSH1 0 PUSH1 7 JUMPI PUSH1 1 JUMPDEST STOP: not taken
          "0x600060075760015b00",
          // JUMPDEST PUSH1 0 JUMP, until the gas runs out
          "0x5b600056",
          // JUMPDEST PUSH1 1 PUSH1 0 JUMPI, until the gas runs out
          "0x5b6001600057");

  private static final OperationTracer SEES_OPERATIONS =
      new OperationTracer() {
        @Override
        public boolean tracesOperations() {
          return true;
        }
      };

  @ParameterizedTest(name = "{0}")
  @FieldSource("FORKS")
  void tracedAndUntracedRunsEndAlikeAtEveryGasLimit(final Fork fork) {
    final EVM evm = fork.factory().apply(ProgramRun.V2);
    final List<String> differences = new ArrayList<>();
    for (final String program : PROGRAMS) {
      for (long gas = 0; gas <= 60; gas++) {
        final Execution untraced = run(evm, program, gas, OperationTracer.NO_TRACING);
        final Execution traced = run(evm, program, gas, SEES_OPERATIONS);
        if (!untraced.outcome().equals(traced.outcome())
            || untraced.frame().getPC() != traced.frame().getPC()) {
          differences.add(
              program
                  + " with gas "
                  + gas
                  + ": untraced pc "
                  + untraced.frame().getPC()
                  + " "
                  + untraced.outcome()
                  + ", traced pc "
                  + traced.frame().getPC()
                  + " "
                  + traced.outcome());
        }
      }
    }
    assertThat(differences).isEmpty();
  }

  @Test
  void aJumpDestWithoutGasLeftHaltsForTheGasAfterTheJump() {
    final EVM evm = ProgramRun.FORKS.getLast().factory().apply(ProgramRun.V2);
    // PUSH1 4 (3 gas) and JUMP (8 gas) leave nothing for the JUMPDEST at 4
    final Execution untraced = run(evm, "0x600456fe5b00", 11, OperationTracer.NO_TRACING);

    assertThat(untraced.outcome().haltReason()).isEqualTo("INSUFFICIENT_GAS");
    assertThat(untraced.frame().getPC()).isEqualTo(4);
  }

  private static Execution run(
      final EVM evm, final String program, final long gas, final OperationTracer tracer) {
    return ProgramRun.execute(
        evm, Bytes.fromHexString(program), Bytes.EMPTY, Bytes.EMPTY, gas, false, List.of(), tracer);
  }
}
