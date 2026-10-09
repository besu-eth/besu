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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.internal.UnderflowException;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** Tracers read the stack through the frame, which shows them the v2 stack on EVM v2. */
class TracedExecutionV2Test {

  private final EVM evm =
      MainnetEVMs.osaka(
          BigInteger.ONE,
          new EvmConfiguration(32_000L, EvmConfiguration.WorldUpdaterMode.STACKED, true, true));

  @Test
  void tracerSeesTheOperationAndTheStackBeforeAndAfterIt() {
    final List<String> traced = new ArrayList<>();
    final OperationTracer tracer =
        new OperationTracer() {
          @Override
          public void tracePreExecution(final MessageFrame frame) {
            traced.add("pre " + frame.getCurrentOperation().getName() + " " + stack(frame));
          }

          @Override
          public void tracePostExecution(final MessageFrame frame, final OperationResult result) {
            traced.add("post " + frame.getCurrentOperation().getName() + " " + stack(frame));
          }
        };
    // PUSH1 1 PUSH1 2 ADD STOP
    final MessageFrame frame = start(Bytes.fromHexString("0x600160020100"));

    evm.runToHalt(frame, tracer);

    assertThat(traced)
        .containsExactly(
            "pre PUSH1 []",
            "post PUSH1 [1]",
            "pre PUSH1 [1]",
            "post PUSH1 [2, 1]",
            "pre ADD [2, 1]",
            "post ADD [3]",
            "pre STOP [3]",
            "post STOP [3]");
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(frame.stackDataV2()[3]).isEqualTo(3L);
  }

  @Test
  void frameStackAccessorsMatchTheV2StackAfterEveryOperation() {
    final StringBuilder code = new StringBuilder("0x");
    for (int i = 1; i <= 17; i++) {
      code.append(String.format("60%02x", i)); // PUSH1 i
    }
    // DUP16 SWAP16 SWAP1 POP DUP1 ADD SWAP16 POP MUL SWAP2 POP STOP
    code.append("8f9f905080019f50029150" + "00");
    final List<String> mismatches = new ArrayList<>();
    final OperationTracer tracer =
        new OperationTracer() {
          @Override
          public void tracePreExecution(final MessageFrame frame) {
            check(frame);
          }

          @Override
          public void tracePostExecution(final MessageFrame frame, final OperationResult result) {
            check(frame);
          }

          private void check(final MessageFrame frame) {
            final List<String> v2 = new ArrayList<>();
            final long[] s = frame.stackDataV2();
            for (int i = frame.stackTopV2() - 1; i >= 0; i--) {
              v2.add(
                  Bytes32.wrap(
                          Bytes.concatenate(
                              Bytes.ofUnsignedLong(s[i << 2]),
                              Bytes.ofUnsignedLong(s[(i << 2) + 1]),
                              Bytes.ofUnsignedLong(s[(i << 2) + 2]),
                              Bytes.ofUnsignedLong(s[(i << 2) + 3])))
                      .toShortHexString());
            }
            final List<String> seen = new ArrayList<>();
            for (int i = 0; i < frame.stackSize(); i++) {
              seen.add(Bytes32.leftPad(frame.getStackItem(i)).toShortHexString());
            }
            if (!seen.equals(v2)) {
              mismatches.add(frame.getCurrentOperation().getName() + ": " + seen + " vs " + v2);
            }
          }
        };
    final MessageFrame frame = start(Bytes.fromHexString(code.toString()));

    evm.runToHalt(frame, tracer);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.CODE_SUCCESS);
    assertThat(mismatches).isEmpty();
  }

  @Test
  void tracerWithoutOperationHooksGetsNoOperationCalls() {
    // like the parallel block processor's tracer, which only implements a reward hook
    final OperationTracer tracer =
        new OperationTracer() {
          @Override
          public void traceContextEnter(final MessageFrame frame) {}
        };
    // PUSH1 1 PUSH1 2 ADD STOP
    final MessageFrame frame = start(Bytes.fromHexString("0x600160020100"));

    evm.runToHalt(frame, tracer);

    assertThat(frame.getCurrentOperation()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
  }

  @Test
  void readingBelowTheV2StackUnderflowsAsOnV1() {
    // PUSH1 1 PUSH1 2 ADD STOP
    final MessageFrame frame = start(Bytes.fromHexString("0x600160020100"));

    evm.runToHalt(frame, OperationTracer.NO_TRACING);

    assertThat(frame.stackSize()).isEqualTo(1);
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes32.leftPad(Bytes.of(3)));
    assertThatThrownBy(() -> frame.getStackItem(1)).isInstanceOf(UnderflowException.class);
  }

  private static String stack(final MessageFrame frame) {
    final List<String> items = new ArrayList<>();
    for (int i = 0; i < frame.stackSize(); i++) {
      items.add(Bytes32.leftPad(frame.getStackItem(i)).toBigInteger().toString());
    }
    return items.toString();
  }

  private static MessageFrame start(final Bytes code) {
    final MessageFrame frame = new TestMessageFrameBuilder().code(new Code(code)).build();
    frame.setState(MessageFrame.State.CODE_EXECUTING);
    return frame;
  }
}
