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
package org.hyperledger.besu.evm.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.testutils.TestEvmConfigurations;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EvmKillSwitchTest {

  // JUMPDEST; PUSH1 0; JUMP: loops forever on a JUMPDEST poll
  private static final Bytes INFINITE_LOOP = Bytes.fromHexString("0x5b600056");

  @AfterEach
  void cleanUp() {
    EvmKillSwitch.disarmForTesting();
    Thread.interrupted(); // do not leak an interrupt flag into other tests
  }

  @Test
  void disarmedPollIgnoresInterruptStatus() {
    Thread.currentThread().interrupt();
    assertThatCode(EvmKillSwitch::poll).doesNotThrowAnyException();
    assertThat(Thread.interrupted()).isTrue(); // the flag was left untouched
  }

  @Test
  void armedPollThrowsOnlyForInterruptedThreadsAndClearsTheFlag() {
    EvmKillSwitch.armScoped();

    assertThatCode(EvmKillSwitch::poll).doesNotThrowAnyException();

    Thread.currentThread().interrupt();
    assertThatThrownBy(EvmKillSwitch::poll).isInstanceOf(EvmInterruptedException.class);
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }

  @Test
  void scopedArmDisarmsOnRelease() {
    EvmKillSwitch.armScoped();
    assertThat(EvmKillSwitch.isArmed()).isTrue();

    EvmKillSwitch.release();
    assertThat(EvmKillSwitch.isArmed()).isFalse();
  }

  @Test
  void scopedArmsDisarmOnlyWhenAllAreReleased() {
    EvmKillSwitch.armScoped();
    EvmKillSwitch.armScoped();

    EvmKillSwitch.release();
    assertThat(EvmKillSwitch.isArmed()).isTrue();

    EvmKillSwitch.release();
    assertThat(EvmKillSwitch.isArmed()).isFalse();
  }

  @Test
  void missedReleaseGoesStaleAfterTheDefaultArmedPeriod() throws Exception {
    EvmKillSwitch.armScoped();
    assertThat(EvmKillSwitch.isArmed()).isTrue();

    final long deadline = System.currentTimeMillis() + 10_000;
    while (EvmKillSwitch.isArmed() && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertThat(EvmKillSwitch.isArmed()).isFalse();
  }

  @ParameterizedTest(name = "evmV2={0}")
  @ValueSource(booleans = {false, true})
  void stopsAnInfiniteLoopAlreadyRunningInTheInterpreter(final boolean evmV2)
      throws InterruptedException {
    final EVM evm =
        MainnetEVMs.london(evmV2 ? TestEvmConfigurations.EVM_V2 : TestEvmConfigurations.EVM_V1);
    final Code code = new Code(INFINITE_LOOP);
    final MessageFrame frame =
        evmV2
            ? new TestMessageFrameBuilderV2().code(code).build()
            : new TestMessageFrameBuilder().code(code).build();
    frame.setState(MessageFrame.State.CODE_EXECUTING);

    final AtomicReference<Throwable> thrown = new AtomicReference<>();
    final Thread evmThread =
        new Thread(
            () -> {
              try {
                evm.runToHalt(frame, OperationTracer.NO_TRACING);
              } catch (final Throwable t) {
                thrown.set(t);
              }
            },
            "evm-kill-switch-test-loop");
    evmThread.start();

    Thread.sleep(200); // let it spin inside the interpreter loop

    EvmKillSwitch.armScoped();
    evmThread.interrupt();
    evmThread.join(10_000);

    assertThat(evmThread.isAlive()).isFalse();
    assertThat(thrown.get()).isInstanceOf(EvmInterruptedException.class);
  }
}
