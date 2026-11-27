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

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.testutils.TestEvmConfigurations;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Measures the worst-case stop latency of the kill switch: a loop whose body is the longest
 * possible straight-line stretch of opcodes that all hit the fast switch cases of both
 * interpreters, so the only poll in the whole 24 KB contract is the loop's JUMPDEST. The stop
 * latency is bounded by one loop iteration; an interrupt landing right after the poll pays all of
 * it.
 *
 * <p>Not an assertion-heavy test: it prints the measured numbers, asserting only that the kill
 * fired. Run it on demand.
 */
class EvmKillSwitchWorstCaseLatencyTest {

  private static final int CODE_SIZE_LIMIT = 24576; // EIP-170
  private static final int KILLS = 30;
  private static final int TIMED_ITERATIONS = 200;

  @AfterEach
  void cleanUp() {
    EvmKillSwitch.disarmForTesting();
    Thread.interrupted();
  }

  // PUSH1 1; JUMPDEST; (DUP1 POP)*N; PUSH2 2; JUMP -- N pairs fill the code up to the size limit
  private static Bytes worstCaseContract() {
    final int fixed = 2 + 1 + 3 + 1; // PUSH1 1, JUMPDEST, PUSH2 0002, JUMP
    final int pairs = (CODE_SIZE_LIMIT - fixed) / 2;
    final byte[] code = new byte[fixed + pairs * 2];
    int i = 0;
    code[i++] = 0x60; // PUSH1
    code[i++] = 0x01;
    code[i++] = 0x5b; // JUMPDEST, pc = 2
    for (int p = 0; p < pairs; p++) {
      code[i++] = (byte) 0x80; // DUP1
      code[i++] = 0x50; // POP
    }
    code[i++] = 0x61; // PUSH2
    code[i++] = 0x00;
    code[i++] = 0x02;
    code[i] = 0x56; // JUMP
    return Bytes.wrap(code);
  }

  private static long opsPerIteration() {
    final int pairs = (CODE_SIZE_LIMIT - 7) / 2;
    return 1 + pairs * 2L + 2; // JUMPDEST + DUP1/POP pairs + PUSH2 + JUMP
  }

  private static long gasPerIteration() {
    final int pairs = (CODE_SIZE_LIMIT - 7) / 2;
    return 1 + pairs * 5L + 3 + 8; // JUMPDEST + (DUP1 3 + POP 2) + PUSH2 + JUMP
  }

  private MessageFrame frame(final boolean evmV2, final Code code, final long gas) {
    final MessageFrame frame =
        evmV2
            ? new TestMessageFrameBuilderV2().code(code).initialGas(gas).build()
            : new TestMessageFrameBuilder().code(code).initialGas(gas).build();
    frame.setState(MessageFrame.State.CODE_EXECUTING);
    return frame;
  }

  @ParameterizedTest(name = "evmV2={0}")
  @ValueSource(booleans = {false, true})
  void worstCaseStopLatency(final boolean evmV2) throws Exception {
    final EVM evm =
        MainnetEVMs.london(evmV2 ? TestEvmConfigurations.EVM_V2 : TestEvmConfigurations.EVM_V1);
    final Code code = new Code(worstCaseContract());

    // warm up the interpreter loop, then time uninterrupted iterations while disarmed
    runIterations(evm, evmV2, code, 50);
    final long perIterationNanos = runIterations(evm, evmV2, code, TIMED_ITERATIONS);

    // measure the kill latency, from just before arm+interrupt to the catch in the EVM thread
    final List<Long> latencies = new ArrayList<>();
    for (int i = 0; i < KILLS; i++) {
      final MessageFrame frame = frame(evmV2, code, Long.MAX_VALUE);
      final CountDownLatch started = new CountDownLatch(1);
      final AtomicLong caughtAt = new AtomicLong();
      final Thread evmThread =
          new Thread(
              () -> {
                started.countDown();
                try {
                  evm.runToHalt(frame, OperationTracer.NO_TRACING);
                } catch (final EvmInterruptedException e) {
                  caughtAt.set(System.nanoTime());
                }
              },
              "worst-case-latency-" + i);
      evmThread.start();
      started.await();
      Thread.sleep(50); // land the interrupt at an effectively random point of the loop body
      final long armedAt = System.nanoTime();
      EvmKillSwitch.armScoped();
      evmThread.interrupt();
      evmThread.join(10_000);
      assertThat(evmThread.isAlive()).isFalse();
      assertThat(caughtAt.get()).isNotZero();
      latencies.add(caughtAt.get() - armedAt);
      EvmKillSwitch.disarmForTesting();
    }

    latencies.sort(Long::compare);
    System.out.printf(
        "EVM %s worst-case contract: %d ops/iteration, %d bytes%n"
            + "  uninterrupted iteration: %.3f ms (%.2f ns/op)%n"
            + "  kill latency over %d kills: min %.3f ms, median %.3f ms, p90 %.3f ms, max %.3f ms%n",
        evmV2 ? "v2" : "v1",
        opsPerIteration(),
        worstCaseContract().size(),
        perIterationNanos / 1e6,
        (double) perIterationNanos / opsPerIteration(),
        KILLS,
        latencies.get(0) / 1e6,
        latencies.get(KILLS / 2) / 1e6,
        latencies.get((int) (KILLS * 0.9)) / 1e6,
        latencies.get(KILLS - 1) / 1e6);
  }

  private long runIterations(
      final EVM evm, final boolean evmV2, final Code code, final int iterations) {
    final MessageFrame frame = frame(evmV2, code, gasPerIteration() * iterations + 2);
    final long started = System.nanoTime();
    evm.runToHalt(frame, OperationTracer.NO_TRACING);
    return (System.nanoTime() - started) / iterations;
  }
}
