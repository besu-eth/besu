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
package org.hyperledger.besu.ethereum.vm.operations.v2;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Calls a contract {@value #CALLS} times on EVM v2, as one contract calls another: the call data is
 * a slice of the caller's memory at the free memory pointer, and the callee loads the function
 * selector and its first two arguments. One benchmark operation is one call, so the cost of the
 * callee's first load, which a benchmark of the operation alone pays only once, is included. Run
 * with {@code -prof gc} for the allocation per call.
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CallDataLoadCallBenchmarkV2 {

  private static final int CALLS = 1_000;
  private static final Address CALLER = Address.fromHexString("0xc0de");
  private static final Address CALLEE = Address.fromHexString("0xca11ee");
  // the free memory pointer, where contracts usually encode the call data of their calls
  private static final int INPUT_OFFSET = 0x80;

  // PUSH0 CALLDATALOAD PUSH1 0xe0 SHR POP, PUSH1 4 CALLDATALOAD POP, PUSH1 0x24 CALLDATALOAD POP
  private static final Bytes CALLEE_CODE = Bytes.fromHexString("0x5f3560e01c50600435506024355000");

  // a function without arguments, with two, with eight, and payloads a proxy forwards
  @Param({"4", "68", "260", "1024", "32768", "1048576"})
  public int inputSize;

  private EVMExecutor executor;

  @Setup
  public void setUp() {
    final SimpleWorld world = new SimpleWorld();
    world.createAccount(CALLEE, 1, Wei.ZERO).setCode(CALLEE_CODE);
    final CallCounter callCounter = new CallCounter();
    executor =
        new EVMExecutor(
                EvmSpec.osaka(
                    BigInteger.ONE,
                    new EvmConfiguration(
                        EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
                        EvmConfiguration.DEFAULT.worldUpdaterMode(),
                        EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
                        true)))
            .worldUpdater(world)
            .code(callerCode(inputSize))
            .receiver(CALLER)
            .contract(CALLER)
            .gas(1_000_000_000_000L)
            .tracer(callCounter);
    callWithCallData();
    if (callCounter.successfulCalls != CALLS) {
      throw new IllegalStateException(
          "Expected " + CALLS + " calls, got " + callCounter.successfulCalls);
    }
    executor.tracer(OperationTracer.NO_TRACING);
  }

  @Benchmark
  @OperationsPerInvocation(CALLS)
  public Bytes callWithCallData() {
    return executor.execute();
  }

  /** CALLS times: CALL(GAS, CALLEE, 0, INPUT_OFFSET, inputSize, 0, 0). */
  private static Bytes callerCode(final int inputSize) {
    final ByteArrayOutputStream code = new ByteArrayOutputStream();
    code.writeBytes(new byte[] {0x61, (byte) (CALLS >>> 8), (byte) CALLS}); // PUSH2 CALLS
    final int loop = code.size();
    code.write(0x5b); // JUMPDEST
    code.writeBytes(new byte[] {0x60, 0x00, 0x60, 0x00}); // output size, output offset
    code.writeBytes(
        new byte[] {0x62, (byte) (inputSize >>> 16), (byte) (inputSize >>> 8), (byte) inputSize});
    code.writeBytes(new byte[] {0x60, (byte) INPUT_OFFSET}); // input offset
    code.writeBytes(new byte[] {0x60, 0x00}); // value
    code.write(0x73); // PUSH20
    code.writeBytes(CALLEE.getBytes().toArrayUnsafe());
    code.writeBytes(new byte[] {0x5a, (byte) 0xf1, 0x50}); // GAS, CALL, POP
    code.writeBytes(new byte[] {0x60, 0x01, (byte) 0x90, 0x03}); // PUSH1 1, SWAP1, SUB
    code.writeBytes(new byte[] {(byte) 0x80, 0x61, 0x00, (byte) loop, 0x57}); // DUP1, JUMPI
    code.writeBytes(new byte[] {0x50, 0x00}); // POP, STOP
    return Bytes.wrap(code.toByteArray());
  }

  private static final class CallCounter implements OperationTracer {
    private int successfulCalls;

    @Override
    public void traceContextExit(final MessageFrame frame) {
      if (frame.getDepth() == 1 && frame.getState() == MessageFrame.State.COMPLETED_SUCCESS) {
        successfulCalls++;
      }
    }
  }
}
