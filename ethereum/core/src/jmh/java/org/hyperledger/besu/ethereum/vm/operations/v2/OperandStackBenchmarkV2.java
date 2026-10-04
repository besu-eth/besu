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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.v2.StackArithmetic;
import org.hyperledger.besu.evm.v2.operation.PopOperationV2;
import org.hyperledger.besu.evm.v2.operation.Push0OperationV2;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks for EVM v2 stack manipulation (POP and PUSH0) on the flat {@code long[]} stack.
 *
 * <p>Measures the cost of filling the stack to a given depth using PUSH0, then popping all items
 * back to empty using POP.
 */
@State(Scope.Thread)
@Warmup(iterations = 6, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class OperandStackBenchmarkV2 {

  private static final int OPERATIONS_PER_INVOCATION = 1000;

  @Param({"6", "15", "34", "100", "234", "500", "800", "1024"})
  private int stackDepth;

  @Benchmark
  @OperationsPerInvocation(OPERATIONS_PER_INVOCATION)
  public void fillUpWithPush0() {
    for (int i = 0; i < OPERATIONS_PER_INVOCATION; i++) {
      MessageFrame frame = BenchmarkHelperV2.createMessageCallFrame();
      long[] s = frame.stackDataV2();
      for (int j = 0; j < stackDepth; j++) {
        frame.setTopV2(StackArithmetic.pushZero(s, frame.stackTopV2()));
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(OPERATIONS_PER_INVOCATION)
  public void push0AndPop() {
    for (int i = 0; i < OPERATIONS_PER_INVOCATION; i++) {
      MessageFrame frame = BenchmarkHelperV2.createMessageCallFrame();
      // Push stackDepth zeros
      for (int j = 0; j < stackDepth; j++) {
        Push0OperationV2.staticOperation(frame, frame.stackDataV2());
      }
      // Pop all of them
      for (int j = 0; j < stackDepth; j++) {
        PopOperationV2.staticOperation(frame);
      }
    }
  }
}
