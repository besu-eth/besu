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

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.v2.operation.TLoadOperationV2;
import org.hyperledger.besu.evm.v2.operation.TStoreOperationV2;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Benchmarks for TLOAD and TSTORE EVM v2 operations on transient storage. */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class TransientStorageOperationBenchmarkV2 {

  private static final int SAMPLE_SIZE = 30_000;

  private UInt256[] keyPool;
  private UInt256[] valuePool;
  private int index;
  private MessageFrame frame;

  @Setup
  public void setUp() {
    frame = BenchmarkHelperV2.createMessageCallFrame();
    keyPool = new UInt256[SAMPLE_SIZE];
    valuePool = new UInt256[SAMPLE_SIZE];
    BenchmarkHelperV2.fillUInt256Pool(keyPool);
    BenchmarkHelperV2.fillUInt256Pool(valuePool);
    index = 0;
  }

  /** Benchmark TSTORE followed by TLOAD (round-trip). */
  @Benchmark
  public void tStoreAndLoad(final Blackhole blackhole) {
    final long[] s = frame.stackDataV2();

    // TSTORE: depth 0 = key, depth 1 = value
    BenchmarkHelperV2.pushUInt256(frame, valuePool[index]);
    BenchmarkHelperV2.pushUInt256(frame, keyPool[index]);
    blackhole.consume(TStoreOperationV2.staticOperation(frame, s));

    // TLOAD: depth 0 = key; overwrites top slot with loaded value
    BenchmarkHelperV2.pushUInt256(frame, keyPool[index]);
    blackhole.consume(TLoadOperationV2.staticOperation(frame, s));

    // Clean up the loaded value left on the stack
    frame.setTopV2(frame.stackTopV2() - 1);

    index = (index + 1) % SAMPLE_SIZE;
  }

  /** Benchmark TSTORE only. */
  @Benchmark
  public void tStoreOnly(final Blackhole blackhole) {
    final long[] s = frame.stackDataV2();

    // TSTORE: depth 0 = key, depth 1 = value
    BenchmarkHelperV2.pushUInt256(frame, valuePool[index]);
    BenchmarkHelperV2.pushUInt256(frame, keyPool[index]);
    blackhole.consume(TStoreOperationV2.staticOperation(frame, s));

    index = (index + 1) % SAMPLE_SIZE;
  }

  /** Benchmark TLOAD only (pre-stores a value to ensure a non-empty result). */
  @Benchmark
  public void tLoadOnly(final Blackhole blackhole) {
    final long[] s = frame.stackDataV2();

    // Pre-store a value for the key (not measured by JMH — this is setup work within the method)
    BenchmarkHelperV2.pushUInt256(frame, valuePool[index]);
    BenchmarkHelperV2.pushUInt256(frame, keyPool[index]);
    TStoreOperationV2.staticOperation(frame, s);

    // TLOAD: depth 0 = key; overwrites top slot with loaded value
    BenchmarkHelperV2.pushUInt256(frame, keyPool[index]);
    blackhole.consume(TLoadOperationV2.staticOperation(frame, s));

    // Clean up the loaded value left on the stack
    frame.setTopV2(frame.stackTopV2() - 1);

    index = (index + 1) % SAMPLE_SIZE;
  }
}
