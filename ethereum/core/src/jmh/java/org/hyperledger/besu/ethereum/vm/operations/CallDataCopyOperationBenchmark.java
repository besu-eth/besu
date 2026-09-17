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
package org.hyperledger.besu.ethereum.vm.operations;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.operation.CallDataCopyOperation;

import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class CallDataCopyOperationBenchmark implements GasCostBenchmark {

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator calc) {
    final long size = Long.parseLong(params.getParam("dataSize"));
    final MessageFrame frame = BenchmarkHelper.createMessageCallFrame();
    frame.expandMemory(0, size);
    return calc.dataCopyOperationGasCost(frame, 0, size);
  }

  private CallDataCopyOperation callDataCopyOperation;
  protected static final int SAMPLE_SIZE = 30_000;

  // Parameters matching the test case variations
  @Param({"0", "100", "10240", "1048576"}) // 0 bytes, 100 bytes, 10KiB, 1MiB
  public int dataSize;

  // dataSize=0 byte          -> gas cost = 3 gas
  // dataSize=100 bytes       -> gas cost = 15 gas
  // dataSize=10240 bytes     -> gas cost = 963 gas
  // dataSize=1048576 bytes   -> gas cost = 98307 gas

  @Param({"false", "true"})
  public boolean fixedSrcDst;

  @Param({"false", "true"})
  public boolean nonZeroData;

  protected Bytes[] destOffsetPool;
  protected Bytes[] srcOffsetPool;
  protected Bytes[] sizePool;
  protected int index;
  protected MessageFrame frame;
  protected Bytes callData;

  @Setup
  public void setUp() {
    callDataCopyOperation = new CallDataCopyOperation(new OsakaGasCalculator());
    callData = BenchmarkHelper.createCallData(dataSize, nonZeroData);
    frame = BenchmarkHelper.createMessageCallFrameWithCallData(callData);

    // Initialize parameter pools
    destOffsetPool = new Bytes[SAMPLE_SIZE];
    srcOffsetPool = new Bytes[SAMPLE_SIZE];
    sizePool = new Bytes[SAMPLE_SIZE];

    // Fill pools with appropriate values based on test parameters
    BenchmarkHelper.fillPoolsForCallData(
        sizePool, destOffsetPool, srcOffsetPool, dataSize, fixedSrcDst);

    index = 0;
  }

  @Override
  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    frame.pushStackItem(sizePool[index]);
    frame.pushStackItem(srcOffsetPool[index]);
    frame.pushStackItem(destOffsetPool[index]);

    blackhole.consume(callDataCopyOperation.execute(frame, null));

    index = (index + 1) % SAMPLE_SIZE;
  }
}
