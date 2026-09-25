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

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.v2.operation.CallDataCopyOperationV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

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
import org.openjdk.jmh.infra.Blackhole;

/** Benchmark for CALLDATACOPY EVM v2 operation. */
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class CallDataCopyBenchmarkV2 {

  protected static final int SAMPLE_SIZE = 30_000;

  @Param({"0", "100", "10240", "1048576"}) // 0 bytes, 100 bytes, 10KiB, 1MiB
  public int dataSize;

  @Param({"false", "true"})
  public boolean fixedSrcDst;

  private CancunGasCalculator gasCalculator;
  private long[] destOffsetPool;
  private long[] srcOffsetPool;
  private long[] sizePool;
  private int index;
  private MessageFrame frame;

  @Setup
  public void setUp() {
    gasCalculator = new CancunGasCalculator();

    // Build call data payload
    final byte[] data = new byte[dataSize];
    for (int i = 0; i < dataSize; i++) {
      data[i] = (byte) (i % 256);
    }
    final Bytes callData = Bytes.wrap(data);

    frame =
        MessageFrame.builder()
            .enableEvmV2(true)
            .worldUpdater(mock(WorldUpdater.class))
            .originator(Address.ZERO)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(mock(BlockValues.class))
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(Long.MAX_VALUE)
            .address(Address.ZERO)
            .contract(Address.ZERO)
            .inputData(callData)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(Code.EMPTY_CODE)
            .completer(__ -> {})
            .build();

    destOffsetPool = new long[SAMPLE_SIZE];
    srcOffsetPool = new long[SAMPLE_SIZE];
    sizePool = new long[SAMPLE_SIZE];

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      sizePool[i] = dataSize;
      if (fixedSrcDst) {
        destOffsetPool[i] = 0L;
        srcOffsetPool[i] = 0L;
      } else {
        destOffsetPool[i] = (long) ((i * 32) % 1024);
        srcOffsetPool[i] = (long) (i % Math.max(1, dataSize));
      }
    }
    index = 0;

    // Pre-warm destination memory to avoid expansion costs during benchmark
    final long maxDstOffset = 1024L + 32L;
    for (long off = 0; off < maxDstOffset; off += 32) {
      frame.writeMemory(off, 32, Bytes.EMPTY);
    }
  }

  @Benchmark
  public void callDataCopy(final Blackhole blackhole) {
    // Push: size, srcOffset, destOffset (stack order: destOffset is top)
    pushLong(sizePool[index]);
    pushLong(srcOffsetPool[index]);
    pushLong(destOffsetPool[index]);
    blackhole.consume(
        CallDataCopyOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator));
    index = (index + 1) % SAMPLE_SIZE;
  }

  private void pushLong(final long value) {
    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int dst = top << 2;
    s[dst] = 0L;
    s[dst + 1] = 0L;
    s[dst + 2] = 0L;
    s[dst + 3] = value;
    frame.setTopV2(top + 1);
  }
}
