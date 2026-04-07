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
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.v2.operation.BlockHashOperationV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes32;
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

/** Benchmark for BLOCKHASH EVM v2 operation. */
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BlockHashOperationBenchmarkV2 {

  protected static final int SAMPLE_SIZE = 30_000;
  private static final long CURRENT_BLOCK_NUMBER = 1_000L;

  @Param({"IN_RANGE", "OUT_OF_RANGE", "FUTURE"})
  public String blockCase;

  private long[] blockNumberPool;
  private int index;
  private MessageFrame frame;

  @Setup
  public void setUp() {
    final BlockValues blockValues = mock(BlockValues.class);
    when(blockValues.getNumber()).thenReturn(CURRENT_BLOCK_NUMBER);

    frame =
        MessageFrame.builder()
            .enableEvmV2(true)
            .worldUpdater(mock(WorldUpdater.class))
            .originator(Address.ZERO)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(blockValues)
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(Long.MAX_VALUE)
            .address(Address.ZERO)
            .contract(Address.ZERO)
            .inputData(Bytes32.ZERO)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(Code.EMPTY_CODE)
            .completer(__ -> {})
            .build();

    blockNumberPool = new long[SAMPLE_SIZE];
    final Random random = new Random(42);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      blockNumberPool[i] =
          switch (blockCase) {
            case "IN_RANGE" ->
                // Valid block in the lookback window
                CURRENT_BLOCK_NUMBER - 1 - (random.nextInt(256));
            case "OUT_OF_RANGE" ->
                // Outside lookback window
                CURRENT_BLOCK_NUMBER - 257 - random.nextInt(100);
            default ->
                // Future block
                CURRENT_BLOCK_NUMBER + random.nextInt(100);
          };
    }
    index = 0;
  }

  @Benchmark
  public void blockHash(final Blackhole blackhole) {
    pushLong(blockNumberPool[index]);
    blackhole.consume(BlockHashOperationV2.staticOperation(frame, frame.stackDataV2()));
    // Pop the resulting hash
    frame.setTopV2(frame.stackTopV2() - 1);
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
