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

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.CountLeadingZerosOperation;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
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

@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class CountLeadingZerosOperationBenchmark {
  // variable used to run inner loop of invocations because CLZ runs in under 15 nanoseconds so
  // overhead from framework
  // taking measurements is high. There are variable and offset errors either in baseline and the
  // operation benchmarks that
  // can't be ignored.
  private static final int OPERATIONS_PER_INVOCATION = 1_000_000;

  @Param({
    "0x23",
    "0x2323232323232323232323232323232323232323232323232323232323232323",
    "0x23232323232323232323232323232323232323232323232323232323232323",
    "0x2323232323232323232323232323232323232323232323",
    "0x232323232323232323232323232323",
  })
  private String bytesHex;

  private Bytes bytes;

  private MessageFrame frame;

  @Setup
  public void setUp() {
    frame =
        MessageFrame.builder()
            .worldUpdater(mock(WorldUpdater.class))
            .originator(Address.ZERO)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(mock(BlockValues.class))
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(1)
            .address(Address.ZERO)
            .contract(Address.ZERO)
            .inputData(Bytes32.ZERO)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(Code.EMPTY_CODE)
            .completer(messageFrame -> {})
            .build();
    bytes = Bytes.fromHexString(bytesHex);
  }

  @Benchmark
  @OperationsPerInvocation(OPERATIONS_PER_INVOCATION)
  public void executeOperation() {
    for (int i = 0; i < OPERATIONS_PER_INVOCATION; i++) {
      frame.pushStackItem(bytes);
      CountLeadingZerosOperation.staticOperation(frame);
      frame.popStackItem();
    }
  }
}
