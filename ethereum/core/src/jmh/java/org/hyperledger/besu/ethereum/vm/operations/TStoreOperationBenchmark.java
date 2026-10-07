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

import static org.hyperledger.besu.ethereum.vm.operations.BenchmarkHelper.fillPool;
import static org.hyperledger.besu.ethereum.vm.operations.BenchmarkHelper.fillPoolWithCollidingHashes;
import static org.hyperledger.besu.ethereum.vm.operations.BenchmarkHelper.fillPoolWithDistinctHashes;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.TStoreOperation;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

public class TStoreOperationBenchmark extends BinaryOperationNoPushBenchmark
    implements GasCostBenchmark {
  TStoreOperation operation;

  @Param({"DISTINCT_KEYS", "COLLIDING_KEYS", "RANDOM_KEYS"})
  protected String scenario;

  @Param({"1000", "10000", "150000"})
  int slotCount;

  @Override
  public void setUp() throws Exception {
    operation = new TStoreOperation(new CancunGasCalculator());
    frame = buildFrame();
    aPool = new Bytes[getSampleSize()];
    bPool = new Bytes[getSampleSize()];

    BenchmarkHelper.fillPool(bPool);
    switch (scenario) {
      case "RANDOM_KEYS" -> fillPool(aPool);
      case "DISTINCT_KEYS" -> fillPoolWithDistinctHashes(aPool, frame.getRecipientAddress(), 0);
      case "COLLIDING_KEYS" -> fillPoolWithCollidingHashes(aPool, frame.getRecipientAddress(), 0);
    }
    index = 0;
  }

  @TearDown(Level.Iteration)
  public void tearDown() {
    // create new frame - way of rolling back without calling rollback (big overhead)
    frame = buildFrame();
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return operation.execute(frame, null);
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator gasCalculator) {
    return gasCalculator.getTransientStoreOperationGasCost();
  }

  public static class FilledSlots extends TStoreOperationBenchmark {
    private Bytes[] keysPool;

    @Override
    public void setUp() throws Exception {
      super.setUp();
      keysPool = new Bytes[getSampleSize()];
      switch (scenario) {
        // Need to fill with offset to make sure slots are not the same
        case "RANDOM_KEYS" -> fillPool(keysPool);
        case "DISTINCT_KEYS" ->
            fillPoolWithDistinctHashes(keysPool, frame.getRecipientAddress(), getSampleSize());
        case "COLLIDING_KEYS" ->
            fillPoolWithCollidingHashes(keysPool, frame.getRecipientAddress(), getSampleSize());
      }
    }

    @Setup(Level.Invocation)
    public void fillSlots() {
      for (int i = 0; i < keysPool.length; i++) {
        frame.pushStackItem(bPool[i]);
        frame.pushStackItem(keysPool[i]);
        operation.execute(frame, null);
      }
    }

    @Benchmark
    @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
    public void rollback() {
      frame.rollback();
    }

    @TearDown(Level.Invocation)
    public void emptySlots() {
      tearDown();
    }
  }

  @Override
  protected int getSampleSize() {
    return slotCount;
  }

  private static MessageFrame buildFrame() {
    return MessageFrame.builder()
        .worldUpdater(mock(WorldUpdater.class))
        .originator(Address.ZERO)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .blockValues(mock(BlockValues.class))
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .type(MessageFrame.Type.MESSAGE_CALL)
        .initialGas(Long.MAX_VALUE)
        .address(Address.fromHexString("0x0102030405"))
        .contract(Address.ZERO)
        .inputData(Bytes32.ZERO)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(Code.EMPTY_CODE)
        .completer(__ -> {})
        .build();
  }
}
