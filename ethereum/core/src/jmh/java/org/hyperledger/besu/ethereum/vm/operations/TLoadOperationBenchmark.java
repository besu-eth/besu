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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.TLoadOperation;
import org.hyperledger.besu.evm.operation.TStoreOperation;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

public class TLoadOperationBenchmark extends UnaryOperationBenchmark implements GasCostBenchmark {
  TLoadOperation operation;

  @Param({"DISTINCT_KEYS", "COLLIDING_KEYS"})
  private String scenario;

  @Param({"1000", "10000", "150000"})
  int slotCount;

  @Override
  public void setUp() {
    operation = new TLoadOperation(new CancunGasCalculator());
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
    aPool = new Bytes[SAMPLE_SIZE]; // slot keys
    Bytes[] valuePool = new Bytes[SAMPLE_SIZE]; // values;

    BenchmarkHelper.fillPool(valuePool);
    int unique = Math.min(slotCount, SAMPLE_SIZE);
    switch (scenario) {
      case "DISTINCT_KEYS" -> fillDistinct(aPool, unique);
      case "COLLIDING_KEYS" -> fillColliding(aPool, unique);
    }

    fillFrame(aPool, valuePool);
    index = 0;
  }

  @TearDown
  public void rollback() {
    frame.rollback();
  }

  private void fillFrame(final Bytes[] keyPool, final Bytes[] valuePool) {
    TStoreOperation tStoreOperation = new TStoreOperation(new CancunGasCalculator());
    for (int i = 0; i < Math.min(slotCount, keyPool.length); i++) {
      frame.pushStackItem(valuePool[i]);
      frame.pushStackItem(keyPool[i]);
      tStoreOperation.execute(frame, null);
    }
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return operation.execute(frame, null);
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator gasCalculator) {
    return gasCalculator.getTransientLoadOperationGasCost();
  }

  private static void fillDistinct(final Bytes[] pool, final int unique) {
    for (int i = 0; i < pool.length; i++) {
      pool[i] = UInt256.valueOf(i % unique).toBytes();
    }
  }

  private static void fillColliding(final Bytes[] pool, final int unique) {
    for (int i = 0; i < pool.length; i++) {
      pool[i] = collidingSlot(i % unique);
    }
  }

  private static Bytes32 collidingSlot(final int index) {
    int high = index / 256;
    int low = index % 256;
    byte[] bytes = new byte[32];
    bytes[28] = (byte) high;
    bytes[29] = (byte) (-31 * high);
    bytes[30] = (byte) low;
    bytes[31] = (byte) (-31 * low);
    return Bytes32.wrap(bytes);
  }
}
