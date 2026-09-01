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
import org.hyperledger.besu.evm.operation.EqOperation;
import org.hyperledger.besu.evm.operation.Operation;

import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.BenchmarkParams;

public class EqOperationBenchmark extends BinaryOperationBenchmark
    implements GasCostBenchmark {

  // public because of jmh code generator
  public enum MODE {
    EMPTY_INPUTS,
    ONE_EMPTY_INPUT,
    BYTES1_EQUAL,
    BYTES1_NOT_EQUAL,
    BYTES1_EQUAL_WITH_ZEROS,
    BYTES1_NOT_EQUAL_WITH_ZEROS,
    BYTES16_EQUAL,
    BYTES16_NOT_EQUAL,
    BYTES16_EQUAL_WITH_ZEROS,
    BYTES16_NOT_EQUAL_WITH_ZEROS,
    BYTES32_NOT_EQUAL_MOST_SIGNIFICANT,
    BYTES32_NOT_EQUAL_LEAST_SIGNIFICANT,
    BYTES32_RANDOM,
    BYTES30_BYTES16_RANDOM,
    BYTES32_EQUAL_BYTE_REPEAT
  }

  @Param private MODE mode;

  @Setup
  @Override
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();

    aPool = new Bytes[SAMPLE_SIZE];
    bPool = new Bytes[SAMPLE_SIZE];
    switch (mode) {
      case EMPTY_INPUTS:
        Arrays.fill(aPool, Bytes.EMPTY);
        Arrays.fill(bPool, Bytes.EMPTY);
        break;
      case ONE_EMPTY_INPUT:
        Arrays.fill(aPool, Bytes.EMPTY);
        BenchmarkHelper.fillPool(bPool, () -> 16);
        bPool = Arrays.stream(bPool).map(Bytes32::leftPad).toArray(Bytes[]::new);
        break;
      case BYTES1_EQUAL:
        BenchmarkHelper.fillPool(aPool, () -> 1);
        bPool =
            Arrays.stream(aPool)
                .map(bytes -> Bytes.wrap(bytes.toArrayUnsafe()))
                .toArray(Bytes[]::new);
        break;
      case BYTES1_NOT_EQUAL:
        BenchmarkHelper.fillPools(
            aPool, bPool, () -> 1, () -> 1, (value1, value2) -> !value1.equals(value2));
        break;
      case BYTES1_EQUAL_WITH_ZEROS:
        BenchmarkHelper.fillPool(aPool, () -> 1);
        aPool = Arrays.stream(aPool).map(Bytes32::leftPad).toArray(Bytes[]::new);
        bPool =
            Arrays.stream(aPool)
                .map(bytes -> Bytes.wrap(bytes.toArrayUnsafe()))
                .toArray(Bytes[]::new);
        break;
      case BYTES1_NOT_EQUAL_WITH_ZEROS:
        BenchmarkHelper.fillPools(
            aPool, bPool, () -> 1, () -> 1, (value1, value2) -> !value1.equals(value2));
        aPool = Arrays.stream(aPool).map(Bytes32::leftPad).toArray(Bytes[]::new);
        bPool = Arrays.stream(bPool).map(Bytes32::leftPad).toArray(Bytes[]::new);
        break;
      case BYTES16_EQUAL:
        BenchmarkHelper.fillPool(aPool, () -> 16);
        bPool =
            Arrays.stream(aPool)
                .map(bytes -> Bytes.wrap(bytes.toArrayUnsafe()))
                .toArray(Bytes[]::new);
        break;
      case BYTES16_NOT_EQUAL:
        BenchmarkHelper.fillPools(
            aPool, bPool, () -> 16, () -> 16, (value1, value2) -> !value1.equals(value2));
        break;
      case BYTES16_EQUAL_WITH_ZEROS:
        BenchmarkHelper.fillPool(aPool, () -> 16);
        aPool = Arrays.stream(aPool).map(Bytes32::leftPad).toArray(Bytes[]::new);
        bPool =
            Arrays.stream(aPool)
                .map(bytes -> Bytes.wrap(bytes.toArrayUnsafe()))
                .toArray(Bytes[]::new);
        break;
      case BYTES16_NOT_EQUAL_WITH_ZEROS:
        BenchmarkHelper.fillPools(
            aPool, bPool, () -> 16, () -> 16, (value1, value2) -> !value1.equals(value2));
        aPool = Arrays.stream(aPool).map(Bytes32::leftPad).toArray(Bytes[]::new);
        bPool = Arrays.stream(bPool).map(Bytes32::leftPad).toArray(Bytes[]::new);
        break;
      case BYTES32_NOT_EQUAL_MOST_SIGNIFICANT:
        BenchmarkHelper.fillPool(aPool, () -> 32, value -> value.get(0) != 0);
        bPool =
            Arrays.stream(aPool)
                .map(
                    bytes -> {
                      final MutableBytes mutableBytes = bytes.mutableCopy();
                      byte firstByte = bytes.get(0);
                      mutableBytes.set(0, (byte) (firstByte > 0 ? firstByte + 1 : firstByte - 1));
                      return mutableBytes;
                    })
                .toArray(Bytes[]::new);
        break;
      case BYTES32_NOT_EQUAL_LEAST_SIGNIFICANT:
        BenchmarkHelper.fillPool(aPool, () -> 32, value -> value.get(0) != 0);
        bPool =
            Arrays.stream(aPool)
                .map(
                    bytes -> {
                      final MutableBytes mutableBytes = bytes.mutableCopy();
                      byte lastByte = bytes.get(bytes.size() - 1);
                      mutableBytes.set(
                          bytes.size() - 1, (byte) (lastByte > 0 ? lastByte + 1 : lastByte - 1));
                      return mutableBytes;
                    })
                .toArray(Bytes[]::new);
        break;
      case BYTES32_RANDOM:
        BenchmarkHelper.fillPool(aPool, () -> 32, value -> value.get(0) != 0);
        BenchmarkHelper.fillPool(bPool, () -> 32, value -> value.get(0) != 0);
        break;
      case BYTES30_BYTES16_RANDOM:
        BenchmarkHelper.fillPool(aPool, () -> 30, value -> value.get(0) != 0);
        BenchmarkHelper.fillPool(bPool, () -> 16, value -> value.get(0) != 0);
        break;
      case BYTES32_EQUAL_BYTE_REPEAT:
        BenchmarkHelper.fillPool(aPool, () -> 1);
        aPool =
            Arrays.stream(aPool)
                .map(
                    bytes -> {
                      final byte[] newPool = new byte[32];
                      Arrays.fill(newPool, bytes.get(0));
                      return Bytes.wrap(newPool);
                    })
                .toArray(Bytes[]::new);
        bPool =
            Arrays.stream(aPool)
                .map(bytes -> Bytes.wrap(bytes.toArrayUnsafe()))
                .toArray(Bytes[]::new);
    }
    index = 0;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return EqOperation.staticOperation(frame);
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator calc) {
    return new EqOperation(calc).getGasCost();
  }
}
