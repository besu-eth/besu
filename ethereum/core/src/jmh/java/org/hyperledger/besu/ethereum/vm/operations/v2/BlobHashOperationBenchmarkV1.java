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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.operation.BlobHashOperation;

import java.util.List;
import java.util.Optional;
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

@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BlobHashOperationBenchmarkV1 {

  @Param public BlobHashOperationBenchmarkV2.Scenario scenario;

  private BlobHashOperation operation;
  private MessageFrame frame;
  private Bytes32 index;

  @Setup
  public void setup() {
    operation = new BlobHashOperation(new CancunGasCalculator());
    final Optional<List<VersionedHash>> versionedHashes =
        scenario == BlobHashOperationBenchmarkV2.Scenario.NO_BLOBS
            ? Optional.empty()
            : Optional.of(BlobHashOperationBenchmarkV2.VERSIONED_HASHES);
    frame =
        BenchmarkHelperV2.createMessageCallFrame(
            false, new BlockValues() {}, (__, ___) -> Hash.ZERO, versionedHashes);
    index =
        switch (scenario) {
          case VALID -> Bytes32.ZERO;
          case MAX_VALID -> Bytes32.fromHexString("0x05");
          case NO_BLOBS -> Bytes32.ZERO;
          case OUT_OF_BOUNDS -> Bytes32.fromHexString("0x06");
          case TOO_LARGE -> Bytes32.fromHexString("0x010000000000000000");
        };
  }

  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    frame.pushStackItem(index);
    blackhole.consume(operation.execute(frame, null));
    frame.popStackItem();
  }
}
