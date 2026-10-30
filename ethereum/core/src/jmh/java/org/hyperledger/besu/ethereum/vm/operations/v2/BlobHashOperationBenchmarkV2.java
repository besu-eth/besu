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

import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.vm.operations.GasCostBenchmark;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.BlobHashOperationV2;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.infra.BenchmarkParams;

public class BlobHashOperationBenchmarkV2 extends UnaryOperationBenchmarkV2
    implements GasCostBenchmark {

  static final List<VersionedHash> VERSIONED_HASHES =
      List.of(
          VersionedHash.fromHexString(
              "0x01cafebabeb0b0facedeadbeefbeef0001cafebabeb0b0facedeadbeefbeef00"),
          VersionedHash.fromHexString(
              "0x01cafebabeb0b0facedeadbeefbeef0001cafebabeb0b0facedeadbeefbeef01"),
          VersionedHash.fromHexString(
              "0x01cafebabeb0b0facedeadbeefbeef0001cafebabeb0b0facedeadbeefbeef02"),
          VersionedHash.fromHexString(
              "0x01cafebabeb0b0facedeadbeefbeef0001cafebabeb0b0facedeadbeefbeef03"),
          VersionedHash.fromHexString(
              "0x01cafebabeb0b0facedeadbeefbeef0001cafebabeb0b0facedeadbeefbeef04"),
          VersionedHash.fromHexString(
              "0x01cafebabeb0b0facedeadbeefbeef0001cafebabeb0b0facedeadbeefbeef05"));

  public enum Scenario {
    /** Random index of an existing versioned hash. */
    VALID,
    /** Random index that fits in an int but is past the last versioned hash. */
    OUT_OF_BOUNDS,
    /** Random index with a non-zero upper limb. */
    TOO_LARGE,
    /** Random in-range index, but the transaction has no versioned hashes. */
    NO_BLOBS
  }

  @Param public Scenario scenario;

  private BlobHashOperationV2 operation;

  @Override
  public void setUp() {
    operation = new BlobHashOperationV2(new CancunGasCalculator());
    final Optional<List<VersionedHash>> versionedHashes =
        scenario == Scenario.NO_BLOBS ? Optional.empty() : Optional.of(VERSIONED_HASHES);
    frame = BenchmarkHelperV2.messageCallFrameBuilder().versionedHashes(versionedHashes).build();
    aPool = blobIndexPool(scenario, SAMPLE_SIZE);
    index = 0;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return operation.execute(frame, null);
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator gasCalculator) {
    return gasCalculator.getVeryLowTierGasCost();
  }

  /**
   * Creates the seeded pool of blob indexes for a scenario. Shared with the V1 benchmark so both
   * see the same inputs.
   *
   * @param scenario the scenario
   * @param size the pool size
   * @return the pool of blob indexes
   */
  static UInt256[] blobIndexPool(final Scenario scenario, final int size) {
    final Random random = new Random(BenchmarkHelperV2.SEED);
    final int hashCount = VERSIONED_HASHES.size();
    final UInt256[] pool = new UInt256[size];
    for (int i = 0; i < pool.length; i++) {
      pool[i] =
          switch (scenario) {
            case VALID, NO_BLOBS -> UInt256.fromLong(random.nextInt(hashCount));
            case OUT_OF_BOUNDS -> UInt256.fromLong(random.nextInt(hashCount, Integer.MAX_VALUE));
            case TOO_LARGE ->
                new UInt256(
                    random.nextLong(),
                    random.nextLong(),
                    random.nextLong() | 1L,
                    random.nextLong());
          };
    }
    return pool;
  }
}
