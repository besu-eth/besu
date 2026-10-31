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
import org.hyperledger.besu.ethereum.vm.operations.UnaryOperationBenchmark;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.BlobHashOperation;
import org.hyperledger.besu.evm.operation.Operation;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

// Same warmup as the V2 benchmark, so V1 and V2 results are comparable.
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
public class BlobHashOperationBenchmarkV1 extends UnaryOperationBenchmark
    implements GasCostBenchmark {

  @Param public BlobHashOperationBenchmarkV2.Scenario scenario;

  private BlobHashOperation operation;

  @Override
  public void setUp() {
    operation = new BlobHashOperation(new CancunGasCalculator());
    final Optional<List<VersionedHash>> versionedHashes =
        scenario == BlobHashOperationBenchmarkV2.Scenario.NO_BLOBS
            ? Optional.empty()
            : Optional.of(BlobHashOperationBenchmarkV2.VERSIONED_HASHES);
    frame =
        BenchmarkHelperV2.messageCallFrameBuilder()
            .enableEvmV2(false)
            .versionedHashes(versionedHashes)
            .build();
    aPool =
        BenchmarkHelperV2.toBytes32Pool(
            BlobHashOperationBenchmarkV2.blobIndexPool(scenario, getSampleSize()));
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
}
