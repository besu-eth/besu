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

import static org.hyperledger.besu.ethereum.vm.operations.v2.BlockHashBenchmarkChain.CURRENT_BLOCK;
import static org.hyperledger.besu.ethereum.vm.operations.v2.BlockHashBenchmarkChain.LOOKBACK;

import org.hyperledger.besu.ethereum.vm.operations.GasCostBenchmark;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.PetersburgGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.BlockHashOperationV2;

import java.util.Random;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.infra.BenchmarkParams;

public class BlockHashOperationBenchmarkV2 extends UnaryOperationBenchmarkV2
    implements GasCostBenchmark {

  public enum Scenario {
    /** Random block within the lookback window. */
    VALID,
    /** Random block at or after the current block. */
    CURRENT_OR_FUTURE,
    /** Random block older than the lookback window. */
    OUTSIDE_LOOKBACK,
    /** Random block number with a non-zero upper limb. */
    TOO_LARGE
  }

  @Param public Scenario scenario;

  private BlockHashOperationV2 operation;

  @Override
  public void setUp() {
    operation = new BlockHashOperationV2(new PetersburgGasCalculator());
    final BlockHashBenchmarkChain chain = BlockHashBenchmarkChain.create();
    frame =
        BenchmarkHelperV2.messageCallFrameBuilder()
            .blockValues(chain.currentHeader())
            .blockHashLookup(chain.blockHashLookup())
            .build();
    aPool = requestedBlockPool(scenario, SAMPLE_SIZE);
    index = 0;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return operation.execute(frame, null);
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator gasCalculator) {
    return gasCalculator.getBlockHashOperationGasCost();
  }

  /**
   * Creates the seeded pool of requested block numbers for a scenario. Shared with the V1 benchmark
   * so both see the same inputs.
   *
   * @param scenario the scenario
   * @param size the pool size
   * @return the pool of requested block numbers
   */
  static UInt256[] requestedBlockPool(final Scenario scenario, final int size) {
    final Random random = new Random(BenchmarkHelperV2.SEED);
    final UInt256[] pool = new UInt256[size];
    for (int i = 0; i < pool.length; i++) {
      pool[i] =
          switch (scenario) {
            case VALID ->
                UInt256.fromLong(random.nextLong(CURRENT_BLOCK - LOOKBACK, CURRENT_BLOCK));
            case CURRENT_OR_FUTURE ->
                UInt256.fromLong(random.nextLong(CURRENT_BLOCK, CURRENT_BLOCK + LOOKBACK));
            case OUTSIDE_LOOKBACK -> UInt256.fromLong(random.nextLong(CURRENT_BLOCK - LOOKBACK));
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
