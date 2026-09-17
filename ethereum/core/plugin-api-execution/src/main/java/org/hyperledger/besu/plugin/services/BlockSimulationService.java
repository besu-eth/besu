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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.BlockOverrides;
import org.hyperledger.besu.plugin.data.PluginBlockSimulationResult;

import java.util.List;

/** This class is a service that simulates the processing of a block */
public interface BlockSimulationService extends BesuService {

  /**
   * Simulate the processing of a block given a header, a list of transactions, and blockOverrides.
   *
   * @param blockNumber the block number
   * @param transactions the transactions to include in the block
   * @param blockOverrides the blockSimulationOverride of the block
   * @param stateOverrides state overrides of the block
   * @return the result of the simulation
   */
  PluginBlockSimulationResult simulate(
      long blockNumber,
      List<? extends Transaction> transactions,
      BlockOverrides blockOverrides,
      StateOverrideMap stateOverrides);

  /**
   * Simulate the processing of a block given a header, a list of transactions, blockOverrides, and
   * a tracer.
   *
   * @param blockNumber the block number
   * @param transactions the transactions to include in the block
   * @param blockOverrides the blockSimulationOverride of the block
   * @param stateOverrides state overrides of the block
   * @param tracer the operation tracer to use during simulation
   * @return the result of the simulation
   */
  PluginBlockSimulationResult simulate(
      long blockNumber,
      List<? extends Transaction> transactions,
      BlockOverrides blockOverrides,
      StateOverrideMap stateOverrides,
      OperationTracer tracer);

  /**
   * This method is experimental and should be used with caution. Simulate the processing of a block
   * given a header, a list of transactions, and blockOverrides and persist the WorldState
   *
   * @param blockNumber the block number
   * @param transactions the transactions to include in the block
   * @param blockOverrides block overrides for the block
   * @param stateOverrides state overrides of the block
   * @return the PluginBlockSimulationResult
   */
  @Unstable
  PluginBlockSimulationResult simulateAndPersistWorldState(
      long blockNumber,
      List<? extends Transaction> transactions,
      BlockOverrides blockOverrides,
      StateOverrideMap stateOverrides);
}
