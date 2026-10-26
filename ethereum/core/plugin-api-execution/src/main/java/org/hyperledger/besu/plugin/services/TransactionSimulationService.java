/*
 * Copyright contributors to Hyperledger Besu.
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

import org.hyperledger.besu.datatypes.CallParameter;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.data.TransactionSimulationResult;

import java.util.EnumSet;
import java.util.Optional;

/** Transaction simulation service interface */
@Unstable
public interface TransactionSimulationService extends BesuService {
  /**
   * Enumeration of simulation parameters that control validation behavior during transaction
   * simulation.
   *
   * <p>These parameters allow relaxing certain validation rules that would normally cause a
   * transaction to fail, enabling more flexible simulation scenarios for testing, debugging, or
   * analysis purposes.
   */
  enum SimulationParameters {

    /**
     * Allows the transaction to proceed even if the sender's account balance is insufficient to
     * cover the transaction value and gas costs.
     *
     * <p>When this parameter is enabled, balance validation is bypassed, allowing simulation of
     * transactions that would normally fail due to insufficient funds.
     */
    ALLOW_EXCEEDING_BALANCE,

    /**
     * Allows the transaction to proceed even if the nonce is higher than the sender's current
     * account nonce.
     *
     * <p>When this parameter is enabled, nonce validation is relaxed, allowing simulation of future
     * transactions without requiring all intermediate transactions to be processed first.
     */
    ALLOW_FUTURE_NONCE,

    /**
     * Allows the transaction to proceed even if the gas price is below the network's minimum gas
     * price requirements.
     *
     * <p>When this parameter is enabled, gas price validation is bypassed, allowing simulation of
     * transactions with artificially low gas prices that would normally be rejected by the network.
     */
    ALLOW_UNDERPRICED
  }

  /**
   * Return a simulation of what could be current pending block, it can also be passed to {@link
   * #simulate(Transaction, Optional, ProcessableBlockHeader, OperationTracer, EnumSet)}
   *
   * @return the simulated pending block header
   */
  ProcessableBlockHeader simulatePendingBlockHeader();

  /**
   * Simulate transaction execution at the block identified by the hash, with optional state
   * overrides that can be applied before the simulation.
   *
   * @param transaction the transaction to simulate
   * @param stateOverrides state overrides to apply to this simulation
   * @param blockHash hash of the block
   * @param operationTracer the tracer
   * @param isAllowExceedingBalance should ignore the sender balance during the simulation?
   * @return the result of the simulation
   */
  @Deprecated(forRemoval = true)
  default Optional<TransactionSimulationResult> simulate(
      final Transaction transaction,
      final Optional<StateOverrideMap> stateOverrides,
      final Hash blockHash,
      final OperationTracer operationTracer,
      final boolean isAllowExceedingBalance) {
    final var simulationParameters =
        isAllowExceedingBalance
            ? EnumSet.of(SimulationParameters.ALLOW_EXCEEDING_BALANCE)
            : EnumSet.noneOf(SimulationParameters.class);

    return simulate(transaction, stateOverrides, blockHash, operationTracer, simulationParameters);
  }

  /**
   * Simulate transaction execution at the block identified by the hash, with optional state
   * overrides that can be applied before the simulation.
   *
   * @param transaction the transaction to simulate
   * @param stateOverrides state overrides to apply to this simulation
   * @param blockHash hash of the block
   * @param operationTracer the tracer
   * @param simulationParameters optional set of parameters that affected the behavior of the
   *     simulation
   * @return the result of the simulation
   */
  Optional<TransactionSimulationResult> simulate(
      Transaction transaction,
      Optional<StateOverrideMap> stateOverrides,
      Hash blockHash,
      OperationTracer operationTracer,
      EnumSet<SimulationParameters> simulationParameters);

  /**
   * Simulate transaction execution at the block identified by the block header, with optional state
   * overrides that can be applied before the simulation.
   *
   * @param transaction the transaction to simulate
   * @param stateOverrides state overrides to apply to this simulation
   * @param processableBlockHeader block header to simulate on pending block
   * @param operationTracer the tracer
   * @param isAllowExceedingBalance should ignore the sender balance during the simulation?
   * @param isAllowFutureNonce should skip strict check on sequential nonce?
   * @return the result of the simulation
   */
  @Deprecated(forRemoval = true)
  default Optional<TransactionSimulationResult> simulate(
      final Transaction transaction,
      final Optional<StateOverrideMap> stateOverrides,
      final ProcessableBlockHeader processableBlockHeader,
      final OperationTracer operationTracer,
      final boolean isAllowExceedingBalance,
      final boolean isAllowFutureNonce) {
    final var simulationParameters = EnumSet.noneOf(SimulationParameters.class);
    if (isAllowExceedingBalance) {
      simulationParameters.add(SimulationParameters.ALLOW_EXCEEDING_BALANCE);
    }
    if (isAllowFutureNonce) {
      simulationParameters.add(SimulationParameters.ALLOW_FUTURE_NONCE);
    }
    return simulate(
        transaction, stateOverrides, processableBlockHeader, operationTracer, simulationParameters);
  }

  /**
   * Simulate transaction execution at the block identified by the block header, with optional state
   * overrides that can be applied before the simulation.
   *
   * @param transaction the transaction to simulate
   * @param stateOverrides state overrides to apply to this simulation
   * @param processableBlockHeader block header to simulate on pending block
   * @param operationTracer the tracer
   * @param simulationParameters optional set of parameters that affected the behavior of the
   *     simulation
   * @return the result of the simulation
   */
  Optional<TransactionSimulationResult> simulate(
      Transaction transaction,
      Optional<StateOverrideMap> stateOverrides,
      ProcessableBlockHeader processableBlockHeader,
      OperationTracer operationTracer,
      EnumSet<SimulationParameters> simulationParameters);

  /**
   * Simulate transaction (specified by call parameters) execution at the block identified by the
   * block header, with optional state overrides that can be applied before the simulation.
   *
   * @param callParameters call parameters
   * @param stateOverrides state overrides to apply to this simulation
   * @param processableBlockHeader block header to simulate
   * @param operationTracer the tracer
   * @param simulationParameters optional set of parameters that affected the behavior of the
   *     simulation
   * @return the result of the simulation
   */
  Optional<TransactionSimulationResult> simulate(
      CallParameter callParameters,
      Optional<StateOverrideMap> stateOverrides,
      ProcessableBlockHeader processableBlockHeader,
      OperationTracer operationTracer,
      EnumSet<SimulationParameters> simulationParameters);

  /**
   * Simulate a call on the world state after the given block, using that block's header as the
   * execution environment, as {@code eth_call} does at that block. Fields that are not set in the
   * call parameters keep the simulator defaults: the zero address as sender, the simulator gas cap,
   * the sender's current nonce, and non-strict gas price checks.
   *
   * <p>Validation is relaxed only as {@code simulationParameters} asks. To validate as {@code
   * eth_call} does, pass {@link SimulationParameters#ALLOW_FUTURE_NONCE}, and also {@link
   * SimulationParameters#ALLOW_EXCEEDING_BALANCE} when the call sets no gas price and no non-zero
   * fee per gas.
   *
   * <p>A present result wraps the transaction actually executed, so {@link
   * TransactionSimulationResult#getGasEstimate()} reflects the gas used by that execution.
   *
   * <p>Unlike {@link #simulate(Transaction, Optional, Hash, OperationTracer, EnumSet)}, which
   * returns an invalid result with reason {@code BLOCK_NOT_FOUND} for an unknown block, this method
   * returns empty: there is no transaction to wrap until the call has been executed.
   *
   * @param callParameters the call parameters
   * @param stateOverrides state overrides to apply to this simulation
   * @param blockHash the hash of the block whose post-state and header to use
   * @param operationTracer the tracer
   * @param simulationParameters the simulation parameters
   * @return the result of the simulation; empty if the block is unknown, its world state is
   *     unavailable, or the simulation could not be executed
   * @throws IllegalArgumentException if the state overrides are invalid, for example when they move
   *     a precompile from an address that is not a precompile
   */
  default Optional<TransactionSimulationResult> simulate(
      final CallParameter callParameters,
      final Optional<StateOverrideMap> stateOverrides,
      final Hash blockHash,
      final OperationTracer operationTracer,
      final EnumSet<SimulationParameters> simulationParameters) {
    throw new UnsupportedOperationException();
  }
}
