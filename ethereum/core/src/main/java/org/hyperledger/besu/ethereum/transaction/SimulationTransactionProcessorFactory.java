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
package org.hyperledger.besu.ethereum.transaction;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallException;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.processor.SimulationMessageCallProcessor;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SimulationTransactionProcessorFactory {

  private final ProtocolSchedule protocolSchedule;

  /**
   * Creates a factory capable of producing transaction processors.
   *
   * @param protocolSchedule the protocol schedule used for creating processors
   */
  public SimulationTransactionProcessorFactory(final ProtocolSchedule protocolSchedule) {
    this.protocolSchedule = protocolSchedule;
  }

  /**
   * Creates a transaction processor, optionally applying state overrides.
   *
   * @param processableHeader the block header to process transactions against
   * @param maybeStateOverrides optional state overrides for simulation
   * @return a transaction processor, with overrides applied if provided
   */
  public MainnetTransactionProcessor getTransactionProcessor(
      final ProcessableBlockHeader processableHeader,
      final Optional<StateOverrideMap> maybeStateOverrides) {

    MainnetTransactionProcessor baseProcessor =
        protocolSchedule.getByBlockHeader(processableHeader).getTransactionProcessor();

    Map<Address, Address> precompileOverrides =
        maybeStateOverrides.map(this::extractPrecompileAddressOverrides).orElse(Map.of());

    return createProcessor(baseProcessor, precompileOverrides);
  }

  private Map<Address, Address> extractPrecompileAddressOverrides(
      final StateOverrideMap stateOverrides) {
    return stateOverrides.entrySet().stream()
        .filter(entry -> entry.getValue().getMovePrecompileToAddress().isPresent())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> entry.getValue().getMovePrecompileToAddress().get()));
  }

  private MainnetTransactionProcessor createProcessor(
      final MainnetTransactionProcessor baseProcessor,
      final Map<Address, Address> precompileAddressOverrides) {

    if (precompileAddressOverrides.isEmpty()) {
      return baseProcessor;
    }

    return MainnetTransactionProcessor.builder()
        .populateFrom(baseProcessor)
        .messageCallProcessor(
            new SimulationMessageCallProcessor(
                baseProcessor.getMessageCallProcessor(),
                originalPrecompileRegistry ->
                    overridePrecompileAddresses(
                        originalPrecompileRegistry, precompileAddressOverrides)))
        .build();
  }

  /**
   * Creates a new PrecompileContractRegistry with the specified address overrides.
   *
   * @param originalRegistry the original precompile contract registry
   * @param precompileOverrides the address overrides
   * @return a new PrecompileContractRegistry with the overrides applied
   * @throws IllegalArgumentException if an override address does not exist in the original registry
   */
  public static PrecompileContractRegistry overridePrecompileAddresses(
      final PrecompileContractRegistry originalRegistry,
      final Map<Address, Address> precompileOverrides) {

    PrecompileContractRegistry newRegistry = new PrecompileContractRegistry();
    Set<Address> originalAddresses = originalRegistry.getPrecompileAddresses();

    precompileOverrides.forEach(
        (oldAddress, newAddress) -> {
          if (!originalAddresses.contains(oldAddress)) {
            throw new BlockStateCallException(
                "Address " + oldAddress + " is not a precompile.",
                BlockStateCallError.INVALID_PRECOMPILE_ADDRESS);
          }
          newRegistry.put(newAddress, originalRegistry.get(oldAddress));
        });

    originalAddresses.stream()
        .filter(originalAddress -> !precompileOverrides.containsKey(originalAddress))
        .forEach(
            originalAddress ->
                newRegistry.put(originalAddress, originalRegistry.get(originalAddress)));

    return newRegistry;
  }
}
