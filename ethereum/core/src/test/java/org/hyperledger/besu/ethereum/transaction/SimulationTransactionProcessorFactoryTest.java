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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.transaction.SimulationTransactionProcessorFactory.overridePrecompileAddresses;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallException;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.precompile.PrecompiledContract;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.processor.SimulationMessageCallProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SimulationTransactionProcessorFactoryTest {
  private PrecompiledContract originalPrecompiledContract;
  private PrecompileContractRegistry originalRegistry;
  private MessageCallProcessor originalProcessor;

  private static final Address ORIGINAL_ADDRESS_1 = Address.fromHexString("0x01");
  private static final Address ORIGINAL_ADDRESS_2 = Address.fromHexString("0x02");

  private static final Address OVERRIDE_ADDRESS = Address.fromHexString("0x03");
  private static final Address NON_EXISTENT_ADDRESS = Address.fromHexString("0x04");

  private SimulationTransactionProcessorFactory factory;
  private final Address originalPrecompileAddress = Address.fromHexString("0x1");
  private final Address newPrecompileAddress = Address.fromHexString("0x2");
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);

  @BeforeEach
  void setUp() {
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    PrecompileContractRegistry precompileContractRegistry = new PrecompileContractRegistry();
    precompileContractRegistry.put(originalPrecompileAddress, mock(PrecompiledContract.class));
    MainnetTransactionProcessor mainnetTransactionProcessor =
        MainnetTransactionProcessor.builder()
            .messageCallProcessor(
                new MessageCallProcessor(mock(EVM.class), precompileContractRegistry))
            .build();
    when(protocolSpec.getTransactionProcessor()).thenReturn(mainnetTransactionProcessor);
    factory = new SimulationTransactionProcessorFactory(protocolSchedule);

    originalPrecompiledContract = mock(PrecompiledContract.class);
    originalRegistry = new PrecompileContractRegistry();
    originalRegistry.put(ORIGINAL_ADDRESS_1, originalPrecompiledContract);
    originalRegistry.put(ORIGINAL_ADDRESS_2, originalPrecompiledContract);
    originalProcessor = new MessageCallProcessor(null, originalRegistry);
  }

  @Test
  void shouldReturnProcessorWithOriginalPrecompileAddressesIfNoOverrides() {
    MainnetTransactionProcessor simulationTransactionProcessor =
        factory.getTransactionProcessor(null, Optional.empty());
    Set<Address> precompileAddresses =
        simulationTransactionProcessor.getMessageCallProcessor().getPrecompileAddresses();

    assertThat(precompileAddresses).containsExactlyInAnyOrder(originalPrecompileAddress);
  }

  @Test
  void shouldReturnProcessorWithNewPrecompileAddressesWithOverrides() {
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);

    StateOverrideMap stateOverrideMap = new StateOverrideMap();
    stateOverrideMap.put(
        originalPrecompileAddress,
        new StateOverride.Builder().withMovePrecompileToAddress(newPrecompileAddress).build());

    MainnetTransactionProcessor simulationTransactionProcessor =
        factory.getTransactionProcessor(null, Optional.of(stateOverrideMap));
    Set<Address> precompileAddresses =
        simulationTransactionProcessor.getMessageCallProcessor().getPrecompileAddresses();

    assertThat(precompileAddresses).containsExactlyInAnyOrder(newPrecompileAddress);
  }

  @Test
  void shouldPreserveOriginalPrecompilesWhenNoOverridesProvided() {
    HashMap<Address, Address> precompileOverrides = new HashMap<>();
    SimulationMessageCallProcessor processor =
        new SimulationMessageCallProcessor(originalProcessor, createSupplier(precompileOverrides));

    assertNotNull(processor);
    Assertions.assertThat(processor.getPrecompileAddresses())
        .containsExactlyInAnyOrder(ORIGINAL_ADDRESS_1, ORIGINAL_ADDRESS_2);
    Assertions.assertThat(processor.getPrecompiles().get(ORIGINAL_ADDRESS_1))
        .isEqualTo(originalPrecompiledContract);
  }

  @Test
  void shouldCorrectlyApplyOverrides() {
    Map<Address, Address> precompileOverrides = new HashMap<>();
    precompileOverrides.put(ORIGINAL_ADDRESS_1, OVERRIDE_ADDRESS);

    SimulationMessageCallProcessor processor =
        new SimulationMessageCallProcessor(originalProcessor, createSupplier(precompileOverrides));

    assertNotNull(processor);
    Assertions.assertThat(
            processor.getPrecompiles().getPrecompileAddresses().contains(ORIGINAL_ADDRESS_1))
        .isFalse();
    Assertions.assertThat(processor.getPrecompiles().get(OVERRIDE_ADDRESS))
        .isEqualTo(originalPrecompiledContract);
  }

  @Test
  void shouldHandleBothOverriddenAndNonOverriddenPrecompiles() {
    PrecompiledContract precompiledContract2 = mock(PrecompiledContract.class);
    originalRegistry.put(OVERRIDE_ADDRESS, precompiledContract2);

    Map<Address, Address> precompileOverrides = new HashMap<>();
    precompileOverrides.put(ORIGINAL_ADDRESS_1, NON_EXISTENT_ADDRESS);

    SimulationMessageCallProcessor processor =
        new SimulationMessageCallProcessor(originalProcessor, createSupplier(precompileOverrides));

    assertNotNull(processor);
    Assertions.assertThat(
            processor.getPrecompiles().getPrecompileAddresses().contains(ORIGINAL_ADDRESS_1))
        .isFalse();
    Assertions.assertThat(processor.getPrecompiles().get(NON_EXISTENT_ADDRESS))
        .isEqualTo(originalPrecompiledContract);
    Assertions.assertThat(processor.getPrecompiles().get(OVERRIDE_ADDRESS))
        .isEqualTo(precompiledContract2);
  }

  @Test
  void shouldThrowIfOverrideAddressNotFoundInOriginalRegistry() {
    Map<Address, Address> precompileOverrides = new HashMap<>();
    precompileOverrides.put(NON_EXISTENT_ADDRESS, OVERRIDE_ADDRESS);

    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () ->
                new SimulationMessageCallProcessor(
                    originalProcessor, createSupplier(precompileOverrides)));

    assertThat(exception.getError()).isEqualTo(BlockStateCallError.INVALID_PRECOMPILE_ADDRESS);
    Assertions.assertThat(exception.getMessage())
        .contains("Address " + NON_EXISTENT_ADDRESS + " is not a precompile.");
  }

  @Test
  void shouldAllowDuplicateNewAddressWithLastOverrideWinning() {
    PrecompiledContract precompiledContract2 = mock(PrecompiledContract.class);
    originalRegistry.put(ORIGINAL_ADDRESS_2, precompiledContract2);

    Map<Address, Address> precompileOverrides = new HashMap<>();
    precompileOverrides.put(ORIGINAL_ADDRESS_1, OVERRIDE_ADDRESS);
    precompileOverrides.put(ORIGINAL_ADDRESS_2, OVERRIDE_ADDRESS); // Duplicate target allowed

    SimulationMessageCallProcessor processor =
        new SimulationMessageCallProcessor(originalProcessor, createSupplier(precompileOverrides));

    assertNotNull(processor);
    Assertions.assertThat(processor.getPrecompiles().getPrecompileAddresses())
        .contains(OVERRIDE_ADDRESS);
  }

  @Test
  void shouldAllowOriginalAddressReusedAsNewAddress() {
    Map<Address, Address> precompileOverrides = new HashMap<>();
    precompileOverrides.put(ORIGINAL_ADDRESS_1, ORIGINAL_ADDRESS_2);

    SimulationMessageCallProcessor processor =
        new SimulationMessageCallProcessor(originalProcessor, createSupplier(precompileOverrides));

    assertNotNull(processor);
    Assertions.assertThat(processor.getPrecompiles().getPrecompileAddresses())
        .contains(ORIGINAL_ADDRESS_2);
  }

  private Function<PrecompileContractRegistry, PrecompileContractRegistry> createSupplier(
      final Map<Address, Address> overrides) {
    return (originalRegistry) -> overridePrecompileAddresses(originalRegistry, overrides);
  }
}
