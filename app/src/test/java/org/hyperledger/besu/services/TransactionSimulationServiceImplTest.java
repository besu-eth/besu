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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.plugin.services.TransactionSimulationService.SimulationParameters;

import java.util.EnumSet;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link TransactionSimulationServiceImpl} throws a clear {@link IllegalStateException}
 * when simulation methods are called before {@link TransactionSimulationServiceImpl#init} — rather
 * than a confusing {@link NullPointerException}.
 *
 * <p>Like {@code BlockchainServiceImpl}, this service is registered early in the plugin lifecycle
 * (REGISTERING phase) but is only fully initialized in the STARTED phase.
 */
class TransactionSimulationServiceImplTest {

  private TransactionSimulationServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new TransactionSimulationServiceImpl();
  }

  @Test
  void simulatePendingBlockHeader_beforeInit_throwsIllegalStateException() {
    assertThatThrownBy(() -> service.simulatePendingBlockHeader())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("TransactionSimulationService is not yet fully initialized")
        .hasMessageContaining("start()");
  }

  @Test
  void simulate_byBlockHash_beforeInit_throwsIllegalStateException() {
    final Transaction tx = mock(Transaction.class);
    assertThatThrownBy(
            () ->
                service.simulate(
                    tx,
                    Optional.empty(),
                    Hash.ZERO,
                    org.hyperledger.besu.evm.tracing.OperationTracer.NO_TRACING,
                    EnumSet.noneOf(SimulationParameters.class)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("TransactionSimulationService is not yet fully initialized");
  }

  @Test
  void noExceptionAfterInit() {
    final Blockchain blockchain = mock(Blockchain.class);
    final TransactionSimulator simulator = mock(TransactionSimulator.class);

    // After init, calls should delegate to the simulator (no IllegalStateException)
    service.init(blockchain, simulator);

    // Just verify no exception is thrown on the check itself (mock returns null from simulator)
    org.mockito.Mockito.when(simulator.simulatePendingBlockHeader()).thenReturn(null);
    service.simulatePendingBlockHeader(); // should not throw IllegalStateException
  }
}
