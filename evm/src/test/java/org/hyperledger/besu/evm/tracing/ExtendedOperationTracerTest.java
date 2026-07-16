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
package org.hyperledger.besu.evm.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Collections;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExtendedOperationTracerTest {

  @Mock MessageFrame frame;
  @Mock WorldUpdater worldUpdater;
  @Mock MutableAccount mutableAccount;

  @BeforeEach
  void setUp() {
    when(frame.getOutputData()).thenReturn(Bytes.EMPTY);
    when(frame.getRemainingGas()).thenReturn(1L);
    // futureEips is pre-Amsterdam, so the state-gas charge is 0; the mock must not refuse it.
    when(frame.consumeStateGas(0L)).thenReturn(true);

    when(frame.getWorldUpdater()).thenReturn(worldUpdater);
    when(worldUpdater.getOrCreate(any())).thenReturn(mutableAccount);
  }

  @Test
  void shouldCallTraceAccountCreationResultIfIsExtendedTracing() {
    EVM evm = MainnetEVMs.futureEips(EvmConfiguration.DEFAULT);
    final ContractCreationProcessor contractCreationProcessor =
        new ContractCreationProcessor(evm, false, Collections.emptyList(), 0);

    final ExtendedOperationTracer tracer = new ExtendedOperationTracer();
    contractCreationProcessor.codeSuccess(frame, tracer);

    // traceAccountCreationResult has been called and values have been set
    assertThat(tracer.frame).isNotNull();
    assertThat(tracer.haltReason).isEmpty();
  }

  @Test
  void shouldNotCallTraceAccountCreationResultIfIsNotExtendedTracing() {
    EVM evm = MainnetEVMs.futureEips(EvmConfiguration.DEFAULT);
    final ContractCreationProcessor contractCreationProcessor =
        new ContractCreationProcessor(evm, false, Collections.emptyList(), 0);

    final DefaultOperationTracer tracer = new DefaultOperationTracer();
    contractCreationProcessor.codeSuccess(frame, tracer);

    // traceAccountCreationResult has not been called and values are still null
    assertThat(tracer.frame).isNull();
    assertThat(tracer.haltReason).isNull();
  }

  static class DefaultOperationTracer implements OperationTracer {
    public MessageFrame frame = null;
    public Optional<ExceptionalHaltReason> haltReason = null;

    @Override
    public void traceAccountCreationResult(
        final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {
      this.frame = frame;
      this.haltReason = haltReason;
    }
  }

  static class ExtendedOperationTracer extends DefaultOperationTracer {
    @Override
    public boolean isExtendedTracing() {
      return true;
    }
  }
}
