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
package org.hyperledger.besu.evm.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.precompile.PrecompiledContract;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Set;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Verifies that the simulation processor behaves as the processor it wraps, apart from the
 * precompile registry it is given.
 */
@ExtendWith(MockitoExtension.class)
class SimulationMessageCallProcessorTest {

  private static final Address SENDER = Address.fromHexString("0xc100");
  private static final Address RECIPIENT = Address.fromHexString("0xc200");
  private static final Address RIPEMD160 = Address.fromHexString("0x03");

  @Mock EVM evm;
  @Mock TransferLogEmitter transferLogEmitter;
  @Mock PrecompiledContract precompile;
  @Mock OperationTracer operationTracer;

  @Test
  void emitsTransferLogsAsTheWrappedProcessorDoes() {
    final MessageCallProcessor original =
        new MessageCallProcessor(
            evm, new PrecompileContractRegistry(), Set.of(), transferLogEmitter);
    final SimulationMessageCallProcessor simulation =
        new SimulationMessageCallProcessor(original, Function.identity());

    final ToyWorld world = new ToyWorld();
    world.createAccount(SENDER).setBalance(Wei.of(10));
    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .worldUpdater(world)
            .sender(SENDER)
            .address(RECIPIENT)
            .value(Wei.ONE)
            .build();

    simulation.start(frame, operationTracer);

    verify(transferLogEmitter).emitTransferLog(frame, SENDER, RECIPIENT, Wei.ONE);
  }

  @Test
  void forceDeletesTouchedEmptyAccountsAsTheWrappedProcessorDoes() {
    // A failed call to RIPEMD-160 still deletes the touched empty account, as on mainnet
    final PrecompileContractRegistry registry = new PrecompileContractRegistry();
    registry.put(RIPEMD160, precompile);
    when(precompile.gasRequirement(any())).thenReturn(100L);
    final MessageCallProcessor original =
        new MessageCallProcessor(evm, registry, Set.of(RIPEMD160), TransferLogEmitter.NOOP);
    final SimulationMessageCallProcessor simulation =
        new SimulationMessageCallProcessor(original, Function.identity());

    final ToyWorld world = new ToyWorld();
    world.createAccount(SENDER);
    world.createAccount(RIPEMD160);
    final ToyWorld callUpdater = (ToyWorld) world.updater();
    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .worldUpdater(callUpdater)
            .sender(SENDER)
            .address(RIPEMD160)
            .contract(RIPEMD160)
            .initialGas(10L)
            .build();

    simulation.process(frame, operationTracer);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.COMPLETED_FAILED);
    assertThat(world.get(RIPEMD160)).isNull();
  }
}
