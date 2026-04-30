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
package org.hyperledger.besu.evm.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.precompile.PrecompiledContract;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Nested
@ExtendWith(MockitoExtension.class)
class MessageCallProcessorTest extends AbstractMessageProcessorTest<MessageCallProcessor> {

  @Mock EVM evm;
  @Mock PrecompileContractRegistry precompileContractRegistry;
  @Mock PrecompiledContract contract;

  @Override
  protected MessageCallProcessor getAbstractMessageProcessor() {
    return new MessageCallProcessor(evm, precompileContractRegistry);
  }

  @Test
  public void shouldTracePrecompileContractCall() {
    Address sender = Address.ZERO;
    Address recipient = Address.fromHexString("0x1");

    ToyWorld toyWorld = new ToyWorld();
    toyWorld.createAccount(sender);
    toyWorld.createAccount(recipient);

    final MessageFrame messageFrame =
        new TestMessageFrameBuilder()
            .worldUpdater(toyWorld)
            .sender(sender)
            .address(recipient)
            .initialGas(20L)
            .build();

    when(precompileContractRegistry.get(any())).thenReturn(contract);
    when(contract.gasRequirement(any())).thenReturn(10L);
    when(contract.computePrecompile(any(), eq(messageFrame)))
        .thenReturn(PrecompiledContract.PrecompileContractResult.success(any()));

    getAbstractMessageProcessor().process(messageFrame, operationTracer);

    verify(operationTracer, times(1)).tracePrecompileCall(eq(messageFrame), anyLong(), any());
  }

  @Test
  public void shouldTracePrecompileContractCallOutOfGas() {
    Address sender = Address.ZERO;
    Address recipient = Address.fromHexString("0x1");

    ToyWorld toyWorld = new ToyWorld();
    toyWorld.createAccount(sender);
    toyWorld.createAccount(recipient);

    final MessageFrame messageFrame =
        new TestMessageFrameBuilder()
            .worldUpdater(toyWorld)
            .sender(sender)
            .address(recipient)
            .initialGas(5L)
            .build();

    when(precompileContractRegistry.get(any())).thenReturn(contract);
    when(contract.gasRequirement(any())).thenReturn(10L);

    getAbstractMessageProcessor().process(messageFrame, operationTracer);

    verify(operationTracer, times(1)).tracePrecompileCall(eq(messageFrame), anyLong(), any());
  }
}
