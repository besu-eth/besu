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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.precompile.PrecompiledContract;
import org.hyperledger.besu.evm.testutils.TestEvmConfigurations;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class MessageCallProcessorV2Test {

  private static final Address SENDER = Address.fromHexString("0x5e4d");
  private static final Address RECIPIENT = Address.fromHexString("0xc0de");
  private static final Address PRECOMPILE = Address.fromHexString("0x01");

  private final EVM evm = MainnetEVMs.prague(TestEvmConfigurations.EVM_V2);
  private final PrecompileContractRegistry precompiles = new PrecompileContractRegistry();
  private final MessageCallProcessor processor =
      MessageCallProcessorV2.forEvm(evm, precompiles, Set.of(), TransferLogEmitter.NOOP);
  private final ToyWorld world = new ToyWorld();

  @Test
  void evmV2GetsTheV2Processor() {
    assertThat(processor).isInstanceOf(MessageCallProcessorV2.class);
  }

  @Test
  void evmV1GetsTheV1Processor() {
    final EVM evmV1 = MainnetEVMs.prague(TestEvmConfigurations.EVM_V1);

    assertThat(MessageCallProcessorV2.forEvm(evmV1, precompiles, Set.of(), TransferLogEmitter.NOOP))
        .isExactlyInstanceOf(MessageCallProcessor.class);
  }

  @Test
  void callWithoutValueCreatesTheMissingRecipient() {
    world.createAccount(SENDER, 1, Wei.of(10));
    final MessageFrame frame = frame(world, Wei.ZERO, RECIPIENT);

    processor.start(frame, OperationTracer.NO_TRACING);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.CODE_EXECUTING);
    assertThat(frame.getWorldUpdater().get(RECIPIENT)).isNotNull();
    assertThat(frame.getWorldUpdater().get(SENDER).getBalance()).isEqualTo(Wei.of(10));
  }

  @Test
  void callWithValueTransfersIt() {
    world.createAccount(SENDER, 1, Wei.of(10));
    final MessageFrame frame = frame(world, Wei.of(3), RECIPIENT);

    processor.start(frame, OperationTracer.NO_TRACING);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.CODE_EXECUTING);
    assertThat(frame.getWorldUpdater().get(SENDER).getBalance()).isEqualTo(Wei.of(7));
    assertThat(frame.getWorldUpdater().get(RECIPIENT).getBalance()).isEqualTo(Wei.of(3));
  }

  @Test
  void callWithoutValueToAPrecompileRunsIt() {
    final PrecompiledContract precompile = mock(PrecompiledContract.class);
    when(precompile.gasRequirement(any())).thenReturn(0L);
    when(precompile.computePrecompile(any(), any()))
        .thenReturn(PrecompiledContract.PrecompileContractResult.success(Bytes.of(1)));
    precompiles.put(PRECOMPILE, precompile);
    world.createAccount(SENDER, 1, Wei.of(10));
    final MessageFrame frame = frame(world, Wei.ZERO, PRECOMPILE);

    processor.start(frame, OperationTracer.NO_TRACING);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
    assertThat(frame.getOutputData()).isEqualTo(Bytes.of(1));
  }

  @Test
  void callWithoutValueHaltsWhenTheRecipientCannotBeCreated() {
    final WorldUpdater readOnly = mock(WorldUpdater.class);
    final WorldUpdater child = mock(WorldUpdater.class);
    when(readOnly.updater()).thenReturn(child);
    when(child.getOrCreate(RECIPIENT)).thenThrow(new ModificationNotAllowedException());
    final MessageFrame frame = frame(readOnly, Wei.ZERO, RECIPIENT);

    processor.start(frame, OperationTracer.NO_TRACING);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.EXCEPTIONAL_HALT);
    assertThat(frame.getExceptionalHaltReason())
        .contains(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
  }

  /** A v2 call frame below a transaction's frame, as a CALL starts it. */
  private MessageFrame frame(final WorldUpdater world, final Wei value, final Address recipient) {
    final MessageFrame parent =
        MessageFrame.builder()
            .enableEvmV2(true)
            .worldUpdater(world)
            .originator(SENDER)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(new ToyBlockValues())
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(100_000)
            .address(SENDER)
            .contract(SENDER)
            .inputData(Bytes.EMPTY)
            .sender(SENDER)
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(Code.EMPTY_CODE)
            .completer(__ -> {})
            .build();
    return MessageFrame.childCallFrameV2(
        parent,
        50_000,
        recipient,
        recipient,
        Bytes.EMPTY,
        SENDER,
        value,
        value,
        Code.EMPTY_CODE,
        false,
        __ -> {});
  }
}
