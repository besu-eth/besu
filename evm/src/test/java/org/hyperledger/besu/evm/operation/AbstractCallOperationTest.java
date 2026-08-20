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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.precompile.PrecompiledContract;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class AbstractCallOperationTest {

  private static final Address STATE_CONTEXT =
      Address.fromHexString("0x00000000000000000000000000ca110b15012381");
  private static final Address CODE_ADDRESS = Address.BLS12_G1ADD;
  private static final long INITIAL_GAS = 1_000_000L;

  private final GasCalculator gasCalculator = new AmsterdamGasCalculator();
  private final EVM evm =
      MainnetEVMs.amsterdam(gasCalculator, BigInteger.ONE, EvmConfiguration.DEFAULT);
  private final long newAccountStateGas =
      gasCalculator.stateGasCostCalculator().newAccountStateGas();
  private final long initialStateGasReservoir = 2 * newAccountStateGas;

  @Test
  void failedCallCodeDoesNotRefundStateGasThatWasNotCharged() {
    assertThat(gasCalculator.stateGasCostCalculator().isActive()).isTrue();

    final SimpleWorld world = worldWithAliveStateContext();
    final MessageFrame parentFrame = createParentFrame(world);
    final CallCodeOperation operation = new CallCodeOperation(gasCalculator);

    pushCallArguments(parentFrame);
    operation.execute(parentFrame, evm);

    assertThat(world.get(STATE_CONTEXT)).isNotNull();
    assertThat(world.get(STATE_CONTEXT).isEmpty()).isFalse();
    assertThat(world.get(CODE_ADDRESS)).isNull();
    assertThat(parentFrame.getStateGasReservoir()).isEqualTo(initialStateGasReservoir);
    assertThat(parentFrame.getStateGasUsed()).isZero();

    final MessageFrame childFrame = parentFrame.getMessageFrameStack().peek();
    assertThat(childFrame).isNotSameAs(parentFrame);
    assertThat(childFrame.getRecipientAddress()).isEqualTo(STATE_CONTEXT);
    assertThat(childFrame.getContractAddress()).isEqualTo(CODE_ADDRESS);
    assertThat(childFrame.getValue()).isEqualTo(Wei.ONE);
    assertThat(childFrame.getApparentValue()).isEqualTo(Wei.ONE);

    failingPrecompileProcessor().process(childFrame, OperationTracer.NO_TRACING);

    assertThat(childFrame.getState()).isEqualTo(MessageFrame.State.COMPLETED_FAILED);
    assertThat(parentFrame.getStateGasReservoir()).isEqualTo(initialStateGasReservoir);
    assertThat(parentFrame.getStateGasUsed()).isZero();
  }

  @Test
  void failedCallRefundsChargedNewAccountStateGasOnce() {
    assertThat(gasCalculator.stateGasCostCalculator().isActive()).isTrue();

    final SimpleWorld world = worldWithAliveStateContext();
    final MessageFrame parentFrame = createParentFrame(world);
    final CallOperation operation = new CallOperation(gasCalculator);

    pushCallArguments(parentFrame);
    operation.execute(parentFrame, evm);

    assertThat(world.get(CODE_ADDRESS)).isNull();
    assertThat(parentFrame.getStateGasReservoir())
        .isEqualTo(initialStateGasReservoir - newAccountStateGas);
    assertThat(parentFrame.getStateGasUsed()).isEqualTo(newAccountStateGas);

    final MessageFrame childFrame = parentFrame.getMessageFrameStack().peek();
    assertThat(childFrame.getRecipientAddress()).isEqualTo(CODE_ADDRESS);
    assertThat(childFrame.getContractAddress()).isEqualTo(CODE_ADDRESS);

    failingPrecompileProcessor().process(childFrame, OperationTracer.NO_TRACING);

    assertThat(childFrame.getState()).isEqualTo(MessageFrame.State.COMPLETED_FAILED);
    assertThat(parentFrame.getStateGasReservoir()).isEqualTo(initialStateGasReservoir);
    assertThat(parentFrame.getStateGasUsed()).isZero();
  }

  private SimpleWorld worldWithAliveStateContext() {
    final SimpleWorld world = new SimpleWorld();
    world.createAccount(STATE_CONTEXT, 1L, Wei.ONE);
    return world;
  }

  private MessageFrame createParentFrame(final SimpleWorld world) {
    return MessageFrame.builder()
        .type(MessageFrame.Type.MESSAGE_CALL)
        .contract(STATE_CONTEXT)
        .inputData(Bytes.EMPTY)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(Code.EMPTY_CODE)
        .completer(__ -> {})
        .address(STATE_CONTEXT)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .blockValues(new FakeBlockValues(1L))
        .gasPrice(Wei.ZERO)
        .miningBeneficiary(Address.ZERO)
        .originator(Address.ZERO)
        .initialGas(INITIAL_GAS)
        .initialStateGasReservoir(initialStateGasReservoir)
        .worldUpdater(world)
        .build();
  }

  private void pushCallArguments(final MessageFrame frame) {
    frame.pushStackItem(Bytes.EMPTY); // output size
    frame.pushStackItem(Bytes.EMPTY); // output offset
    frame.pushStackItem(Bytes.EMPTY); // input size
    frame.pushStackItem(Bytes.EMPTY); // input offset
    frame.pushStackItem(Wei.ONE);
    frame.pushStackItem(CODE_ADDRESS.getBytes());
    frame.pushStackItem(Bytes.ofUnsignedLong(100_000L));
  }

  private MessageCallProcessor failingPrecompileProcessor() {
    final PrecompileContractRegistry precompiles = new PrecompileContractRegistry();
    precompiles.put(
        CODE_ADDRESS,
        new PrecompiledContract() {
          @Override
          public String getName() {
            return "FAILING";
          }

          @Override
          public long gasRequirement(final Bytes input) {
            return 0L;
          }

          @Override
          public PrecompileContractResult computePrecompile(
              final Bytes input, final MessageFrame messageFrame) {
            return PrecompileContractResult.halt(
                null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
          }
        });
    return new MessageCallProcessor(evm, precompiles);
  }
}
