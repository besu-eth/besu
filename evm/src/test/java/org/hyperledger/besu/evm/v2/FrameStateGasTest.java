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
package org.hyperledger.besu.evm.v2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.collections.undo.UndoSet;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.FrameInfo;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.TxValues;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * EIP-8141 frame state gas: SSTORE draws state gas only from the executing frame's isolated pool,
 * and a refill for a cleared slot goes back to the frame that paid the outstanding charge. Every
 * case runs on the v1 interpreter and on EVM v2 with the same expectations.
 */
class FrameStateGasTest {

  private static final Address SENDER = Address.fromHexString("0x5e4d");
  private static final Address CONTRACT = Address.fromHexString("0xc0de");
  private static final Bytes32 SLOT = Bytes32.ZERO;

  // PUSH1 1 PUSH1 0 SSTORE STOP
  private static final Bytes SET = Bytes.fromHexString("0x600160005500");
  // PUSH1 0 PUSH1 0 SSTORE STOP
  private static final Bytes CLEAR = Bytes.fromHexString("0x600060005500");
  // PUSH1 1 PUSH1 0 SSTORE PUSH1 0 PUSH1 0 SSTORE STOP
  private static final Bytes SET_THEN_CLEAR = Bytes.fromHexString("0x6001600055600060005500");
  // PUSH1 1 PUSH1 0 SSTORE PUSH1 0 PUSH1 0 REVERT
  private static final Bytes SET_THEN_REVERT = Bytes.fromHexString("0x600160005560006000fd");

  private static final long GAS = 100_000L;
  private static final long STORAGE_SET_STATE_GAS =
      new AmsterdamGasCalculator().stateGasCostCalculator().storageSetStateGas();

  static Stream<Named<EvmConfiguration>> interpreters() {
    return Stream.of(Named.of("v1", ProgramRun.V1), Named.of("v2", ProgramRun.V2));
  }

  @ParameterizedTest
  @MethodSource("interpreters")
  void storageSetDrawsOnlyFromTheFramePool(final EvmConfiguration configuration) {
    final FrameTransactionContext context = context();
    final TxValues txValues = txValues(context, STORAGE_SET_STATE_GAS + 7);

    final MessageFrame frame = run(configuration, world(), txValues, SET);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
    assertThat(txValues.stateGasReservoir().get()).isEqualTo(7L);
    assertThat(txValues.stateGasUsed().get()).isEqualTo(STORAGE_SET_STATE_GAS);
    // same execution gas as outside a frame transaction with a sufficient reservoir
    final MessageFrame plain =
        runOutsideFrameTransaction(configuration, STORAGE_SET_STATE_GAS + 7, SET);
    assertThat(frame.getRemainingGas()).isEqualTo(plain.getRemainingGas());
    // the executing frame owns the outstanding charge
    assertThat(context.removeSlotCharge(CONTRACT, SLOT)).isZero();
  }

  @ParameterizedTest
  @MethodSource("interpreters")
  void storageSetFailsWithoutSpillingIntoExecutionGas(final EvmConfiguration configuration) {
    final FrameTransactionContext context = context();
    final TxValues txValues = txValues(context, STORAGE_SET_STATE_GAS - 1);

    final MessageFrame frame = run(configuration, world(), txValues, SET);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.COMPLETED_FAILED);
    assertThat(frame.getExceptionalHaltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(txValues.stateGasReservoir().get()).isEqualTo(STORAGE_SET_STATE_GAS - 1);
    assertThat(txValues.stateGasUsed().get()).isZero();
    assertThat(context.removeSlotCharge(CONTRACT, SLOT)).isNull();

    // outside a frame transaction the same shortfall spills into execution gas
    final MessageFrame plain =
        runOutsideFrameTransaction(configuration, STORAGE_SET_STATE_GAS - 1, SET);
    assertThat(plain.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
    assertThat(plain.getStateGasReservoir()).isZero();
  }

  @ParameterizedTest
  @MethodSource("interpreters")
  void setThenClearInTheSameFrameRefillsThePool(final EvmConfiguration configuration) {
    final FrameTransactionContext context = context();
    final TxValues txValues = txValues(context, STORAGE_SET_STATE_GAS);

    final MessageFrame frame = run(configuration, world(), txValues, SET_THEN_CLEAR);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
    assertThat(txValues.stateGasReservoir().get()).isEqualTo(STORAGE_SET_STATE_GAS);
    assertThat(txValues.stateGasUsed().get()).isZero();
    assertThat(context.removeSlotCharge(CONTRACT, SLOT)).isNull();
  }

  @ParameterizedTest
  @MethodSource("interpreters")
  void crossFrameClearReducesTheOwningFramesReceiptNotTheCurrentPool(
      final EvmConfiguration configuration) {
    final FrameTransactionContext context = context();
    final TxValues txValues = txValues(context, STORAGE_SET_STATE_GAS);
    final WorldUpdater world = world();

    final MessageFrame first = run(configuration, world, txValues, SET);
    assertThat(first.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
    context.recordFrameCompletion(
        0,
        FrameTransactionContext.STATUS_SUCCESS,
        GAS - first.getRemainingGas(),
        STORAGE_SET_STATE_GAS - txValues.stateGasReservoir().get());
    assertThat(context.frameStateGasUsed(0)).isEqualTo(STORAGE_SET_STATE_GAS);

    // the second frame starts with an empty pool of its own
    context.setCurrentFrameIndex(1);
    txValues.stateGasReservoir().set(0L);
    final MessageFrame second = run(configuration, world, txValues, CLEAR);

    assertThat(second.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
    assertThat(txValues.stateGasReservoir().get()).isZero();
    assertThat(context.frameStateGasUsed(0)).isZero();
    assertThat(txValues.stateGasUsed().get()).isZero();
    assertThat(context.removeSlotCharge(CONTRACT, SLOT)).isNull();
  }

  @ParameterizedTest
  @MethodSource("interpreters")
  void revertedFrameRestoresThePoolAndForgetsTheCharge(final EvmConfiguration configuration) {
    final FrameTransactionContext context = context();
    final TxValues txValues = txValues(context, STORAGE_SET_STATE_GAS);

    final MessageFrame frame = run(configuration, world(), txValues, SET_THEN_REVERT);

    assertThat(frame.getState()).isEqualTo(MessageFrame.State.COMPLETED_FAILED);
    assertThat(frame.getExceptionalHaltReason()).isEmpty();
    assertThat(txValues.stateGasReservoir().get()).isEqualTo(STORAGE_SET_STATE_GAS);
    assertThat(txValues.stateGasUsed().get()).isZero();
    assertThat(context.removeSlotCharge(CONTRACT, SLOT)).isNull();
  }

  private static FrameTransactionContext context() {
    final FrameInfo frame =
        new FrameInfo(
            FrameTransactionContext.MODE_SENDER,
            0,
            CONTRACT,
            GAS,
            STORAGE_SET_STATE_GAS,
            Wei.ZERO,
            Bytes.EMPTY);
    return new FrameTransactionContext(
        List.of(frame, frame),
        List.of(),
        SENDER,
        Bytes32.ZERO,
        0L,
        List.of(UInt256.ZERO),
        0L,
        Wei.ONE,
        Wei.ONE,
        Wei.ZERO,
        Wei.ZERO);
  }

  private static TxValues txValues(
      final FrameTransactionContext context, final long stateGasReservoir) {
    final TxValues txValues =
        TxValues.forTransaction(
            (__, ___) -> Hash.ZERO,
            1024,
            UndoSet.of(new TreeSet<>(List.of(SENDER))),
            SENDER,
            Wei.ONE,
            Wei.ONE,
            new ToyBlockValues(),
            Address.ZERO,
            Optional.empty(),
            stateGasReservoir);
    txValues.setFrameTransactionContext(context);
    return txValues;
  }

  private static WorldUpdater world() {
    final ToyWorld world = new ToyWorld();
    final WorldUpdater setup = world.updater();
    setup.getOrCreate(SENDER).setBalance(Wei.of(1_000_000));
    setup.getOrCreate(CONTRACT).setBalance(Wei.of(1));
    setup.commit();
    // every frame of a transaction shares the transaction's updater, so the slot's original value
    // stays the pre-transaction one across frames
    return world.updater();
  }

  private static EVM evm(final EvmConfiguration configuration) {
    return MainnetEVMs.bogota(new AmsterdamGasCalculator(), BigInteger.ONE, configuration);
  }

  private static MessageFrame.Builder frameBuilder(
      final EVM evm, final WorldUpdater updater, final Bytes code) {
    return MessageFrame.builder()
        .enableEvmV2(evm.getEvmConfiguration().enableEvmV2())
        .type(MessageFrame.Type.MESSAGE_CALL)
        .worldUpdater(updater)
        .initialGas(GAS)
        .address(CONTRACT)
        .contract(CONTRACT)
        .originator(SENDER)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .inputData(Bytes.EMPTY)
        .sender(SENDER)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(new Code(code))
        .blockValues(new ToyBlockValues())
        .completer(__ -> {})
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO);
  }

  private static MessageFrame run(
      final EvmConfiguration configuration,
      final WorldUpdater updater,
      final TxValues txValues,
      final Bytes code) {
    final EVM evm = evm(configuration);
    final MessageFrame frame = frameBuilder(evm, updater, code).txValues(txValues).build();
    return process(evm, frame);
  }

  private static MessageFrame runOutsideFrameTransaction(
      final EvmConfiguration configuration, final long stateGasReservoir, final Bytes code) {
    final EVM evm = evm(configuration);
    final MessageFrame frame =
        frameBuilder(evm, world(), code).initialStateGasReservoir(stateGasReservoir).build();
    return process(evm, frame);
  }

  private static MessageFrame process(final EVM evm, final MessageFrame frame) {
    final MessageCallProcessor processor =
        new MessageCallProcessor(evm, new PrecompileContractRegistry());
    while (!frame.getMessageFrameStack().isEmpty()) {
      processor.process(frame.getMessageFrameStack().peekFirst(), OperationTracer.NO_TRACING);
    }
    return frame;
  }
}
