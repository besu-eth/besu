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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.operation.OperationsV2;
import org.hyperledger.besu.evm.v2.operation.SendAllOperationV2;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** EIP-4758 SENDALL on EVM v1 and v2, against the Amsterdam SELFDESTRUCT it replaces. */
class SendAllOperationTest {

  private static final Address ORIGINATOR = Address.fromHexString("0xc0de");
  private static final Address BENEFICIARY = Address.fromHexString("0xbeef");
  private static final Wei BALANCE = Wei.of(1_000);
  private static final long GAS = 1_000_000L;

  private static final GasCalculator GAS_CALCULATOR = new AmsterdamGasCalculator();
  private static final long BASE = GAS_CALCULATOR.selfDestructOperationStaticGasCost();
  private static final long COLD = GAS_CALCULATOR.getColdAccountAccessCost();
  private static final long ACCOUNT_WRITE = GAS_CALCULATOR.getAccountWriteGasCost();
  private static final long NEW_ACCOUNT =
      GAS_CALCULATOR.stateGasCostCalculator().newAccountStateGas();

  private final WorldUpdater worldUpdater = new ToyWorld().updater();

  enum Fork {
    AMSTERDAM,
    BOGOTA;

    EVM evm() {
      return switch (this) {
        case AMSTERDAM ->
            MainnetEVMs.amsterdam(GAS_CALCULATOR, BigInteger.ONE, EvmConfiguration.DEFAULT);
        case BOGOTA -> MainnetEVMs.bogota(GAS_CALCULATOR, BigInteger.ONE, EvmConfiguration.DEFAULT);
      };
    }
  }

  enum Interpreter {
    V1,
    V2
  }

  @ParameterizedTest
  @EnumSource(Interpreter.class)
  void replacesSelfDestructFromBogota(final Interpreter interpreter) {
    final Operation amsterdam = operation(Fork.AMSTERDAM, interpreter);
    final Operation bogota = operation(Fork.BOGOTA, interpreter);

    assertThat(amsterdam.getName()).isEqualTo("SELFDESTRUCT");
    assertThat(bogota.getName()).isEqualTo("SENDALL");
    assertThat(bogota.getOpcode()).isEqualTo(0xFF);
    assertThat(bogota)
        .isInstanceOf(
            interpreter == Interpreter.V1 ? SendAllOperation.class : SendAllOperationV2.class);
  }

  @ParameterizedTest
  @EnumSource(Interpreter.class)
  void sweepsTheBalanceToTheBeneficiary(final Interpreter interpreter) {
    originator(BALANCE);
    worldUpdater.getOrCreate(BENEFICIARY).setBalance(Wei.ONE);
    final MessageFrame frame = frame(interpreter, BENEFICIARY, GAS, false);

    final Operation.OperationResult result = execute(Fork.BOGOTA, interpreter, frame);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.getState()).isEqualTo(MessageFrame.State.CODE_SUCCESS);
    assertThat(worldUpdater.get(BENEFICIARY).getBalance()).isEqualTo(Wei.of(1_001));
    assertThat(worldUpdater.get(ORIGINATOR).getBalance()).isEqualTo(Wei.ZERO);
    assertThat(frame.getLogs()).hasSize(1);
    assertThat(frame.getRefunds()).containsEntry(BENEFICIARY, BALANCE);
    assertThat(frame.getSelfDestructs()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Interpreter.class)
  void aSweepToItselfKeepsTheBalanceAndLogsNothing(final Interpreter interpreter) {
    originator(BALANCE);
    final MessageFrame frame = frame(interpreter, ORIGINATOR, GAS, false);

    final Operation.OperationResult result = execute(Fork.BOGOTA, interpreter, frame);

    assertThat(result.getHaltReason()).isNull();
    assertThat(worldUpdater.get(ORIGINATOR).getBalance()).isEqualTo(BALANCE);
    assertThat(frame.getLogs()).isEmpty();
    assertThat(frame.getSelfDestructs()).isEmpty();
  }

  static Stream<Arguments> createdInTheTransaction() {
    return Stream.of(
        Arguments.of(Fork.AMSTERDAM, Interpreter.V1, true),
        Arguments.of(Fork.AMSTERDAM, Interpreter.V2, true),
        Arguments.of(Fork.BOGOTA, Interpreter.V1, false),
        Arguments.of(Fork.BOGOTA, Interpreter.V2, false));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("createdInTheTransaction")
  void anAccountCreatedInTheTransactionIsDeletedOnlyBeforeBogota(
      final Fork fork, final Interpreter interpreter, final boolean deleted) {
    final MutableAccount originator = originator(BALANCE);
    originator.setCode(Bytes.fromHexString("0x6000ff"));
    originator.setStorageValue(UInt256.ONE, UInt256.ONE);
    final MessageFrame frame = frame(interpreter, BENEFICIARY, GAS, false);
    frame.addCreate(ORIGINATOR);

    final Operation.OperationResult result = execute(fork, interpreter, frame);

    assertThat(result.getHaltReason()).isNull();
    if (deleted) {
      assertThat(frame.getSelfDestructs()).containsExactly(ORIGINATOR);
    } else {
      assertThat(frame.getSelfDestructs()).isEmpty();
    }
    assertThat(worldUpdater.get(BENEFICIARY).getBalance()).isEqualTo(BALANCE);
    assertThat(worldUpdater.get(ORIGINATOR).getCode()).isEqualTo(Bytes.fromHexString("0x6000ff"));
    assertThat(worldUpdater.get(ORIGINATOR).getStorageValue(UInt256.ONE)).isEqualTo(UInt256.ONE);
  }

  @ParameterizedTest
  @EnumSource(Interpreter.class)
  void haltsInAStaticFrameBeforeTouchingAnything(final Interpreter interpreter) {
    originator(BALANCE);
    final MessageFrame frame = frame(interpreter, BENEFICIARY, GAS, true);

    final Operation.OperationResult result = execute(Fork.BOGOTA, interpreter, frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    assertThat(result.getGasCost()).isZero();
    assertThat(frame.isAddressWarm(BENEFICIARY)).isFalse();
    assertThat(worldUpdater.get(ORIGINATOR).getBalance()).isEqualTo(BALANCE);
    assertThat(worldUpdater.get(BENEFICIARY)).isNull();
  }

  static Stream<Arguments> gasCases() {
    return Stream.of(Interpreter.values())
        .flatMap(
            interpreter ->
                Stream.of(
                    // warm beneficiary, existing beneficiary, originator balance, cost, state gas
                    Arguments.of(interpreter, false, true, BALANCE, BASE + COLD, 0L),
                    Arguments.of(interpreter, true, true, BALANCE, BASE, 0L),
                    Arguments.of(
                        interpreter,
                        false,
                        false,
                        BALANCE,
                        BASE + COLD + ACCOUNT_WRITE,
                        NEW_ACCOUNT),
                    Arguments.of(
                        interpreter, true, false, BALANCE, BASE + ACCOUNT_WRITE, NEW_ACCOUNT),
                    Arguments.of(interpreter, false, false, Wei.ZERO, BASE + COLD, 0L)));
  }

  @ParameterizedTest(name = "{0} warm={1} exists={2} balance={3}")
  @MethodSource("gasCases")
  void chargesTheColdAccessAndTheCreationOfADeadBeneficiary(
      final Interpreter interpreter,
      final boolean warm,
      final boolean beneficiaryExists,
      final Wei originatorBalance,
      final long expectedCost,
      final long expectedStateGas) {
    originator(originatorBalance);
    if (beneficiaryExists) {
      worldUpdater.getOrCreate(BENEFICIARY).setBalance(Wei.ONE);
    }
    final MessageFrame frame = frame(interpreter, BENEFICIARY, GAS, false);
    if (warm) {
      frame.warmUpAddress(BENEFICIARY);
    }

    final Operation.OperationResult result = execute(Fork.BOGOTA, interpreter, frame);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(expectedCost);
    assertThat(frame.getStateGasUsed()).isEqualTo(expectedStateGas);
    assertThat(frame.isAddressWarm(BENEFICIARY)).isTrue();
  }

  static Stream<Arguments> outOfGasCases() {
    final long regular = BASE + COLD + ACCOUNT_WRITE;
    return Stream.of(Interpreter.values())
        .flatMap(
            interpreter ->
                Stream.of(
                    // gas, expected cost, beneficiary warmed
                    Arguments.of(interpreter, BASE + COLD - 1, BASE + COLD, false),
                    Arguments.of(interpreter, regular - 1, regular, true),
                    Arguments.of(interpreter, regular + NEW_ACCOUNT - 1, regular, true)));
  }

  @ParameterizedTest(name = "{0} gas={1}")
  @MethodSource("outOfGasCases")
  void checksTheStateIndependentCostFirstAndChargesStateGasLast(
      final Interpreter interpreter,
      final long gas,
      final long expectedCost,
      final boolean beneficiaryWarmed) {
    originator(BALANCE);
    final MessageFrame frame = frame(interpreter, BENEFICIARY, gas, false);

    final Operation.OperationResult result = execute(Fork.BOGOTA, interpreter, frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(result.getGasCost()).isEqualTo(expectedCost);
    assertThat(frame.isAddressWarm(BENEFICIARY)).isEqualTo(beneficiaryWarmed);
    assertThat(frame.getStateGasUsed()).isZero();
    assertThat(worldUpdater.get(ORIGINATOR).getBalance()).isEqualTo(BALANCE);
    assertThat(worldUpdater.get(BENEFICIARY)).isNull();
  }

  private MutableAccount originator(final Wei balance) {
    final MutableAccount originator = worldUpdater.getOrCreate(ORIGINATOR);
    originator.setBalance(balance);
    return originator;
  }

  private MessageFrame frame(
      final Interpreter interpreter,
      final Address beneficiary,
      final long gas,
      final boolean isStatic) {
    final Bytes32 beneficiaryWord = Bytes32.leftPad(beneficiary.getBytes());
    return switch (interpreter) {
      case V1 ->
          new TestMessageFrameBuilder()
              .address(ORIGINATOR)
              .contract(ORIGINATOR)
              .worldUpdater(worldUpdater)
              .initialGas(gas)
              .isStatic(isStatic)
              .pushStackItem(beneficiaryWord)
              .build();
      case V2 ->
          new TestMessageFrameBuilderV2()
              .address(ORIGINATOR)
              .contract(ORIGINATOR)
              .worldUpdater(worldUpdater)
              .initialGas(gas)
              .isStatic(isStatic)
              .pushStackItem(beneficiaryWord)
              .build();
    };
  }

  private static Operation operation(final Fork fork, final Interpreter interpreter) {
    final EVM evm = fork.evm();
    final Operation[] operations = evm.getOperationsUnsafe();
    return switch (interpreter) {
      case V1 -> operations[0xFF];
      case V2 -> OperationsV2.of(operations, evm.getGasCalculator(), evm.getEvmVersion())[0xFF];
    };
  }

  private static Operation.OperationResult execute(
      final Fork fork, final Interpreter interpreter, final MessageFrame frame) {
    return operation(fork, interpreter).execute(frame, null);
  }
}
