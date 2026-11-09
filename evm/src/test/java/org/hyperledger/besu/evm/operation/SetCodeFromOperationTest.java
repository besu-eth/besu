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
import static org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2.getV2StackItem;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.operation.OperationsV2;
import org.hyperledger.besu.evm.v2.operation.SetCodeFromOperationV2;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** EIP-8298 SETCODEFROM on EVM v1 and v2. */
class SetCodeFromOperationTest {

  private static final Address ADOPTER = Address.fromHexString("0xc0de");
  private static final Address SOURCE = Address.fromHexString("0x5005ce");
  private static final Bytes ADOPTER_CODE = Bytes.fromHexString("0x6001");
  private static final Bytes SOURCE_CODE = Bytes.fromHexString("0x6002");
  private static final long GAS = 1_000_000L;

  private static final GasCalculator GAS_CALCULATOR = new AmsterdamGasCalculator();
  private static final long WARM = GAS_CALCULATOR.getWarmStorageReadCost();
  private static final long COLD = GAS_CALCULATOR.getColdAccountAccessCost();
  private static final long ACCOUNT_WRITE = GAS_CALCULATOR.getAccountWriteGasCost();

  private final WorldUpdater worldUpdater = new ToyWorld().updater();

  enum Interpreter {
    V1,
    V2
  }

  @ParameterizedTest
  @EnumSource(Interpreter.class)
  void isDefinedFromBogota(final Interpreter interpreter) {
    final EVM amsterdam =
        MainnetEVMs.amsterdam(GAS_CALCULATOR, BigInteger.ONE, EvmConfiguration.DEFAULT);
    assertThat(amsterdam.getOperationsUnsafe()[SetCodeFromOperation.OPCODE])
        .isInstanceOf(InvalidOperation.class);

    final Operation bogota = operation(interpreter);
    assertThat(bogota.getName()).isEqualTo("SETCODEFROM");
    assertThat(bogota.getOpcode()).isEqualTo(0xA7);
    assertThat(bogota)
        .isInstanceOf(
            interpreter == Interpreter.V1
                ? SetCodeFromOperation.class
                : SetCodeFromOperationV2.class);
  }

  @ParameterizedTest
  @EnumSource(Interpreter.class)
  void adoptsTheCodeOfTheSource(final Interpreter interpreter) {
    account(ADOPTER, ADOPTER_CODE);
    account(SOURCE, SOURCE_CODE);
    final MessageFrame frame = frame(interpreter, GAS, false);

    final Operation.OperationResult result = operation(interpreter).execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(COLD + WARM + WARM + ACCOUNT_WRITE);
    assertThat(pushed(interpreter, frame)).isEqualTo(1);
    assertThat(worldUpdater.get(ADOPTER).getCode()).isEqualTo(SOURCE_CODE);
    assertThat(worldUpdater.get(ADOPTER).getCodeHash())
        .isEqualTo(worldUpdater.get(SOURCE).getCodeHash());
    assertThat(frame.getAdoptedCode()).containsExactly(ADOPTER);
    assertThat(frame.isAddressWarm(SOURCE)).isTrue();
  }

  @ParameterizedTest
  @EnumSource(Interpreter.class)
  void sameCodeHashSucceedsWithoutAWrite(final Interpreter interpreter) {
    account(ADOPTER, SOURCE_CODE);
    account(SOURCE, SOURCE_CODE);
    final MessageFrame frame = frame(interpreter, GAS, false);
    frame.warmUpAddress(SOURCE);

    final Operation.OperationResult result = operation(interpreter).execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(WARM + WARM + WARM);
    assertThat(pushed(interpreter, frame)).isEqualTo(1);
    assertThat(frame.getAdoptedCode()).isEmpty();
  }

  static Stream<Arguments> invalidSources() {
    return Stream.of(Interpreter.values())
        .flatMap(
            interpreter ->
                Stream.<Arguments>of(
                    Arguments.of(interpreter, "missing", (Consumer<WorldUpdater>) w -> {}, false),
                    Arguments.of(
                        interpreter,
                        "no code",
                        (Consumer<WorldUpdater>) w -> w.getOrCreate(SOURCE).setNonce(1),
                        false),
                    Arguments.of(
                        interpreter,
                        "0xEF prefix",
                        (Consumer<WorldUpdater>)
                            w ->
                                w.getOrCreate(SOURCE)
                                    .setCode(Bytes.fromHexString("0xef0100" + "00".repeat(20))),
                        false),
                    Arguments.of(
                        interpreter,
                        "created in the transaction",
                        (Consumer<WorldUpdater>) w -> w.getOrCreate(SOURCE).setCode(SOURCE_CODE),
                        true)));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("invalidSources")
  void anInvalidSourcePushesZeroAndChargesOnlyTheAccess(
      final Interpreter interpreter,
      final String name,
      final Consumer<WorldUpdater> source,
      final boolean createdInTransaction) {
    account(ADOPTER, ADOPTER_CODE);
    source.accept(worldUpdater);
    final MessageFrame frame = frame(interpreter, GAS, false);
    if (createdInTransaction) {
      frame.addCreate(SOURCE);
    }

    final Operation.OperationResult result = operation(interpreter).execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(COLD + WARM);
    assertThat(pushed(interpreter, frame)).isZero();
    assertThat(worldUpdater.get(ADOPTER).getCode()).isEqualTo(ADOPTER_CODE);
    assertThat(frame.getAdoptedCode()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Interpreter.class)
  void haltsInAStaticFrameBeforePopping(final Interpreter interpreter) {
    account(ADOPTER, ADOPTER_CODE);
    account(SOURCE, SOURCE_CODE);
    final MessageFrame frame = frame(interpreter, GAS, true);

    final Operation.OperationResult result = operation(interpreter).execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    assertThat(result.getGasCost()).isZero();
    assertThat(frame.isAddressWarm(SOURCE)).isFalse();
    assertThat(worldUpdater.get(ADOPTER).getCode()).isEqualTo(ADOPTER_CODE);
  }

  static Stream<Arguments> systemContracts() {
    return Stream.of(Interpreter.values())
        .flatMap(
            interpreter ->
                Stream.of(
                        "0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02",
                        "0x0000F90827F1C53a10cb7A02335B175320002935",
                        "0x00000961Ef480Eb55e80D19ad83579A64c007002",
                        "0x0000BBdDc7CE488642fb579F8B00f3a590007251",
                        "0x0000BFF46984E3725691FA540A8C7589300D8282",
                        "0x000064D678505AD48F8CCB093BC65613800E8282")
                    .map(address -> Arguments.of(interpreter, Address.fromHexString(address))));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("systemContracts")
  void neverReplacesTheCodeOfASystemContract(
      final Interpreter interpreter, final Address systemContract) {
    account(systemContract, ADOPTER_CODE);
    account(SOURCE, SOURCE_CODE);
    final MessageFrame frame = frame(interpreter, systemContract, GAS, false);

    final Operation.OperationResult result = operation(interpreter).execute(frame, null);

    assertThat(result.getHaltReason())
        .isEqualTo(ExceptionalHaltReason.CODE_CHANGE_IN_SYSTEM_CONTRACT);
    assertThat(result.getHaltReason().getDescription()).isNotEmpty();
    assertThat(result.getGasCost()).isZero();
    assertThat(frame.isAddressWarm(SOURCE)).isFalse();
    assertThat(worldUpdater.get(systemContract).getCode()).isEqualTo(ADOPTER_CODE);
  }

  static Stream<Arguments> outOfGasCases() {
    return Stream.of(Interpreter.values())
        .flatMap(
            interpreter ->
                Stream.of(
                    // gas, expected cost, source warmed
                    Arguments.of(interpreter, COLD + WARM - 1, COLD + WARM, false),
                    Arguments.of(interpreter, COLD + 2 * WARM - 1, COLD + 2 * WARM, true),
                    Arguments.of(
                        interpreter,
                        COLD + 2 * WARM + ACCOUNT_WRITE - 1,
                        COLD + 2 * WARM + ACCOUNT_WRITE,
                        true)));
  }

  @ParameterizedTest(name = "{0} gas={1}")
  @MethodSource("outOfGasCases")
  void checksEachCostBeforeTheStepItPays(
      final Interpreter interpreter,
      final long gas,
      final long expectedCost,
      final boolean sourceWarmed) {
    account(ADOPTER, ADOPTER_CODE);
    account(SOURCE, SOURCE_CODE);
    final MessageFrame frame = frame(interpreter, gas, false);

    final Operation.OperationResult result = operation(interpreter).execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(result.getGasCost()).isEqualTo(expectedCost);
    assertThat(frame.isAddressWarm(SOURCE)).isEqualTo(sourceWarmed);
    assertThat(worldUpdater.get(ADOPTER).getCode()).isEqualTo(ADOPTER_CODE);
    assertThat(frame.getAdoptedCode()).isEmpty();
  }

  private void account(final Address address, final Bytes code) {
    worldUpdater.getOrCreate(address).setCode(code);
  }

  private MessageFrame frame(
      final Interpreter interpreter, final long gas, final boolean isStatic) {
    return frame(interpreter, ADOPTER, gas, isStatic);
  }

  private MessageFrame frame(
      final Interpreter interpreter,
      final Address adopter,
      final long gas,
      final boolean isStatic) {
    final Bytes32 sourceWord = Bytes32.leftPad(SOURCE.getBytes());
    return switch (interpreter) {
      case V1 ->
          new TestMessageFrameBuilder()
              .address(adopter)
              .contract(adopter)
              .worldUpdater(worldUpdater)
              .initialGas(gas)
              .isStatic(isStatic)
              .pushStackItem(sourceWord)
              .build();
      case V2 ->
          new TestMessageFrameBuilderV2()
              .address(adopter)
              .contract(adopter)
              .worldUpdater(worldUpdater)
              .initialGas(gas)
              .isStatic(isStatic)
              .pushStackItem(sourceWord)
              .build();
    };
  }

  private static int pushed(final Interpreter interpreter, final MessageFrame frame) {
    return switch (interpreter) {
      case V1 -> frame.getStackItem(0).toInt();
      case V2 -> getV2StackItem(frame, 0).intValue();
    };
  }

  private static Operation operation(final Interpreter interpreter) {
    final EVM evm = MainnetEVMs.bogota(GAS_CALCULATOR, BigInteger.ONE, EvmConfiguration.DEFAULT);
    final Operation[] operations = evm.getOperationsUnsafe();
    return switch (interpreter) {
      case V1 -> operations[SetCodeFromOperation.OPCODE];
      case V2 ->
          OperationsV2.of(operations, evm.getGasCalculator(), evm.getEvmVersion())[
              SetCodeFromOperation.OPCODE];
    };
  }
}
