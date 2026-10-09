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
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PayOperationTest {
  private static final Address CURRENT_ADDRESS = Address.fromHexString("0xc0ff");
  private static final Address RECIPIENT_ADDRESS = Address.fromHexString("0xc0de");
  private static final EVM EVM_INSTANCE = mock(EVM.class);

  private SimpleWorld worldUpdater;

  @BeforeEach
  void beforeEach() {
    worldUpdater = new SimpleWorld();
    worldUpdater.createAccount(CURRENT_ADDRESS, 0, Wei.ZERO);
    worldUpdater.createAccount(RECIPIENT_ADDRESS, 0, Wei.ZERO);
  }

  static Iterable<Arguments> data() {
    return List.of(
        Arguments.of(
            "not enough gas",
            CURRENT_ADDRESS,
            RECIPIENT_ADDRESS,
            99,
            100,
            ExceptionalHaltReason.INSUFFICIENT_GAS,
            RECIPIENT_ADDRESS.getBytes(),
            true),
        Arguments.of(
            "enough gas",
            CURRENT_ADDRESS,
            RECIPIENT_ADDRESS,
            5000,
            100,
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            true),
        Arguments.of(
            "current address == recipient",
            RECIPIENT_ADDRESS,
            RECIPIENT_ADDRESS,
            5000,
            100,
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            // the current address is always warm
            true),
        Arguments.of(
            "cold address",
            CURRENT_ADDRESS,
            RECIPIENT_ADDRESS,
            7300,
            2600,
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            false),
        Arguments.of(
            "cold address - no account and zero value",
            CURRENT_ADDRESS,
            Address.fromHexString("0x341a2e456a2c23ca9a8c7d765521bcee2188d66a"),
            30000,
            2600,
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            false));
  }

  @ParameterizedTest(name = "{index}: {0} {1}")
  @MethodSource("data")
  void noValueTest(
      final String name,
      final Address currentAddress,
      final Address recipientAddress,
      final long initialGas,
      final long chargedGas,
      final ExceptionalHaltReason haltReason,
      final Bytes stackItem,
      final boolean warmAddress) {
    final PayOperation operation = new PayOperation(new OsakaGasCalculator());

    final var frame =
        new TestMessageFrameBuilder()
            .address(currentAddress)
            .initialGas(initialGas)
            .pushStackItem(Bytes.EMPTY)
            .pushStackItem(Bytes.EMPTY)
            .pushStackItem(Wei.ZERO)
            .pushStackItem(recipientAddress.getBytes())
            .worldUpdater(worldUpdater)
            .build();
    if (warmAddress) {
      frame.warmUpAddress(recipientAddress);
    }

    var result = operation.execute(frame, EVM_INSTANCE);

    assertThat(result.getGasCost()).isEqualTo(chargedGas);
    assertThat(result.getHaltReason()).isEqualTo(haltReason);

    assertThat(frame.getStackItem(0)).isEqualTo(stackItem);

    Account recipientAccount = worldUpdater.get(recipientAddress);
    if (recipientAccount != null) {
      assertThat(recipientAccount.getBalance()).isEqualTo(Wei.ZERO);
    }
  }

  static Iterable<Arguments> valueData() {
    return List.of(
        Arguments.of(
            "not enough gas",
            CURRENT_ADDRESS,
            RECIPIENT_ADDRESS,
            5000,
            9100,
            ExceptionalHaltReason.INSUFFICIENT_GAS,
            RECIPIENT_ADDRESS.getBytes(),
            Wei.of(1000),
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(2000),
            true),
        Arguments.of(
            "enough value",
            CURRENT_ADDRESS,
            RECIPIENT_ADDRESS,
            10000,
            9100,
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            Wei.of(1000),
            Wei.of(2000),
            Wei.of(1000),
            Wei.of(1000),
            Wei.of(2000),
            true),
        Arguments.of(
            "not enough value",
            CURRENT_ADDRESS,
            RECIPIENT_ADDRESS,
            40000,
            9100,
            null,
            AbstractCallOperation.LEGACY_FAILURE_STACK_ITEM,
            Wei.of(1000),
            Wei.of(500),
            Wei.of(500),
            Wei.of(0),
            Wei.of(0),
            true),
        Arguments.of(
            "current address == recipient",
            RECIPIENT_ADDRESS,
            RECIPIENT_ADDRESS,
            10000,
            9100,
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            Wei.of(1000),
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(2000),
            true),
        Arguments.of(
            "cold address",
            CURRENT_ADDRESS,
            RECIPIENT_ADDRESS,
            12000,
            11600,
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            Wei.of(1000),
            Wei.of(2000),
            Wei.of(1000),
            Wei.of(1000),
            Wei.of(2000),
            false),
        Arguments.of(
            "cold address new account",
            CURRENT_ADDRESS,
            Address.fromHexString("0x341a2e456a2c23ca9a8c7d765521bcee2188d66a"),
            40000,
            36600,
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            Wei.of(1000),
            Wei.of(2000),
            Wei.of(1000),
            Wei.of(0),
            Wei.of(1000),
            false),
        Arguments.of(
            "precompile address",
            CURRENT_ADDRESS,
            Address.fromHexString("0x01"),
            10000,
            9100,
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            Wei.of(1000),
            Wei.of(2000),
            Wei.of(1000),
            Wei.of(0),
            Wei.of(1000),
            false));
  }

  @ParameterizedTest(name = "{index}: {0} {1}")
  @MethodSource("valueData")
  void valueTest(
      final String name,
      final Address currentAddress,
      final Address recipientAddress,
      final long initialGas,
      final long chargedGas,
      final ExceptionalHaltReason haltReason,
      final Bytes stackItem,
      final Wei valueSent,
      final Wei initialCurrentBalance,
      final Wei currentBalance,
      final Wei initialRecipientBalance,
      final Wei recipientBalance,
      final boolean warmAddress) {
    final PayOperation operation = new PayOperation(new OsakaGasCalculator());

    final var frame =
        new TestMessageFrameBuilder()
            .address(currentAddress)
            .initialGas(initialGas)
            .pushStackItem(Bytes.EMPTY)
            .pushStackItem(Bytes.EMPTY)
            .pushStackItem(valueSent)
            .pushStackItem(recipientAddress.getBytes())
            .worldUpdater(worldUpdater)
            .build();
    if (warmAddress) {
      frame.warmUpAddress(recipientAddress);
    }

    MutableAccount currentAccount = worldUpdater.getAccount(currentAddress);
    currentAccount.setBalance(initialCurrentBalance);
    MutableAccount recipientAccount = worldUpdater.getAccount(recipientAddress);
    if (recipientAccount != null) {
      recipientAccount.setBalance(initialRecipientBalance);
    }

    var result = operation.execute(frame, EVM_INSTANCE);

    assertThat(result.getGasCost()).isEqualTo(chargedGas);
    assertThat(result.getHaltReason()).isEqualTo(haltReason);

    assertThat(frame.getStackItem(0)).isEqualTo(stackItem);

    assertThat(currentAccount.getBalance()).isEqualTo(currentBalance);
    assertThat(worldUpdater.getAccount(recipientAddress).getBalance()).isEqualTo(recipientBalance);
  }

  static List<Arguments> staticContext() {
    return List.of(
        Arguments.of(
            "no value in static context",
            CURRENT_ADDRESS,
            101,
            0,
            ExceptionalHaltReason.ILLEGAL_STATE_CHANGE,
            RECIPIENT_ADDRESS.getBytes(),
            Wei.of(0),
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(1000),
            Wei.of(1000)),
        Arguments.of(
            "current address == recipient static context",
            RECIPIENT_ADDRESS,
            10000,
            0,
            ExceptionalHaltReason.ILLEGAL_STATE_CHANGE,
            RECIPIENT_ADDRESS.getBytes(),
            Wei.of(1000),
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(2000)),
        Arguments.of(
            "value in static context",
            CURRENT_ADDRESS,
            9200,
            0,
            ExceptionalHaltReason.ILLEGAL_STATE_CHANGE,
            RECIPIENT_ADDRESS.getBytes(),
            Wei.of(1000),
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(1000),
            Wei.of(1000)),
        Arguments.of(
            "no value in static context",
            CURRENT_ADDRESS,
            9200,
            0,
            ExceptionalHaltReason.ILLEGAL_STATE_CHANGE,
            RECIPIENT_ADDRESS.getBytes(),
            Wei.ZERO,
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(1000),
            Wei.of(1000)),
        Arguments.of(
            "no gas in static context",
            CURRENT_ADDRESS,
            9000,
            0,
            ExceptionalHaltReason.ILLEGAL_STATE_CHANGE,
            RECIPIENT_ADDRESS.getBytes(),
            Wei.of(1),
            Wei.of(2000),
            Wei.of(2000),
            Wei.of(1000),
            Wei.of(1000)));
  }

  @ParameterizedTest
  @MethodSource("staticContext")
  void staticCallContext(
      final String name,
      final Address currentAddress,
      final long initialGas,
      final long chargedGas,
      final ExceptionalHaltReason haltReason,
      final Bytes stackItem,
      final Wei valueSent,
      final Wei initialCurrentBalance,
      final Wei currentBalance,
      final Wei initialRecipientBalance,
      final Wei recipientBalance) {

    final PayOperation operation = new PayOperation(new OsakaGasCalculator());

    final var frame =
        new TestMessageFrameBuilder()
            .address(currentAddress)
            .initialGas(initialGas)
            .pushStackItem(Bytes.EMPTY)
            .pushStackItem(Bytes.EMPTY)
            .pushStackItem(valueSent)
            .pushStackItem(RECIPIENT_ADDRESS.getBytes())
            .worldUpdater(worldUpdater)
            .isStatic(true)
            .build();

    frame.warmUpAddress(RECIPIENT_ADDRESS);
    MutableAccount currentAccount = worldUpdater.getAccount(CURRENT_ADDRESS);
    currentAccount.setBalance(initialCurrentBalance);
    MutableAccount recipientAccount = worldUpdater.getAccount(RECIPIENT_ADDRESS);
    recipientAccount.setBalance(initialRecipientBalance);

    var result = operation.execute(frame, EVM_INSTANCE);

    assertThat(result.getGasCost()).isEqualTo(chargedGas);
    assertThat(result.getHaltReason()).isEqualTo(haltReason);

    assertThat(frame.getStackItem(0)).isEqualTo(stackItem);

    assertThat(currentAccount.getBalance()).isEqualTo(currentBalance);
    assertThat(recipientAccount.getBalance()).isEqualTo(recipientBalance);
  }

  static List<Arguments> bigAddress() {
    return List.of(
        Arguments.of(
            "2 byte address",
            Bytes.fromHexString("0xd66a"),
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            36600L),
        Arguments.of(
            "exact 20 byte address",
            Bytes.fromHexString("0x341a2e456a2c23ca9a8c7d765521bcee2188d66a"),
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            36600L),
        Arguments.of(
            "21 byte address with leading zero",
            Bytes.fromHexString("0x00341a2e456a2c23ca9a8c7d765521bcee2188d66a"),
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            36600L),
        Arguments.of(
            "exact 21 bytes address",
            Bytes.fromHexString("0x341a2e456a2c23ca9a8c7d765521bcee2188d66a76"),
            ExceptionalHaltReason.ADDRESS_OUT_OF_RANGE,
            Bytes.fromHexString("0x341a2e456a2c23ca9a8c7d765521bcee2188d66a76"),
            0),
        Arguments.of(
            "20 bytes address padded to 32 bytes",
            Bytes.fromHexString("0x341a2e456a2c23ca9a8c7d765521bcee2188d66a", 32),
            null,
            AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM,
            36600L),
        Arguments.of(
            "22 byte address size padded to 32 bytes",
            Bytes.fromHexString("0x341a2e456a2c23ca9a8c7d765521bcee2188d66a76ad", 32),
            ExceptionalHaltReason.ADDRESS_OUT_OF_RANGE,
            Bytes.fromHexString("0x341a2e456a2c23ca9a8c7d765521bcee2188d66a76ad", 32),
            0));
  }

  @ParameterizedTest
  @MethodSource("bigAddress")
  void tooBigAddress(
      final String name,
      final Bytes recipient,
      final ExceptionalHaltReason haltReason,
      final Bytes stackItem,
      final long gasCost) {
    final PayOperation operation = new PayOperation(new OsakaGasCalculator());

    final var frame =
        new TestMessageFrameBuilder()
            .address(CURRENT_ADDRESS)
            .initialGas(Long.MAX_VALUE)
            .pushStackItem(Bytes.EMPTY)
            .pushStackItem(Bytes.EMPTY)
            .pushStackItem(Wei.of(1000))
            .pushStackItem(recipient)
            .worldUpdater(worldUpdater)
            .build();

    MutableAccount currentAccount = worldUpdater.getAccount(CURRENT_ADDRESS);
    currentAccount.setBalance(Wei.of(2000));

    var result = operation.execute(frame, EVM_INSTANCE);

    assertThat(result.getGasCost()).isEqualTo(gasCost);
    assertThat(result.getHaltReason()).isEqualTo(haltReason);

    assertThat(frame.getStackItem(0)).isEqualTo(stackItem);
  }

  @Test
  void paysFromTheCurrentAddressNotFromTheCaller() {
    final Address caller = Address.fromHexString("0xca11");
    worldUpdater.createAccount(caller, 0, Wei.of(5000));
    worldUpdater.getAccount(CURRENT_ADDRESS).setBalance(Wei.of(2000));
    final var frame =
        new TestMessageFrameBuilder()
            .sender(caller)
            .address(CURRENT_ADDRESS)
            .initialGas(10000)
            .pushStackItem(Wei.of(1000))
            .pushStackItem(RECIPIENT_ADDRESS.getBytes())
            .worldUpdater(worldUpdater)
            .build();
    frame.warmUpAddress(RECIPIENT_ADDRESS);

    final var result = new PayOperation(new OsakaGasCalculator()).execute(frame, EVM_INSTANCE);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.getStackItem(0)).isEqualTo(AbstractCallOperation.LEGACY_SUCCESS_STACK_ITEM);
    assertThat(worldUpdater.get(CURRENT_ADDRESS).getBalance()).isEqualTo(Wei.of(1000));
    assertThat(worldUpdater.get(RECIPIENT_ADDRESS).getBalance()).isEqualTo(Wei.of(1000));
    assertThat(worldUpdater.get(caller).getBalance()).isEqualTo(Wei.of(5000));
  }

  @Test
  void payingItselfMoreThanItsBalanceFails() {
    worldUpdater.getAccount(CURRENT_ADDRESS).setBalance(Wei.of(2000));
    final var frame =
        new TestMessageFrameBuilder()
            .address(CURRENT_ADDRESS)
            .initialGas(10000)
            .pushStackItem(Wei.of(3000))
            .pushStackItem(CURRENT_ADDRESS.getBytes())
            .worldUpdater(worldUpdater)
            .build();
    frame.warmUpAddress(CURRENT_ADDRESS);

    final var result = new PayOperation(new OsakaGasCalculator()).execute(frame, EVM_INSTANCE);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.getStackItem(0)).isEqualTo(AbstractCallOperation.LEGACY_FAILURE_STACK_ITEM);
    assertThat(worldUpdater.get(CURRENT_ADDRESS).getBalance()).isEqualTo(Wei.of(2000));
  }
}
