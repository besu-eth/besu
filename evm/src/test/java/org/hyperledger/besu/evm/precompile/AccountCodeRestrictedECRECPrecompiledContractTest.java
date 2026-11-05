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
package org.hyperledger.besu.evm.precompile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.Eip7928AccessList;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.precompile.PrecompiledContract.PrecompileContractResult;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** EIP-8151 checks over a real {@link MessageFrame} and {@link SimpleWorld}. */
class AccountCodeRestrictedECRECPrecompiledContractTest {

  private static final Bytes INPUT =
      Bytes.fromHexString(
          "0x0049872459827432342344987245982743234234498724598274323423429943000000000000000000000000000000000000000000000000000000000000001be8359c341771db7f9ea3a662a1741d27775ce277961470028e054ed3285aab8e31f63eaac35c4e6178abbc2a1073040ac9bbb0b67f2bc89a2e9593ba9abe8c53");
  private static final Address SIGNER =
      Address.fromHexString("0x0c65a9d9ffc02c7c99e36e32ce0f950c7804ceda");
  private static final Bytes SIGNER_WORD = Bytes32.leftPad(SIGNER.getBytes());
  private static final Bytes DELEGATION =
      Bytes.concatenate(
          CodeDelegationHelper.CODE_DELEGATION_PREFIX,
          Address.fromHexString("0x00000000000000000000000000000000000000aa").getBytes());
  private static final long INITIAL_GAS = 10_000L;

  private final GasCalculator gasCalculator = new AmsterdamGasCalculator();
  private final AccountCodeRestrictedECRECPrecompiledContract contract =
      new AccountCodeRestrictedECRECPrecompiledContract(gasCalculator);
  private final Eip7928AccessList accessList = mock(Eip7928AccessList.class);
  private final long coldCost =
      gasCalculator.getColdAccountAccessCost() + gasCalculator.getWarmStorageReadCost();
  private final long warmCost = 2 * gasCalculator.getWarmStorageReadCost();

  @AfterEach
  void disableCaching() {
    AbstractPrecompiledContract.setPrecompileCaching(false);
  }

  static Stream<Arguments> signerCode() {
    return Stream.of(
        Arguments.of("no account", null, true),
        Arguments.of("balance only", Bytes.EMPTY, true),
        Arguments.of("delegation designation", DELEGATION, true),
        Arguments.of("contract code", Bytes.fromHexString("0x60006000f3"), false),
        Arguments.of("truncated delegation", DELEGATION.slice(0, 22), false),
        Arguments.of("bare delegation prefix", CodeDelegationHelper.CODE_DELEGATION_PREFIX, false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("signerCode")
  void chargesWarmsAndTouchesBeforeRestrictingOutput(
      final String name, final Bytes code, final boolean returnsSigner) {
    final SimpleWorld world = new SimpleWorld();
    if (code != null) {
      world.createAccount(SIGNER, 1, Wei.ONE).setCode(code);
    }
    final MessageFrame frame = frame(world, INITIAL_GAS);

    final PrecompileContractResult result = contract.computePrecompile(INPUT, frame);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.output()).isEqualTo(returnsSigner ? SIGNER_WORD : Bytes.EMPTY);
    assertThat(frame.getRemainingGas()).isEqualTo(INITIAL_GAS - coldCost);
    assertThat(frame.isAddressWarm(SIGNER)).isTrue();
    verify(accessList).addTouchedAccount(SIGNER);
  }

  static Stream<Arguments> accessGas() {
    return Stream.of(
        Arguments.of("cold, exact gas", false, 0L),
        Arguments.of("cold, one gas short", false, 1L),
        Arguments.of("warm, exact gas", true, 0L),
        Arguments.of("warm, one gas short", true, 1L));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("accessGas")
  void chargesAccessGasOrHaltsWithoutTouching(
      final String name, final boolean warm, final long shortfall) {
    final long cost = warm ? warmCost : coldCost;
    final long initialGas = cost - shortfall;
    final MessageFrame frame = frame(new SimpleWorld(), initialGas);
    if (warm) {
      frame.warmUpAddress(SIGNER);
    }

    final PrecompileContractResult result = contract.computePrecompile(INPUT, frame);

    if (shortfall == 0) {
      assertThat(result.isSuccessful()).isTrue();
      assertThat(result.output()).isEqualTo(SIGNER_WORD);
      assertThat(frame.getRemainingGas()).isZero();
      assertThat(frame.isAddressWarm(SIGNER)).isTrue();
      verify(accessList).addTouchedAccount(SIGNER);
    } else {
      assertThat(result.state()).isEqualTo(MessageFrame.State.EXCEPTIONAL_HALT);
      assertThat(result.haltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);
      assertThat(frame.getRemainingGas()).isEqualTo(initialGas);
      assertThat(frame.isAddressWarm(SIGNER)).isEqualTo(warm);
      verify(accessList, never()).addTouchedAccount(any());
    }
  }

  @Test
  void failedRecoveryChargesAndTouchesNothing() {
    final Bytes badSignature = Bytes.concatenate(INPUT.slice(0, 64), Bytes.wrap(new byte[64]));
    final MessageFrame frame = frame(new SimpleWorld(), INITIAL_GAS);

    final PrecompileContractResult result = contract.computePrecompile(badSignature, frame);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.output()).isEqualTo(Bytes.EMPTY);
    assertThat(frame.getRemainingGas()).isEqualTo(INITIAL_GAS);
    verify(accessList, never()).addTouchedAccount(any());
  }

  @Test
  void resultCacheHoldsTheUnrestrictedRecovery() {
    AbstractPrecompiledContract.setPrecompileCaching(true);
    final SimpleWorld withCode = new SimpleWorld();
    withCode.createAccount(SIGNER, 1, Wei.ONE).setCode(Bytes.fromHexString("0x00"));

    assertThat(contract.computePrecompile(INPUT, frame(withCode, INITIAL_GAS)).output())
        .isEqualTo(Bytes.EMPTY);
    assertThat(contract.computePrecompile(INPUT, frame(new SimpleWorld(), INITIAL_GAS)).output())
        .isEqualTo(SIGNER_WORD);
    assertThat(contract.computePrecompile(INPUT, frame(withCode, INITIAL_GAS)).output())
        .isEqualTo(Bytes.EMPTY);
  }

  private MessageFrame frame(final SimpleWorld world, final long gas) {
    final BlockValues blockValues = new FakeBlockValues(1337);
    return MessageFrame.builder()
        .type(MessageFrame.Type.MESSAGE_CALL)
        .worldUpdater(world)
        .initialGas(gas)
        .address(Address.ECREC)
        .originator(Address.ZERO)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .inputData(INPUT)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .contract(Address.ECREC)
        .code(Code.EMPTY_CODE)
        .blockValues(blockValues)
        .completer(__ -> {})
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, number) -> Hash.hash(Words.longBytes(number)))
        .eip7928AccessList(accessList)
        .build();
  }
}
