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
package org.hyperledger.besu.evm.gascalculator;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class Eip8037StateGasCostCalculatorTest {

  private static final long AMSTERDAM_CPSB = 1174L;

  private final Eip8037StateGasCostCalculator calculator = new Eip8037StateGasCostCalculator();

  @Test
  void costPerStateByteReturnsDynamicValue() {
    assertThat(calculator.costPerStateByte()).isEqualTo(AMSTERDAM_CPSB);
  }

  @Test
  void createStateGas() {
    // 112 bytes per account * cpsb(1174) = 131_488
    assertThat(calculator.createStateGas()).isEqualTo(112L * AMSTERDAM_CPSB);
  }

  @Test
  void storageSetStateGas() {
    // 32 bytes per storage slot * cpsb(1174) = 37_568
    assertThat(calculator.storageSetStateGas()).isEqualTo(32L * AMSTERDAM_CPSB);
  }

  @Test
  void codeDepositStateGas() {
    // cpsb * codeSize
    assertThat(calculator.codeDepositStateGas(100)).isEqualTo(AMSTERDAM_CPSB * 100L);
    assertThat(calculator.codeDepositStateGas(0)).isEqualTo(0L);
  }

  @Test
  void codeDepositHashGas() {
    // 6 * ceil(codeSize / 32)
    assertThat(calculator.codeDepositHashGas(0)).isEqualTo(0L);
    assertThat(calculator.codeDepositHashGas(1)).isEqualTo(6L);
    assertThat(calculator.codeDepositHashGas(32)).isEqualTo(6L);
    assertThat(calculator.codeDepositHashGas(33)).isEqualTo(12L);
    assertThat(calculator.codeDepositHashGas(100)).isEqualTo(24L);
  }

  @Test
  void newAccountStateGasMatchesCreate() {
    assertThat(calculator.newAccountStateGas()).isEqualTo(calculator.createStateGas());
  }

  @Test
  void authBaseStateGas() {
    // 23 bytes per auth * cpsb(1174) = 27_002
    assertThat(calculator.authBaseStateGas()).isEqualTo(23L * AMSTERDAM_CPSB);
  }

  @Test
  void emptyAccountDelegationStateGasMatchesCreate() {
    assertThat(calculator.emptyAccountDelegationStateGas()).isEqualTo(calculator.createStateGas());
  }

  @Test
  void constantRegularGasCosts() {
    assertThat(calculator.storageSetRegularGas()).isEqualTo(2_900L);
    assertThat(calculator.authBaseRegularGas()).isEqualTo(7_500L);
    assertThat(calculator.transactionRegularGasLimit()).isEqualTo(16_777_216L);
  }

  @Test
  void noneImplementationReturnsZeroForAllCosts() {
    final StateGasCostCalculator none = StateGasCostCalculator.NONE;
    assertThat(none.costPerStateByte()).isEqualTo(0L);
    assertThat(none.createStateGas()).isEqualTo(0L);
    assertThat(none.storageSetStateGas()).isEqualTo(0L);
    assertThat(none.codeDepositStateGas(100)).isEqualTo(0L);
    assertThat(none.codeDepositHashGas(100)).isEqualTo(0L);
    assertThat(none.newAccountStateGas()).isEqualTo(0L);
    assertThat(none.authBaseStateGas()).isEqualTo(0L);
    assertThat(none.emptyAccountDelegationStateGas()).isEqualTo(0L);
    assertThat(none.storageSetRegularGas()).isEqualTo(0L);
    assertThat(none.authBaseRegularGas()).isEqualTo(0L);
    assertThat(none.transactionRegularGasLimit()).isEqualTo(Long.MAX_VALUE);
  }

  // --- refundSameTransactionSelfDestructStateGas ---

  // Uses only the updater's journaled writes rather than trie enumeration. ToyAccount mirrors
  // Bonsai here: storageEntriesFrom throws UnsupportedOperationException, so a regression that
  // reintroduces trie iteration would fail these tests.

  @Test
  void refundSameTxSelfDestructRefundsCreationCodeAndNonZeroStorageSlots() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final ToyWorld world = new ToyWorld();
    final MutableAccount account = world.createAccount(addr, 1, Wei.ZERO);
    final Bytes code = Bytes.fromHexString("0x60016002600360045050");
    account.setCode(code);
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(42L));
    account.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(7L));
    // 0 → X → 0: set and reset; final value is zero and should not be refunded here.
    account.setStorageValue(UInt256.valueOf(3), UInt256.ZERO);

    final MessageFrame frame = buildFrame(world);
    frame.addCreate(addr);
    frame.addSelfDestruct(addr);

    calculator.refundSameTransactionSelfDestructStateGas(frame, 0L);

    final long expected =
        calculator.createStateGas()
            + calculator.codeDepositStateGas(code.size())
            + 2L * calculator.storageSetStateGas();
    assertThat(frame.getStateGasReservoir()).isEqualTo(expected);
    assertThat(frame.getStateGasUsed()).isEqualTo(-expected);
  }

  @Test
  void refundSameTxSelfDestructSkipsAccountsNotCreatedInThisTx() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final ToyWorld world = new ToyWorld();
    world.createAccount(addr, 1, Wei.ZERO).setStorageValue(UInt256.ONE, UInt256.valueOf(42L));

    final MessageFrame frame = buildFrame(world);
    frame.addSelfDestruct(addr); // destroyed but not created in this tx — EIP-6780 no-op

    calculator.refundSameTransactionSelfDestructStateGas(frame, 0L);

    assertThat(frame.getStateGasReservoir()).isZero();
    assertThat(frame.getStateGasUsed()).isZero();
  }

  @Test
  void refundSameTxSelfDestructNoOpWhenSetEmpty() {
    final ToyWorld world = new ToyWorld();
    final MessageFrame frame = buildFrame(world);

    calculator.refundSameTransactionSelfDestructStateGas(frame, 0L);

    assertThat(frame.getStateGasReservoir()).isZero();
    assertThat(frame.getStateGasUsed()).isZero();
  }

  private static MessageFrame buildFrame(final ToyWorld world) {
    return new TestMessageFrameBuilder()
        .worldUpdater(world)
        .blockValues(new FakeBlockValues(1, Optional.empty()))
        .build();
  }
}
