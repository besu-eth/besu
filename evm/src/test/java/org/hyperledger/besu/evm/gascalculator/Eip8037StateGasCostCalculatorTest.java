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

import org.junit.jupiter.api.Test;

class Eip8037StateGasCostCalculatorTest {

  private static final long AMSTERDAM_CPSB = 1530L;

  private final Eip8037StateGasCostCalculator calculator = new Eip8037StateGasCostCalculator();

  @Test
  void costPerStateByteReturnsDynamicValue() {
    assertThat(calculator.costPerStateByte()).isEqualTo(AMSTERDAM_CPSB);
  }

  @Test
  void newContractStateGas() {
    assertThat(calculator.newContractStateGas()).isEqualTo(120L * AMSTERDAM_CPSB);
  }

  @Test
  void storageSetStateGas() {
    assertThat(calculator.storageSetStateGas()).isEqualTo(64L * AMSTERDAM_CPSB);
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
    assertThat(calculator.newAccountStateGas()).isEqualTo(calculator.newContractStateGas());
  }

  @Test
  void authBaseStateGas() {
    assertThat(calculator.authBaseStateGas()).isEqualTo(23L * AMSTERDAM_CPSB);
  }

  @Test
  void emptyAccountDelegationStateGasMatchesCreate() {
    assertThat(calculator.emptyAccountDelegationStateGas())
        .isEqualTo(calculator.newContractStateGas());
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
    assertThat(none.newContractStateGas()).isEqualTo(0L);
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
}
