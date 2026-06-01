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
package org.hyperledger.besu.evm.account;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.fluent.SimpleAccount;

import org.junit.jupiter.api.Test;

class MutableAccountTest {

  private static final Address ADDRESS = Address.fromHexString("0xc0de");

  /**
   * Regression test for the goevmlab specfuzz finding: when a value credit pushes the balance past
   * 2^256-1, besu must wrap modulo 2^256 (matching geth/nethermind/erigon/reth) instead of throwing
   * {@code ArithmeticException} from Tuweni's overflow-checked {@code addExact}.
   */
  @Test
  void incrementBalanceWrapsOnOverflow() {
    final MutableAccount account = new SimpleAccount(ADDRESS, 0, Wei.MAX_WEI.subtract(Wei.ONE));

    final Wei previous = account.incrementBalance(Wei.of(2));

    assertThat(previous).isEqualTo(Wei.MAX_WEI.subtract(Wei.ONE));
    assertThat(account.getBalance()).isEqualTo(Wei.ZERO);
  }

  @Test
  void incrementBalanceWrapsOnExactBoundary() {
    final MutableAccount account = new SimpleAccount(ADDRESS, 0, Wei.MAX_WEI);

    account.incrementBalance(Wei.ONE);

    assertThat(account.getBalance()).isEqualTo(Wei.ZERO);
  }

  @Test
  void incrementBalanceWithoutOverflowBehavesNormally() {
    final MutableAccount account = new SimpleAccount(ADDRESS, 0, Wei.of(100));

    final Wei previous = account.incrementBalance(Wei.of(42));

    assertThat(previous).isEqualTo(Wei.of(100));
    assertThat(account.getBalance()).isEqualTo(Wei.of(142));
  }
}
