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
package org.hyperledger.besu.evm.gascalculator;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class AmsterdamGasCalculatorTest {

  private final AmsterdamGasCalculator amsterdamGasCalculator = new AmsterdamGasCalculator();

  @Test
  void transactionFloorCostShouldBeAtLeastTransactionBaseCost() {
    // floor cost = 21000 (base cost) + 0
    assertThat(amsterdamGasCalculator.transactionFloorCost(Bytes.EMPTY, 0)).isEqualTo(21000L);
    // EIP-7976: floor cost = 21000 + 256 * 64 (uniform per-byte floor)
    assertThat(amsterdamGasCalculator.transactionFloorCost(Bytes.repeat((byte) 0x0, 256), 256))
        .isEqualTo(37384L);
    // EIP-7976: non-zero bytes priced identically to zero bytes for the floor
    assertThat(amsterdamGasCalculator.transactionFloorCost(Bytes.repeat((byte) 0x1, 256), 0))
        .isEqualTo(37384L);
    // 11-byte mixed payload: 21000 + 11 * 64 = 21704
    assertThat(
            amsterdamGasCalculator.transactionFloorCost(
                Bytes.fromHexString("0x0001000100010001000101"), 5))
        .isEqualTo(21704L);
  }
}
