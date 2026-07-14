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
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AmsterdamGasCalculatorTest {

  private final AmsterdamGasCalculator amsterdamGasCalculator = new AmsterdamGasCalculator();

  @Mock private Transaction transaction;

  @Test
  void transactionFloorCostShouldBeAtLeastTransactionBaseCost() {
    // EIP-2780: floor base cost = TX_BASE = 12000
    assertThat(amsterdamGasCalculator.transactionFloorCost(Bytes.EMPTY, 0)).isEqualTo(12000L);
    // EIP-7976: floor cost = 12000 + 256 * 64 (uniform per-byte floor)
    assertThat(amsterdamGasCalculator.transactionFloorCost(Bytes.repeat((byte) 0x0, 256), 256))
        .isEqualTo(28384L);
    // EIP-7976: non-zero bytes priced identically to zero bytes for the floor
    assertThat(amsterdamGasCalculator.transactionFloorCost(Bytes.repeat((byte) 0x1, 256), 0))
        .isEqualTo(28384L);
    // 11-byte mixed payload: 12000 + 11 * 64 = 12704
    assertThat(
            amsterdamGasCalculator.transactionFloorCost(
                Bytes.fromHexString("0x0001000100010001000101"), 5))
        .isEqualTo(12704L);
  }

  @Test
  void accessListGasCostIncludesDataFloor() {
    // EIP-2780/EIP-8038: 3000/address + 3000/key; EIP-7981: +1280/address + 2048/key
    // One address + zero keys  = 3000 + 1280 = 4280
    assertThat(amsterdamGasCalculator.accessListGasCost(1, 0)).isEqualTo(4280L);
    // One address + one key    = 4280 + 3000 + 2048 = 9328
    assertThat(amsterdamGasCalculator.accessListGasCost(1, 1)).isEqualTo(9328L);
    // Three addresses + five keys = 3*4280 + 5*(3000+2048) = 12840 + 25240 = 38080
    assertThat(amsterdamGasCalculator.accessListGasCost(3, 5)).isEqualTo(38080L);
  }

  @Test
  void transactionFloorCostWithoutAccessListMatchesCalldataOnlyFloor() {
    // Zero-value simple call: EIP-3120 anchors the floor on TX_BASE + COLD_ACCOUNT_ACCESS.
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x1, 256));
    when(transaction.getAccessList()).thenReturn(Optional.empty());
    when(transaction.getValue()).thenReturn(Wei.ZERO);

    // (12000 + 3000) + 256 * 64 = 15000 + 16384 = 31384
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(31384L);
  }

  @Test
  void transactionFloorCostIncludesAccessListBytes() {
    // Zero-value simple call: anchor = TX_BASE + COLD_ACCOUNT_ACCESS = 15000 (EIP-3120).
    // 10 calldata bytes + 1 address (20 bytes) + 2 keys (2*32 = 64 bytes) = 94 bytes
    // 15000 + 94 * 64 = 15000 + 6016 = 21016
    final AccessListEntry entry =
        new AccessListEntry(
            Address.fromHexString("0x00000000000000000000000000000000000000aa"),
            List.of(Bytes32.ZERO, Bytes32.ZERO));
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x1, 10));
    when(transaction.getAccessList()).thenReturn(Optional.of(List.of(entry)));
    when(transaction.getValue()).thenReturn(Wei.ZERO);

    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(21016L);
  }
}
