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
    // floor cost = 21000 (base cost) + 0
    when(transaction.getPayload()).thenReturn(Bytes.EMPTY);
    when(transaction.getAccessList()).thenReturn(Optional.empty());
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(21000L);

    // EIP-7976: floor cost = 21000 + 256 * 64 (uniform per-byte floor)
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x0, 256));
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(37384L);

    // EIP-7976: non-zero bytes priced identically to zero bytes for the floor
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x1, 256));
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(37384L);

    // 11-byte mixed payload: 21000 + 11 * 64 = 21704
    when(transaction.getPayload()).thenReturn(Bytes.fromHexString("0x0001000100010001000101"));
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(21704L);
  }

  @Test
  void accessListGasCostIncludesDataFloor() {
    // EIP-2930: 2400/address + 1900/key; EIP-7981: +1280/address + 2048/key
    // One address + zero keys  = 2400 + 1280 = 3680
    assertThat(amsterdamGasCalculator.accessListGasCost(1, 0)).isEqualTo(3680L);
    // One address + one key    = 3680 + 1900 + 2048 = 7628
    assertThat(amsterdamGasCalculator.accessListGasCost(1, 1)).isEqualTo(7628L);
    // Three addresses + five keys = 3*3680 + 5*(1900+2048) = 11040 + 19740 = 30780
    assertThat(amsterdamGasCalculator.accessListGasCost(3, 5)).isEqualTo(30780L);
  }

  @Test
  void transactionFloorCostWithoutAccessListMatchesCalldataOnlyFloor() {
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x1, 256));
    when(transaction.getAccessList()).thenReturn(Optional.empty());

    // 21000 + 256 * 64 = 37384
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(37384L);
  }

  @Test
  void transactionFloorCostIncludesAccessListBytes() {
    // 10 calldata bytes + 1 address (20 bytes) + 2 keys (2*32 = 64 bytes) = 94 bytes
    // 21000 + 94 * 64 = 21000 + 6016 = 27016
    final AccessListEntry entry =
        new AccessListEntry(
            Address.fromHexString("0x00000000000000000000000000000000000000aa"),
            List.of(Bytes32.ZERO, Bytes32.ZERO));
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x1, 10));
    when(transaction.getAccessList()).thenReturn(Optional.of(List.of(entry)));

    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(27016L);
  }

  @Test
  void transactionFloorCostAggregatesMultipleAccessListEntries() {
    // 4 calldata bytes
    // entry A: 20 address bytes + 0 keys                = 20 bytes
    // entry B: 20 address bytes + 1 key  (1*32 = 32)    = 52 bytes
    // entry C: 20 address bytes + 3 keys (3*32 = 96)    = 116 bytes
    // total bytes = 4 + 20 + 52 + 116 = 192
    // floor = 21000 + 192 * 64 = 21000 + 12288 = 33288
    final AccessListEntry entryA =
        new AccessListEntry(
            Address.fromHexString("0x00000000000000000000000000000000000000aa"), List.of());
    final AccessListEntry entryB =
        new AccessListEntry(
            Address.fromHexString("0x00000000000000000000000000000000000000bb"),
            List.of(Bytes32.ZERO));
    final AccessListEntry entryC =
        new AccessListEntry(
            Address.fromHexString("0x00000000000000000000000000000000000000cc"),
            List.of(Bytes32.ZERO, Bytes32.ZERO, Bytes32.ZERO));
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x1, 4));
    when(transaction.getAccessList()).thenReturn(Optional.of(List.of(entryA, entryB, entryC)));

    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(33288L);
  }
}
