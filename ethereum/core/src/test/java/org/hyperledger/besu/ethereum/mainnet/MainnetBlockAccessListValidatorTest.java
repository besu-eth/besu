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
package org.hyperledger.besu.ethereum.mainnet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Tests for {@link MainnetBlockAccessListValidator}. */
class MainnetBlockAccessListValidatorTest {

  private static final Address ADDR_1 =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address ADDR_2 =
      Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final StorageSlotKey SLOT_1 = new StorageSlotKey(UInt256.ONE);
  private static final StorageSlotKey SLOT_2 = new StorageSlotKey(UInt256.valueOf(2));
  private static final StorageSlotKey SLOT_3 = new StorageSlotKey(UInt256.valueOf(3));

  private static BlockAccessListValidator validator() {
    return validatorWithItemCost(2000L);
  }

  private static BlockAccessListValidator validatorWithItemCost(final long itemCost) {
    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    final org.hyperledger.besu.evm.gascalculator.GasCalculator gasCalculator =
        mock(org.hyperledger.besu.evm.gascalculator.GasCalculator.class);
    when(gasCalculator.getBlockAccessListItemCost()).thenReturn(itemCost);
    when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    return new MainnetBlockAccessListValidator(protocolSchedule);
  }

  private static BlockHeader headerWithBal(final BlockAccessList bal, final long gasLimit) {
    return new BlockHeaderTestFixture()
        .gasLimit(gasLimit)
        .balHash(BodyValidation.balHash(bal))
        .buildHeader();
  }

  @Nested
  class ValidCases {

    @Test
    void emptyBAL() {
      final BlockAccessList bal = new BlockAccessList(List.of());
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isTrue();
    }

    @Test
    void singleAccountNoChanges() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1, List.of(), List.of(), List.of(), List.of(), List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isTrue();
    }

    @Test
    void twoAccountsWithStorageAndChanges() {
      final BlockAccessList.AccountChanges account1 =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(0, UInt256.valueOf(100))))),
              List.of(new BlockAccessList.SlotRead(SLOT_2)),
              List.of(new BlockAccessList.BalanceChange(0, Wei.ONE)),
              List.of(new BlockAccessList.NonceChange(0, 1L)),
              List.of(new BlockAccessList.CodeChange(0, Bytes.EMPTY)));
      final BlockAccessList.AccountChanges account2 =
          new BlockAccessList.AccountChanges(
              ADDR_2, List.of(), List.of(), List.of(), List.of(), List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account1, account2));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isTrue();
    }

    @Test
    void atExactSizeLimit() {
      final BlockAccessList.AccountChanges account1 =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(0, UInt256.ZERO)))),
              List.of(),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList.AccountChanges account2 =
          new BlockAccessList.AccountChanges(
              ADDR_2, List.of(), List.of(), List.of(), List.of(), List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account1, account2));
      // 2 addresses + 1 storage key = 3 items, max = 3 with gas 6000 (Prague ITEM_COST=2000)
      Assertions.assertThat(validator().validate(Optional.of(bal), headerWithBal(bal, 6_000L), 0))
          .isTrue();
    }
  }

  @Nested
  class HeaderAndHash {

    @Test
    void failsWhenHeaderMissingBalHash() {
      final BlockAccessList bal = new BlockAccessList(List.of());
      final BlockHeader header = new BlockHeaderTestFixture().gasLimit(30_000_000L).buildHeader();
      Assertions.assertThat(header.getBalHash()).isEmpty();
      Assertions.assertThat(validator().validate(Optional.of(bal), header, 0)).isFalse();
    }

    @Test
    void failsWhenBalHashMismatch() {
      final BlockAccessList bal = new BlockAccessList(List.of());
      final Hash wrongHash = Hash.fromHexString("01".repeat(32));
      final BlockHeader header =
          new BlockHeaderTestFixture().gasLimit(30_000_000L).balHash(wrongHash).buildHeader();
      Assertions.assertThat(validator().validate(Optional.of(bal), header, 0)).isFalse();
    }
  }

  @Nested
  class SizeLimit {

    @Test
    void failsWhenExceedingMaxItems() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(0, UInt256.ZERO)))),
              List.of(
                  new BlockAccessList.SlotRead(SLOT_2),
                  new BlockAccessList.SlotRead(SLOT_3),
                  new BlockAccessList.SlotRead(new StorageSlotKey(UInt256.valueOf(4))),
                  new BlockAccessList.SlotRead(new StorageSlotKey(UInt256.valueOf(5)))),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      // 1 addr + 1 storage change + 4 reads = 6 items. ITEM_COST=2000, gas 10_000 → max 5 items
      final BlockHeader header = headerWithBal(bal, 10_000L);
      Assertions.assertThat(validator().validate(Optional.of(bal), header, 0)).isFalse();
    }

    @Test
    void sizeCheckSkippedWhenItemCostZero() {
      // BAL with 6 items would fail with itemCost=2000 and gas 10_000 (max 5 items)
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(0, UInt256.ZERO)))),
              List.of(
                  new BlockAccessList.SlotRead(SLOT_2),
                  new BlockAccessList.SlotRead(SLOT_3),
                  new BlockAccessList.SlotRead(new StorageSlotKey(UInt256.valueOf(4))),
                  new BlockAccessList.SlotRead(new StorageSlotKey(UInt256.valueOf(5)))),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      final BlockHeader header = headerWithBal(bal, 10_000L);
      // With itemCost=0 the size constraint is not applied (no division, check skipped)
      Assertions.assertThat(validatorWithItemCost(0L).validate(Optional.of(bal), header, 0))
          .isTrue();
    }
  }

  @Nested
  class UniquenessConstraints {

    @Test
    void failsWhenDuplicateAddress() {
      final BlockAccessList.AccountChanges account1 =
          new BlockAccessList.AccountChanges(
              ADDR_1, List.of(), List.of(), List.of(), List.of(), List.of());
      final BlockAccessList.AccountChanges account2SameAddress =
          new BlockAccessList.AccountChanges(
              ADDR_1, List.of(), List.of(), List.of(), List.of(), List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account1, account2SameAddress));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenDuplicateStorageKeyInStorageChanges() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(0, UInt256.ZERO))),
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(1, UInt256.ONE)))),
              List.of(),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenDuplicateStorageKeyInStorageReads() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(),
              List.of(new BlockAccessList.SlotRead(SLOT_1), new BlockAccessList.SlotRead(SLOT_1)),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenStorageKeyInBothStorageChangesAndStorageReads() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(0, UInt256.ZERO)))),
              List.of(new BlockAccessList.SlotRead(SLOT_1)),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenDuplicateBlockAccessIndexInStorageChangesPerSlot() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1,
                      List.of(
                          new BlockAccessList.StorageChange(0, UInt256.ZERO),
                          new BlockAccessList.StorageChange(0, UInt256.ONE)))),
              List.of(),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenDuplicateBlockAccessIndexInBalanceChanges() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(),
              List.of(),
              List.of(
                  new BlockAccessList.BalanceChange(0, Wei.ONE),
                  new BlockAccessList.BalanceChange(0, Wei.ZERO)),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenDuplicateBlockAccessIndexInNonceChanges() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(),
              List.of(),
              List.of(),
              List.of(
                  new BlockAccessList.NonceChange(0, 1L), new BlockAccessList.NonceChange(0, 2L)),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenDuplicateBlockAccessIndexInCodeChanges() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(),
              List.of(),
              List.of(),
              List.of(),
              List.of(
                  new BlockAccessList.CodeChange(0, Bytes.of(1)),
                  new BlockAccessList.CodeChange(0, Bytes.of(2))));
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void passesWhenSameTxIndexInDifferentSlots() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(0, UInt256.ZERO))),
                  new BlockAccessList.SlotChanges(
                      SLOT_2, List.of(new BlockAccessList.StorageChange(0, UInt256.ONE)))),
              List.of(),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isTrue();
    }
  }

  @Nested
  class IndexRangeValidation {

    @Test
    void failsWhenNbTransactionsNegative() {
      // nbTransactions must be >= 0; reject when negative and BAL is present
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1, List.of(), List.of(), List.of(), List.of(), List.of());
      final BlockAccessList balWithAccount = new BlockAccessList(List.of(account));
      final BlockHeader headerWithAccount = headerWithBal(balWithAccount, 30_000_000L);
      Assertions.assertThat(
              validator().validate(Optional.of(balWithAccount), headerWithAccount, -1))
          .isFalse();
    }

    @Test
    void failsWhenBlockAccessIndexExceedsTransactionCountPlusOne() {
      // 2 transactions -> max index = 3; txIndex 4 is invalid
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(),
              List.of(),
              List.of(new BlockAccessList.BalanceChange(4, Wei.ONE)),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      final BlockHeader header = headerWithBal(bal, 30_000_000L);
      Assertions.assertThat(validator().validate(Optional.of(bal), header, 2)).isFalse();
    }

    @Test
    void passesWhenBlockAccessIndexEqualsTransactionCountPlusOne() {
      // 1 transaction -> max index = 2; indices 0, 1, 2 are valid
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(2, UInt256.ZERO)))),
              List.of(),
              List.of(new BlockAccessList.BalanceChange(0, Wei.ONE)),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      final BlockHeader header = headerWithBal(bal, 30_000_000L);
      Assertions.assertThat(validator().validate(Optional.of(bal), header, 1)).isTrue();
    }

    @Test
    void passesWhenBlockAccessIndexWithinRange() {
      // nbTransactions=10 -> maxIndex=11, so txIndex 10 is valid
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(),
              List.of(),
              List.of(new BlockAccessList.BalanceChange(10, Wei.ONE)),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      final BlockHeader header = headerWithBal(bal, 30_000_000L);
      Assertions.assertThat(validator().validate(Optional.of(bal), header, 10)).isTrue();
    }
  }

  @Nested
  class OrderingConstraints {

    @Test
    void failsWhenAccountsNotSortedByAddress() {
      // ADDR_2 < ADDR_1 in hex, so canonical order is ADDR_1 then ADDR_2
      final BlockAccessList.AccountChanges a1 =
          new BlockAccessList.AccountChanges(
              ADDR_1, List.of(), List.of(), List.of(), List.of(), List.of());
      final BlockAccessList.AccountChanges a2 =
          new BlockAccessList.AccountChanges(
              ADDR_2, List.of(), List.of(), List.of(), List.of(), List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(a2, a1));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenStorageChangesNotSortedBySlot() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_2, List.of(new BlockAccessList.StorageChange(0, UInt256.ZERO))),
                  new BlockAccessList.SlotChanges(
                      SLOT_1, List.of(new BlockAccessList.StorageChange(0, UInt256.ONE)))),
              List.of(),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenStorageChangesNotSortedByTxIndexWithinSlot() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(
                  new BlockAccessList.SlotChanges(
                      SLOT_1,
                      List.of(
                          new BlockAccessList.StorageChange(1, UInt256.ZERO),
                          new BlockAccessList.StorageChange(0, UInt256.ONE)))),
              List.of(),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenStorageReadsNotSortedBySlot() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(),
              List.of(new BlockAccessList.SlotRead(SLOT_3), new BlockAccessList.SlotRead(SLOT_1)),
              List.of(),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }

    @Test
    void failsWhenBalanceChangesNotSortedByTxIndex() {
      final BlockAccessList.AccountChanges account =
          new BlockAccessList.AccountChanges(
              ADDR_1,
              List.of(),
              List.of(),
              List.of(
                  new BlockAccessList.BalanceChange(1, Wei.ONE),
                  new BlockAccessList.BalanceChange(0, Wei.ZERO)),
              List.of(),
              List.of());
      final BlockAccessList bal = new BlockAccessList(List.of(account));
      Assertions.assertThat(
              validator().validate(Optional.of(bal), headerWithBal(bal, 30_000_000L), 0))
          .isFalse();
    }
  }
}
