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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
class AmsterdamGasCalculatorTest {

  private static final Address SENDER =
      Address.fromHexString("0x00000000000000000000000000000000000000a1");
  private static final Address RECIPIENT =
      Address.fromHexString("0x00000000000000000000000000000000000000b2");

  private final AmsterdamGasCalculator amsterdamGasCalculator = new AmsterdamGasCalculator();

  @Mock private Transaction transaction;

  @Test
  void transactionFloorCostShouldBeAtLeastTransactionBaseCost() {
    // EIP-2780: TX_BASE (12000) replaces the flat 21000 minimum.
    // floor cost = 12000 (base cost) + 0
    when(transaction.getPayload()).thenReturn(Bytes.EMPTY);
    when(transaction.getAccessList()).thenReturn(Optional.empty());
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(12000L);

    // EIP-7976: floor cost = 12000 + 256 * 64 (uniform per-byte floor) = 28384
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x0, 256));
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(28384L);

    // EIP-7976: non-zero bytes priced identically to zero bytes for the floor
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x1, 256));
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(28384L);

    // 11-byte mixed payload: 12000 + 11 * 64 = 12704
    when(transaction.getPayload()).thenReturn(Bytes.fromHexString("0x0001000100010001000101"));
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(12704L);
  }

  @Test
  void accessListGasCostIncludesDataFloor() {
    // EIP-8038: per-entry access cost raised to COLD_ACCESS (3,000) for both addresses and keys.
    // EIP-7981 data floor: +1280/address + 2048/key.
    // One address + zero keys  = 3000 + 1280 = 4280
    assertThat(amsterdamGasCalculator.accessListGasCost(1, 0)).isEqualTo(4280L);
    // One address + one key    = 4280 + 3000 + 2048 = 9328
    assertThat(amsterdamGasCalculator.accessListGasCost(1, 1)).isEqualTo(9328L);
    // Three addresses + five keys = 3*4280 + 5*(3000+2048) = 12840 + 25240 = 38080
    assertThat(amsterdamGasCalculator.accessListGasCost(3, 5)).isEqualTo(38080L);
  }

  @Test
  void eip8038CreateAccessGasCost() {
    // EIP-8038: CREATE/CREATE2 regular-gas cost = CREATE_ACCESS = ACCOUNT_WRITE (8,000)
    // + COLD_STORAGE_ACCESS (3,000) = 11,000.
    assertThat(amsterdamGasCalculator.txCreateCost()).isEqualTo(11_000L);
  }

  @Test
  void eip8038StateAccessGasRepricing() {
    // EIP-8038: cold access raised to 3,000 (account was 2,600, storage slot was 2,100).
    assertThat(amsterdamGasCalculator.getColdAccountAccessCost()).isEqualTo(3_000L);
    assertThat(amsterdamGasCalculator.getColdSloadCost()).isEqualTo(3_000L);
    // SSTORE cold surcharge excludes the warm base (100) folded into slotAccessCost:
    // COLD_STORAGE_ACCESS 3,000 - WARM_ACCESS 100 = 2,900.
    assertThat(amsterdamGasCalculator.getSStoreColdAccessGasCost()).isEqualTo(2_900L);
    // CALL value cost = ACCOUNT_WRITE (8,000) + CALL_STIPEND (2,300) = 10,300.
    assertThat(amsterdamGasCalculator.callValueTransferGasCost()).isEqualTo(10_300L);
    // EXTCODESIZE base = extra WARM_ACCESS "code reading cost" (100).
    assertThat(amsterdamGasCalculator.getExtCodeSizeOperationGasCost()).isEqualTo(100L);
  }

  @Test
  void eip8038SStoreFlatWriteCost() {
    final Supplier<UInt256> zero = () -> UInt256.ZERO;
    final Supplier<UInt256> nonZero = () -> UInt256.valueOf(42);
    // No change: warm access base only (100).
    assertThat(amsterdamGasCalculator.slotAccessCost(UInt256.valueOf(42), nonZero, nonZero))
        .isEqualTo(100L);
    // First change (0 -> nonzero): warm base (100) + flat STORAGE_WRITE (10,000) = 10,100.
    assertThat(amsterdamGasCalculator.slotAccessCost(UInt256.valueOf(42), zero, zero))
        .isEqualTo(10_100L);
  }

  /**
   * The "Transaction reference cases" table of EIP-2780, intrinsic (regular) column. Rows that
   * differ only in their runtime charges collapse to the same intrinsic cost — they are listed
   * separately anyway so the table can be read against the EIP row by row.
   */
  static Stream<Arguments> eip2780ReferenceCases() {
    return Stream.of(
        // description, to, value, expected intrinsic regular gas
        Arguments.of("self-transfer", SENDER, Wei.ONE, 12_000L),
        Arguments.of("no-transfer to EOA", RECIPIENT, Wei.ZERO, 15_000L),
        Arguments.of("no-transfer to contract", RECIPIENT, Wei.ZERO, 15_000L),
        Arguments.of("ETH to existing EOA", RECIPIENT, Wei.ONE, 21_000L),
        Arguments.of("ETH to contract", RECIPIENT, Wei.ONE, 21_000L),
        Arguments.of("no-transfer to delegated account", RECIPIENT, Wei.ZERO, 15_000L),
        Arguments.of("ETH to delegated account", RECIPIENT, Wei.ONE, 21_000L),
        Arguments.of("self-transfer, sender delegated", SENDER, Wei.ONE, 12_000L),
        Arguments.of("ETH creating a new account", RECIPIENT, Wei.ONE, 21_000L),
        // to == null: contract creation. The recipient balance write is already covered by
        // CREATE_ACCESS, but a value-bearing creation still pays the EIP-7708 transfer log (1,756).
        Arguments.of("create, value = 0", null, Wei.ZERO, 23_000L),
        Arguments.of("create, value > 0", null, Wei.ONE, 24_756L),
        Arguments.of("create, target pre-exists", null, Wei.ZERO, 23_000L));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("eip2780ReferenceCases")
  void eip2780IntrinsicGasMatchesReferenceCases(
      final String description, final Address to, final Wei value, final long expected) {
    final Transaction tx = transactionWith(to, value, Bytes.EMPTY, 0);
    assertThat(amsterdamGasCalculator.transactionIntrinsicRegularGas(tx)).isEqualTo(expected);
  }

  @Test
  void eip2780IntrinsicGasChargesCalldataTokens() {
    // data_cost = (zeroBytes * 1 + nonZeroBytes * 4) * 4. Payload of 3 zero + 2 non-zero bytes:
    // (3 + 8) * 4 = 44, on top of the 15,000 for a zero-value call to another account.
    final Transaction tx =
        transactionWith(RECIPIENT, Wei.ZERO, Bytes.fromHexString("0x0000000102"), 0);
    assertThat(amsterdamGasCalculator.transactionIntrinsicRegularGas(tx)).isEqualTo(15_044L);
  }

  @Test
  void eip2780IntrinsicGasChargesInitCodeWords() {
    // Creation of a 33-byte init code: 23,000 + CODE_INIT_PER_WORD (2) * ceil(33/32) = 23,004.
    // All non-zero bytes, so data_cost = 33 * 4 * 4 = 528.
    final Transaction tx = transactionWith(null, Wei.ZERO, Bytes.repeat((byte) 0x1, 33), 0);
    assertThat(amsterdamGasCalculator.transactionIntrinsicRegularGas(tx)).isEqualTo(23_532L);
  }

  @Test
  void eip2780AuthorizationIntrinsicReservesAccountWrite() {
    // EIP-2780: ACCOUNT_WRITE (8,000) + REGULAR_PER_AUTH_BASE_COST (7,816) = 15,816 is reserved
    // per authorization at the intrinsic phase.
    assertThat(amsterdamGasCalculator.delegateCodeGasCost(1)).isEqualTo(15_816L);
    assertThat(amsterdamGasCalculator.delegateCodeGasCost(3)).isEqualTo(47_448L);

    // A zero-value 7702 transaction with one authorization: 15,000 + 15,816 = 30,816.
    final Transaction tx = transactionWith(RECIPIENT, Wei.ZERO, Bytes.EMPTY, 1);
    assertThat(amsterdamGasCalculator.transactionIntrinsicRegularGas(tx)).isEqualTo(30_816L);
  }

  @Test
  void eip2780AuthorityWriteIsRefundedWhenNoAccountGrows() {
    // The worst-case ACCOUNT_WRITE reserved per authorization is refunded for authorities that
    // already existed (and for invalid authorizations, which the processor folds into that count).
    assertThat(amsterdamGasCalculator.calculateDelegateCodeGasRefund(0)).isZero();
    assertThat(amsterdamGasCalculator.calculateDelegateCodeGasRefund(2)).isEqualTo(16_000L);
  }

  private Transaction transactionWith(
      final Address to, final Wei value, final Bytes payload, final int codeDelegations) {
    long zeroBytes = 0L;
    for (int i = 0; i < payload.size(); i++) {
      if (payload.get(i) == (byte) 0x0) {
        zeroBytes++;
      }
    }
    final Transaction tx = mock(Transaction.class, withSettings().strictness(Strictness.LENIENT));
    when(tx.getSender()).thenReturn(SENDER);
    doReturn(Optional.ofNullable(to)).when(tx).getTo();
    when(tx.isContractCreation()).thenReturn(to == null);
    when(tx.getValue()).thenReturn(value);
    when(tx.getPayload()).thenReturn(payload);
    when(tx.getPayloadZeroBytes()).thenReturn(zeroBytes);
    when(tx.getAccessList()).thenReturn(Optional.empty());
    when(tx.codeDelegationListSize()).thenReturn(codeDelegations);
    return tx;
  }

  @Test
  void transactionFloorCostWithoutAccessListMatchesCalldataOnlyFloor() {
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x1, 256));
    when(transaction.getAccessList()).thenReturn(Optional.empty());

    // EIP-2780: 12000 + 256 * 64 = 28384
    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(28384L);
  }

  @Test
  void transactionFloorCostIncludesAccessListBytes() {
    // 10 calldata bytes + 1 address (20 bytes) + 2 keys (2*32 = 64 bytes) = 94 bytes
    // EIP-2780: 12000 + 94 * 64 = 12000 + 6016 = 18016
    final AccessListEntry entry =
        new AccessListEntry(
            Address.fromHexString("0x00000000000000000000000000000000000000aa"),
            List.of(Bytes32.ZERO, Bytes32.ZERO));
    when(transaction.getPayload()).thenReturn(Bytes.repeat((byte) 0x1, 10));
    when(transaction.getAccessList()).thenReturn(Optional.of(List.of(entry)));

    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(18016L);
  }

  @Test
  void transactionFloorCostAggregatesMultipleAccessListEntries() {
    // 4 calldata bytes
    // entry A: 20 address bytes + 0 keys                = 20 bytes
    // entry B: 20 address bytes + 1 key  (1*32 = 32)    = 52 bytes
    // entry C: 20 address bytes + 3 keys (3*32 = 96)    = 116 bytes
    // total bytes = 4 + 20 + 52 + 116 = 192
    // EIP-2780: floor = 12000 + 192 * 64 = 12000 + 12288 = 24288
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

    assertThat(amsterdamGasCalculator.transactionFloorCost(transaction)).isEqualTo(24288L);
  }

  @Test
  void eip8246SelfDestructBalancePreserved() {
    // EIP-8246: Amsterdam preserves the originator's balance on SELFDESTRUCT instead of burning it.
    assertThat(amsterdamGasCalculator.isSelfDestructBalancePreserved()).isTrue();
  }

  @Test
  void eip8246SelfDestructOperationGasCost() {
    // EIP-8038/EIP-8246: static SELFDESTRUCT cost is 5,000; sending a positive balance to a new
    // (non-existent or empty) beneficiary adds ACCOUNT_WRITE (8,000) => 13,000. The cold-access
    // surcharge and NEW_ACCOUNT state gas are charged elsewhere (in SelfDestructOperation).

    // null beneficiary + positive balance => 5,000 + 8,000 = 13,000
    assertThat(amsterdamGasCalculator.selfDestructOperationGasCost(null, Wei.ONE))
        .isEqualTo(13_000L);

    // empty beneficiary + positive balance => 5,000 + 8,000 = 13,000
    final Account emptyBeneficiary = mock(Account.class);
    when(emptyBeneficiary.isEmpty()).thenReturn(true);
    assertThat(amsterdamGasCalculator.selfDestructOperationGasCost(emptyBeneficiary, Wei.ONE))
        .isEqualTo(13_000L);

    // existing (non-empty) beneficiary + positive balance => static 5,000 only
    final Account aliveBeneficiary = mock(Account.class);
    when(aliveBeneficiary.isEmpty()).thenReturn(false);
    assertThat(amsterdamGasCalculator.selfDestructOperationGasCost(aliveBeneficiary, Wei.ONE))
        .isEqualTo(5_000L);

    // null beneficiary + zero balance (nothing sent) => static 5,000 only
    assertThat(amsterdamGasCalculator.selfDestructOperationGasCost(null, Wei.ZERO))
        .isEqualTo(5_000L);
  }
}
