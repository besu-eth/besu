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
package org.hyperledger.besu.consensus.merge.blockcreation;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;

public class PayloadIdentifierTest {

  private static final BlockHeader PARENT = new BlockHeaderTestFixture().buildHeader();

  @Test
  public void serializesToEvenHexRepresentation() {

    List.of("0x150dd", "0xdeadbeef", Long.valueOf(Long.MAX_VALUE).toString(), "0").stream()
        .forEach(
            id -> {
              PayloadIdentifier idTest = new PayloadIdentifier(id);
              assertThat(idTest.toHexString().length() % 2).isEqualTo(0);
              assertThat(idTest.toShortHexString().length() % 2).isEqualTo(0);
              assertThat(idTest.serialize().length() % 2).isEqualTo(0);
            });
  }

  @Test
  public void conversionCoverage() {
    var idTest =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(Bytes32.random())
                .feeRecipient(Address.fromHexString("0x42"))
                .build());
    assertThat(new PayloadIdentifier(idTest.getAsBigInteger().longValue())).isEqualTo(idTest);
  }

  @Test
  public void differentWithdrawalAmountsYieldDifferentHash() {
    final List<Withdrawal> withdrawals1 =
        List.of(
            new Withdrawal(
                UInt64.valueOf(100),
                UInt64.valueOf(1000),
                Address.fromHexString("0x1"),
                GWei.of(1)),
            new Withdrawal(
                UInt64.valueOf(200),
                UInt64.valueOf(2000),
                Address.fromHexString("0x2"),
                GWei.of(2)));
    final List<Withdrawal> withdrawals2 =
        List.of(
            new Withdrawal(
                UInt64.valueOf(100),
                UInt64.valueOf(1000),
                Address.fromHexString("0x1"),
                GWei.of(3)),
            new Withdrawal(
                UInt64.valueOf(200),
                UInt64.valueOf(2000),
                Address.fromHexString("0x2"),
                GWei.of(4)));
    final Bytes32 prevRandao = Bytes32.random();
    var idForWithdrawals1 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .withdrawals(withdrawals1)
                .build());
    var idForWithdrawals2 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .withdrawals(withdrawals2)
                .build());
    assertThat(idForWithdrawals1).isNotEqualTo(idForWithdrawals2);
  }

  @Test
  public void differentOrderedWithdrawalsYieldSameHash() {
    final List<Withdrawal> withdrawals1 =
        List.of(
            new Withdrawal(
                UInt64.valueOf(100),
                UInt64.valueOf(1000),
                Address.fromHexString("0x1"),
                GWei.of(1)),
            new Withdrawal(
                UInt64.valueOf(200),
                UInt64.valueOf(2000),
                Address.fromHexString("0x2"),
                GWei.of(2)));
    final List<Withdrawal> withdrawals2 =
        List.of(
            new Withdrawal(
                UInt64.valueOf(200),
                UInt64.valueOf(2000),
                Address.fromHexString("0x2"),
                GWei.of(2)),
            new Withdrawal(
                UInt64.valueOf(100),
                UInt64.valueOf(1000),
                Address.fromHexString("0x1"),
                GWei.of(1)));
    final Bytes32 prevRandao = Bytes32.random();
    var idForWithdrawals1 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .withdrawals(withdrawals1)
                .build());
    var idForWithdrawals2 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .withdrawals(withdrawals2)
                .build());
    assertThat(idForWithdrawals1).isEqualTo(idForWithdrawals2);
  }

  @Test
  public void emptyOptionalAndEmptyListWithdrawalsYieldDifferentHash() {
    final Bytes32 prevRandao = Bytes32.random();
    var idForWithdrawals1 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .build());
    var idForWithdrawals2 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .withdrawals(emptyList())
                .build());
    assertThat(idForWithdrawals1).isNotEqualTo(idForWithdrawals2);
  }

  @Test
  public void emptyOptionalAndNonEmptyParentBeaconBlockRootYieldDifferentHash() {
    final Bytes32 prevRandao = Bytes32.random();
    var idForWithdrawals1 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .build());
    var idForWithdrawals2 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .parentBeaconBlockRoot(Bytes32.ZERO)
                .build());
    assertThat(idForWithdrawals1).isNotEqualTo(idForWithdrawals2);
  }

  @Test
  public void differentParentBeaconBlockRootYieldDifferentHash() {
    final Bytes32 prevRandao = Bytes32.random();
    var idForWithdrawals1 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .parentBeaconBlockRoot(Bytes32.fromHexStringLenient("0x1"))
                .build());
    var idForWithdrawals2 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .parentBeaconBlockRoot(Bytes32.ZERO)
                .build());
    assertThat(idForWithdrawals1).isNotEqualTo(idForWithdrawals2);
  }

  @Test
  public void differentSlotNumberYieldDifferentHash() {
    final Bytes32 prevRandao = Bytes32.random();
    var idForWithdrawals1 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .parentBeaconBlockRoot(Bytes32.fromHexStringLenient("0x1"))
                .slotNumber(100L)
                .build());
    var idForWithdrawals2 =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(PARENT)
                .timestamp(1337L)
                .prevRandao(prevRandao)
                .feeRecipient(Address.fromHexString("0x42"))
                .parentBeaconBlockRoot(Bytes32.fromHexStringLenient("0x1"))
                .slotNumber(101L)
                .build());
    assertThat(idForWithdrawals1).isNotEqualTo(idForWithdrawals2);
  }
}
