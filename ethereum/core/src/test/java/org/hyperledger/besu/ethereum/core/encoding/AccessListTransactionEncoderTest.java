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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

public class AccessListTransactionEncoderTest {

  private static final long UINT32_MAX = 0xFFFF_FFFFL;

  @Test
  void shouldEncodeAndDecodeAccessListWithAllFields() {
    final Address address = Address.fromHexString("0x00000000219ab540356cbb839cbe05303d7705fa");
    final StorageSlotKey slotKey = new StorageSlotKey(Wei.ONE.toUInt256());

    final StorageChange write = new StorageChange(0, Wei.ONE.toUInt256());
    final SlotChanges slotChanges = new SlotChanges(slotKey, List.of(write));
    final SlotRead slotRead = new SlotRead(slotKey);

    final BalanceChange balanceChange = new BalanceChange(0, Wei.fromEth(3));
    final CodeChange codeChange = new CodeChange(1, Bytes.fromHexString("0x6001600101"));
    final NonceChange nonceChange = new NonceChange(2, 42L);

    final AccountChanges accountChanges =
        new AccountChanges(
            address,
            List.of(slotChanges),
            List.of(slotRead),
            List.of(balanceChange),
            List.of(nonceChange),
            List.of(codeChange));

    final BlockAccessList originalAccessList = new BlockAccessList(List.of(accountChanges));

    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    BlockAccessListEncoder.encode(originalAccessList, output);
    final Bytes encoded = output.encoded();

    final BytesValueRLPInput input = new BytesValueRLPInput(encoded, false);
    final BlockAccessList decodedAccessList = BlockAccessListDecoder.decode(input);

    assertThat(decodedAccessList.accountChanges()).hasSize(1);
    final AccountChanges decoded = decodedAccessList.accountChanges().get(0);

    assertThat(decoded.address()).isEqualTo(address);

    assertThat(decoded.storageChanges()).hasSize(1);
    assertThat(decoded.storageChanges().get(0).slot()).isEqualTo(slotKey);
    assertThat(decoded.storageChanges().get(0).changes()).hasSize(1);
    assertThat(decoded.storageChanges().get(0).changes().get(0).txIndex()).isEqualTo(0);
    assertThat(decoded.storageChanges().get(0).changes().get(0).newValue())
        .isEqualTo(Wei.ONE.toBytes());

    assertThat(decoded.storageReads()).containsExactly(slotRead);

    assertThat(decoded.balanceChanges()).containsExactly(balanceChange);
    assertThat(decoded.nonceChanges()).containsExactly(nonceChange);
    assertThat(decoded.codeChanges()).containsExactly(codeChange);
  }

  @Test
  void shouldEncodeAndDecodeAccessListWithEmptyAccountChanges() {
    final BlockAccessList accessList = new BlockAccessList(List.of());

    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    BlockAccessListEncoder.encode(accessList, output);
    final Bytes encoded = output.encoded();

    final BytesValueRLPInput input = new BytesValueRLPInput(encoded, false);
    final BlockAccessList decoded = BlockAccessListDecoder.decode(input);

    assertThat(decoded.accountChanges()).isEmpty();
  }

  @Test
  void shouldRoundTripMaxUint32BlockAccessIndex() {
    final Address address = Address.fromHexString("0x00000000219ab540356cbb839cbe05303d7705fa");
    final StorageSlotKey slotKey = new StorageSlotKey(Wei.ONE.toUInt256());
    final AccountChanges accountChanges =
        new AccountChanges(
            address,
            List.of(
                new SlotChanges(
                    slotKey, List.of(new StorageChange(UINT32_MAX, UInt256.valueOf(1))))),
            List.of(),
            List.of(new BalanceChange(UINT32_MAX, Wei.ONE)),
            List.of(new NonceChange(UINT32_MAX, 1L)),
            List.of(new CodeChange(UINT32_MAX, Bytes.EMPTY)));

    final BlockAccessList original = new BlockAccessList(List.of(accountChanges));
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    BlockAccessListEncoder.encode(original, output);
    final BlockAccessList decoded =
        BlockAccessListDecoder.decode(new BytesValueRLPInput(output.encoded(), false));

    final AccountChanges decodedAcct = decoded.accountChanges().get(0);
    assertThat(decodedAcct.storageChanges().getFirst().changes().getFirst().txIndex())
        .isEqualTo(UINT32_MAX);
    assertThat(decodedAcct.balanceChanges().getFirst().txIndex()).isEqualTo(UINT32_MAX);
    assertThat(decodedAcct.nonceChanges().getFirst().txIndex()).isEqualTo(UINT32_MAX);
    assertThat(decodedAcct.codeChanges().getFirst().txIndex()).isEqualTo(UINT32_MAX);
  }
}
