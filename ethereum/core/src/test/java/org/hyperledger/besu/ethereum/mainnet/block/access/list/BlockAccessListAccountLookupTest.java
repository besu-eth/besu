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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BlockAccessListAccountLookupTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address ABSENT_ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000000000ee");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.ONE);

  @Test
  void looksUpAccountAndSlotByAddress() {
    final Address other = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ADDRESS,
                    List.of(new SlotChanges(SLOT, List.of(new StorageChange(0, UInt256.ONE)))),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of()),
                new AccountChanges(other, List.of(), List.of(), List.of(), List.of(), List.of())));

    final BlockAccessListAccountLookup index = BlockAccessListAccountLookup.of(bal);

    assertThat(index.getAccountChanges(ADDRESS)).isPresent();
    assertThat(index.getAccountChanges(other)).isPresent();
    assertThat(index.getAddressHash(ADDRESS)).contains(ADDRESS.addressHash());
    assertThat(index.getSlotChanges(ADDRESS, SLOT).map(BlockAccessList.SlotChanges::slot))
        .contains(SLOT);
    // "other" is in the BAL but has no storage changes
    assertThat(index.getSlotChanges(other, SLOT)).isEmpty();
  }

  @Test
  void returnsEmptyForAddressAbsentFromBal() {
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ADDRESS, List.of(), List.of(), List.of(), List.of(), List.of())));

    final BlockAccessListAccountLookup index = BlockAccessListAccountLookup.of(bal);

    assertThat(index.getAccountChanges(ABSENT_ADDRESS)).isEmpty();
    assertThat(index.getAddressHash(ABSENT_ADDRESS)).isEmpty();
    assertThat(index.getSlotChanges(ABSENT_ADDRESS, SLOT)).isEmpty();
  }

  @Test
  void readsEachPreBlockAccountOnceAcrossAdoptedCodeHashes() {
    final Address writer = Address.fromHexString("0x00000000000000000000000000000000000000b1");
    final Address firstSource = Address.fromHexString("0x00000000000000000000000000000000000000b2");
    final Address secondSource =
        Address.fromHexString("0x00000000000000000000000000000000000000b3");
    final Address codeless = Address.fromHexString("0x00000000000000000000000000000000000000b4");
    final Bytes writtenCode = Bytes.fromHexString("0x01");
    final Bytes firstCode = Bytes.fromHexString("0x02");
    final Bytes secondCode = Bytes.fromHexString("0x03");
    final List<Hash> unresolvable =
        List.of(Hash.hash(Bytes.fromHexString("0x04")), Hash.hash(Bytes.fromHexString("0x05")));
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                accountChanges(ADDRESS),
                accountChanges(writer, new CodeChange(1, writtenCode)),
                accountChanges(firstSource),
                accountChanges(secondSource),
                accountChanges(codeless)));
    final RecordingPreBlockCodeReader preBlockCode =
        new RecordingPreBlockCodeReader(
            Map.of(
                ADDRESS,
                Bytes.fromHexString("0xAA"),
                firstSource,
                firstCode,
                secondSource,
                secondCode));

    final BlockAccessListAccountLookup lookup = BlockAccessListAccountLookup.of(bal);

    for (int i = 0; i < 2; i++) {
      assertThat(lookup.resolveAdoptedCode(Hash.hash(writtenCode), preBlockCode))
          .contains(writtenCode);
      assertThat(lookup.resolveAdoptedCode(Hash.hash(firstCode), preBlockCode)).contains(firstCode);
      assertThat(lookup.resolveAdoptedCode(Hash.hash(secondCode), preBlockCode))
          .contains(secondCode);
      for (final Hash codeHash : unresolvable) {
        assertThat(lookup.resolveAdoptedCode(codeHash, preBlockCode)).isEmpty();
      }
    }

    assertThat(preBlockCode.codeHashReads)
        .containsExactly(ADDRESS, writer, firstSource, secondSource, codeless);
    assertThat(preBlockCode.codeReads).containsExactly(firstSource, secondSource);
  }

  @Test
  void writtenCodeAloneReadsNoPreBlockState() {
    final Bytes writtenCode = Bytes.fromHexString("0x01");
    final BlockAccessListAccountLookup lookup =
        BlockAccessListAccountLookup.of(
            new BlockAccessList(List.of(accountChanges(ADDRESS, new CodeChange(1, writtenCode)))));
    final RecordingPreBlockCodeReader preBlockCode =
        new RecordingPreBlockCodeReader(Map.of(ADDRESS, Bytes.fromHexString("0xAA")));

    assertThat(lookup.resolveWrittenCode(Hash.hash(writtenCode))).contains(writtenCode);
    assertThat(lookup.resolveWrittenCode(Hash.hash(Bytes.fromHexString("0xAA")))).isEmpty();
    assertThat(lookup.resolveAdoptedCode(Hash.hash(writtenCode), preBlockCode))
        .contains(writtenCode);
    assertThat(preBlockCode.codeHashReads).isEmpty();
    // resolving from written code alone does not leave an empty pre-block index behind
    assertThat(lookup.resolveAdoptedCode(Hash.hash(Bytes.fromHexString("0xAA")), preBlockCode))
        .contains(Bytes.fromHexString("0xAA"));
  }

  private static AccountChanges accountChanges(
      final Address address, final CodeChange... codeChanges) {
    return new AccountChanges(
        address, List.of(), List.of(), List.of(), List.of(), List.of(codeChanges));
  }
}
