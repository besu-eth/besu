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
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BlockAccessListEncoderTest {

  @Test
  void encodesAListOfManyAccountsAsOneEntryAfterTheOther() {
    final List<AccountChanges> accounts =
        IntStream.range(1, 1000).mapToObj(i -> accountWithChanges(i, 1)).toList();

    assertEncodedAsReference(new BlockAccessList(accounts));
  }

  @Test
  void encodesAnAccountWithManyChangesAsOneChangeAfterTheOther() {
    final List<AccountChanges> accounts =
        List.of(accountWithChanges(1, 1000), accountWithChanges(2, 3), accountWithChanges(3, 700));

    assertEncodedAsReference(new BlockAccessList(accounts));
  }

  private static void assertEncodedAsReference(final BlockAccessList blockAccessList) {
    final Bytes encoded = RLP.encode(out -> BlockAccessListEncoder.encode(blockAccessList, out));

    assertThat(encoded).isEqualTo(RLP.encode(out -> encodeOneByOne(blockAccessList, out)));
    assertThat(BlockAccessListDecoder.decode(new BytesValueRLPInput(encoded, false)))
        .isEqualTo(blockAccessList);
  }

  /** Each account with {@code changes} storage writes to a slot, reads, balance, nonce, codes. */
  private static AccountChanges accountWithChanges(final int account, final int changes) {
    final List<StorageChange> storageChanges = new ArrayList<>();
    final List<BalanceChange> balanceChanges = new ArrayList<>();
    final List<NonceChange> nonceChanges = new ArrayList<>();
    final List<CodeChange> codeChanges = new ArrayList<>();
    final List<SlotRead> slotReads = new ArrayList<>();
    for (int i = 1; i <= changes; i++) {
      storageChanges.add(new StorageChange(i, UInt256.valueOf(i)));
      balanceChanges.add(new BalanceChange(i, Wei.of(1000L * account + i)));
      nonceChanges.add(new NonceChange(i, i));
      codeChanges.add(new CodeChange(i, Bytes.of(i % 256)));
      slotReads.add(new SlotRead(new StorageSlotKey(UInt256.valueOf(100_000 + i))));
    }
    return new AccountChanges(
        Address.fromHexString(String.format("0x%040x", account)),
        List.of(new SlotChanges(new StorageSlotKey(UInt256.ONE), storageChanges)),
        slotReads,
        balanceChanges,
        nonceChanges,
        codeChanges);
  }

  /** The block access list's encoding, written one entry after the other. */
  private static void encodeOneByOne(final BlockAccessList bal, final RLPOutput out) {
    out.writeList(
        bal.accountChanges(),
        (acct, acctOut) -> {
          acctOut.startList();
          acctOut.writeBytes(acct.address().getBytes());
          acctOut.writeList(
              acct.storageChanges(),
              (sc, scOut) -> {
                scOut.startList();
                scOut.writeUInt256Scalar(sc.slot().getSlotKey().orElseThrow());
                scOut.writeList(
                    sc.changes(),
                    (chg, chgOut) -> {
                      chgOut.startList();
                      chgOut.writeUnsignedInt(chg.txIndex());
                      chgOut.writeUInt256Scalar(chg.newValue());
                      chgOut.endList();
                    });
                scOut.endList();
              });
          acctOut.writeList(
              acct.storageReads(),
              (sr, srOut) -> srOut.writeUInt256Scalar(sr.slot().getSlotKey().orElseThrow()));
          acctOut.writeList(
              acct.balanceChanges(),
              (bc, bcOut) -> {
                bcOut.startList();
                bcOut.writeUnsignedInt(bc.txIndex());
                bcOut.writeUInt256Scalar(UInt256.fromBytes(bc.postBalance()));
                bcOut.endList();
              });
          acctOut.writeList(
              acct.nonceChanges(),
              (nc, ncOut) -> {
                ncOut.startList();
                ncOut.writeUnsignedInt(nc.txIndex());
                ncOut.writeLongScalar(nc.newNonce());
                ncOut.endList();
              });
          acctOut.writeList(
              acct.codeChanges(),
              (cc, ccOut) -> {
                ccOut.startList();
                ccOut.writeUnsignedInt(cc.txIndex());
                ccOut.writeBytes(cc.newCode());
                ccOut.endList();
              });
          acctOut.endList();
        });
  }
}
