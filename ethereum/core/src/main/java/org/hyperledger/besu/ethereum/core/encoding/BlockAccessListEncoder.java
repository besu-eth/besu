/*
 * Copyright ConsenSys AG.
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

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;
import java.util.function.BiConsumer;

import org.apache.tuweni.bytes.Bytes;

public final class BlockAccessListEncoder {

  /**
   * Lists of at least this many entries, whether the accounts or the changes of one account, are
   * encoded in parallel, one entry per task: a block of many transactions has a list of many
   * accounts or an account with a change for each transaction.
   */
  private static final int MIN_ENTRIES_TO_ENCODE_IN_PARALLEL = 512;

  private BlockAccessListEncoder() {}

  public static void encode(final BlockAccessList bal, final RLPOutput out) {
    writeList(out, bal.accountChanges(), BlockAccessListEncoder::encodeAccount);
  }

  private static void encodeAccount(
      final BlockAccessList.AccountChanges acct, final RLPOutput acctOut) {
    acctOut.startList();
    acctOut.writeBytes(acct.address().getBytes());

    writeList(
        acctOut,
        acct.storageChanges(),
        (sc, scOut) -> {
          scOut.startList();
          scOut.writeUInt256Scalar(sc.slot().getSlotKey().get());
          writeList(
              scOut,
              sc.changes(),
              (chg, chgOut) -> {
                chgOut.startList();
                chgOut.writeUnsignedInt(chg.txIndex());
                chgOut.writeUInt256Scalar(chg.newValue());
                chgOut.endList();
              });
          scOut.endList();
        });

    writeList(
        acctOut,
        acct.storageReads(),
        (sr, srOut) -> srOut.writeUInt256Scalar(sr.slot().getSlotKey().get()));

    writeList(
        acctOut,
        acct.balanceChanges(),
        (bc, bcOut) -> {
          bcOut.startList();
          bcOut.writeUnsignedInt(bc.txIndex());
          bcOut.writeUInt256Scalar(bc.postBalance().toUInt256());
          bcOut.endList();
        });

    writeList(
        acctOut,
        acct.nonceChanges(),
        (nc, ncOut) -> {
          ncOut.startList();
          ncOut.writeUnsignedInt(nc.txIndex());
          ncOut.writeLongScalar(nc.newNonce());
          ncOut.endList();
        });

    writeList(
        acctOut,
        acct.codeChanges(),
        (cc, ccOut) -> {
          ccOut.startList();
          ccOut.writeUnsignedInt(cc.txIndex());
          ccOut.writeBytes(cc.newCode());
          ccOut.endList();
        });

    acctOut.endList();
  }

  private static <T> void writeList(
      final RLPOutput out, final List<T> entries, final BiConsumer<T, RLPOutput> entryEncoder) {
    if (entries.size() < MIN_ENTRIES_TO_ENCODE_IN_PARALLEL) {
      out.writeList(entries, entryEncoder);
      return;
    }
    final List<Bytes> encodedEntries =
        entries.parallelStream()
            .map(entry -> RLP.encode(entryOut -> entryEncoder.accept(entry, entryOut)))
            .toList();
    out.startList();
    encodedEntries.forEach(out::writeRaw);
    out.endList();
  }
}
