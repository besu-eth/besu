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
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import org.apache.tuweni.units.bigints.UInt256;

public final class BlockAccessListEncoder {

  private BlockAccessListEncoder() {}

  public static void encode(final BlockAccessList bal, final RLPOutput out) {
    out.writeList(
        bal.accountChanges(),
        (acct, acctOut) -> {
          acctOut.startList();
          acctOut.writeBytes(acct.address().getBytes());

          acctOut.writeList(
              acct.storageChanges(),
              (sc, scOut) -> {
                scOut.startList();
                scOut.writeUInt256Scalar(sc.slot().getSlotKey().get());
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
              (sr, srOut) -> srOut.writeUInt256Scalar(sr.slot().getSlotKey().get()));

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
