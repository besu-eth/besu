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
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public final class BlockAccessListDecoder {

  private BlockAccessListDecoder() {}

  public static BlockAccessList decode(final RLPInput in) {
    final List<AccountChanges> accounts = new ArrayList<>();

    in.enterList();
    while (!in.isEndOfCurrentList()) {
      RLPInput acctIn = in.readAsRlp();
      acctIn.enterList();

      Address address = Address.readFrom(acctIn);

      List<SlotChanges> slotChanges =
          acctIn.readList(
              scIn -> {
                scIn.enterList();
                StorageSlotKey slot = new StorageSlotKey(UInt256.fromBytes(scIn.readBytes()));
                List<StorageChange> changes =
                    scIn.readList(
                        changeIn -> {
                          changeIn.enterList();
                          long txIndex = changeIn.readUnsignedIntScalar();
                          UInt256 newVal = UInt256.fromBytes(changeIn.readBytes());
                          changeIn.leaveList();
                          return new StorageChange(txIndex, newVal);
                        });
                scIn.leaveList();
                return new SlotChanges(slot, changes);
              });

      List<SlotRead> reads =
          acctIn.readList(r -> new SlotRead(new StorageSlotKey(UInt256.fromBytes(r.readBytes()))));

      List<BalanceChange> balances =
          acctIn.readList(
              bcIn -> {
                bcIn.enterList();
                long txIndex = bcIn.readUnsignedIntScalar();
                Wei postBalance = Wei.of(UInt256.fromBytes(bcIn.readBytes()));
                bcIn.leaveList();
                return new BalanceChange(txIndex, postBalance);
              });

      List<NonceChange> nonces =
          acctIn.readList(
              ncIn -> {
                ncIn.enterList();
                long txIndex = ncIn.readUnsignedIntScalar();
                long newNonce = ncIn.readLongScalar();
                ncIn.leaveList();
                return new NonceChange(txIndex, newNonce);
              });

      List<CodeChange> codes =
          acctIn.readList(
              ccIn -> {
                ccIn.enterList();
                long txIndex = ccIn.readUnsignedIntScalar();
                Bytes newCode = ccIn.readBytes();
                ccIn.leaveList();
                return new CodeChange(txIndex, newCode);
              });

      acctIn.leaveList();

      accounts.add(new AccountChanges(address, slotChanges, reads, balances, nonces, codes));
    }
    in.leaveList();

    return new BlockAccessList(accounts);
  }
}
