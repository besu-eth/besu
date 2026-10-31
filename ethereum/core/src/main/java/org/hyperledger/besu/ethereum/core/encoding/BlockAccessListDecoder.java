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
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public final class BlockAccessListDecoder {

  /**
   * Lists of at least this many entries, whether the accounts or the changes of one account, are
   * decoded in parallel, one entry per task, as {@link BlockAccessListEncoder} encodes them.
   */
  private static final int MIN_ENTRIES_TO_DECODE_IN_PARALLEL = 512;

  private BlockAccessListDecoder() {}

  public static BlockAccessList decode(final RLPInput input) {
    // read on its own: its raw encoding must not be the whole input when it is part of a larger
    // one, e.g. a consensus message
    final RLPInput in = input.readAsRlp();
    final List<AccountChanges> accounts = readList(in, BlockAccessListDecoder::readAccount);
    return new BlockAccessList(accounts, Optional.of(in.raw()));
  }

  private static AccountChanges readAccount(final RLPInput acctIn) {
    acctIn.enterList();

    Address address = Address.readFrom(acctIn);

    List<SlotChanges> slotChanges =
        readList(
            acctIn,
            scIn -> {
              scIn.enterList();
              StorageSlotKey slot = new StorageSlotKey(scIn.readUInt256Scalar());
              List<StorageChange> changes =
                  readList(
                      scIn,
                      changeIn -> {
                        changeIn.enterList();
                        long txIndex = changeIn.readUnsignedIntScalar();
                        UInt256 newVal = changeIn.readUInt256Scalar();
                        changeIn.leaveList();
                        return new StorageChange(txIndex, newVal);
                      });
              // An empty change list is well-formed RLP. The EIP-7928 "at least one storage
              // change" rule is left to MainnetBlockAccessListValidator, so the block comes back
              // INVALID instead of engine_newPayload failing on invalid params.
              scIn.leaveList();
              return new SlotChanges(slot, changes);
            });

    List<SlotRead> reads =
        readList(acctIn, r -> new SlotRead(new StorageSlotKey(r.readUInt256Scalar())));

    List<BalanceChange> balances =
        readList(
            acctIn,
            bcIn -> {
              bcIn.enterList();
              long txIndex = bcIn.readUnsignedIntScalar();
              Wei postBalance = Wei.of(bcIn.readUInt256Scalar());
              bcIn.leaveList();
              return new BalanceChange(txIndex, postBalance);
            });

    List<NonceChange> nonces =
        readList(
            acctIn,
            ncIn -> {
              ncIn.enterList();
              long txIndex = ncIn.readUnsignedIntScalar();
              long newNonce = ncIn.readLongScalar();
              ncIn.leaveList();
              return new NonceChange(txIndex, newNonce);
            });

    List<CodeChange> codes =
        readList(
            acctIn,
            ccIn -> {
              ccIn.enterList();
              long txIndex = ccIn.readUnsignedIntScalar();
              Bytes newCode = ccIn.readBytes();
              ccIn.leaveList();
              return new CodeChange(txIndex, newCode);
            });

    acctIn.leaveList();

    return new AccountChanges(address, slotChanges, reads, balances, nonces, codes);
  }

  /**
   * Reads the next list of the input like {@link RLPInput#readList}, a long one by splitting it
   * into its entries first and decoding those in parallel.
   */
  private static <T> List<T> readList(final RLPInput in, final Function<RLPInput, T> entryReader) {
    final int size = in.enterList();
    if (size < MIN_ENTRIES_TO_DECODE_IN_PARALLEL) {
      final List<T> entries = size == 0 ? List.of() : new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        entries.add(readEntry(in, entryReader, i));
      }
      in.leaveList();
      return entries;
    }
    final List<RLPInput> encodedEntries = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      encodedEntries.add(in.readAsRlp());
    }
    in.leaveList();
    return IntStream.range(0, size)
        .parallel()
        .mapToObj(i -> readEntry(encodedEntries.get(i), entryReader, i))
        .toList();
  }

  private static <T> T readEntry(
      final RLPInput in, final Function<RLPInput, T> entryReader, final int index) {
    try {
      return entryReader.apply(in);
    } catch (final RLPException e) {
      throw e;
    } catch (final Exception e) {
      throw new RLPException(
          String.format(
              "Error applying element decoding function on element %d of the list", index),
          e);
    }
  }
}
