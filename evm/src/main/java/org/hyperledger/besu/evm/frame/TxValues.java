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
package org.hyperledger.besu.evm.frame;

import org.hyperledger.besu.collections.undo.UndoScalar;
import org.hyperledger.besu.collections.undo.UndoSet;
import org.hyperledger.besu.collections.undo.UndoTable;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.HashBasedTable;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Transaction Values used by various EVM Opcodes. These are the values that either do not change or
 * the backing stores whose changes transcend message frames and are not part of state, such as
 * transient storage and address warming.
 *
 * @param blockHashLookup The block hash lookup function
 * @param maxStackSize The maximum stack size
 * @param warmedUpAddresses The warmed-up addresses
 * @param warmedUpStorage The warmed-up storage
 * @param originator The originator address
 * @param gasPrice The gas price
 * @param blobGasPrice The blob gas price
 * @param blockValues The block values
 * @param messageFrameStack The message frame stack
 * @param miningBeneficiary The mining beneficiary address
 * @param versionedHashes The optional list of versioned hashes
 * @param transientStorage The transient storage
 * @param creates The set of addresses that creates
 * @param selfDestructs The set of addresses that self-destructs
 * @param gasRefunds The gas refunds
 * @param stateGasUsed The cumulative state gas used (EIP-8037), undone on revert
 * @param stateGasReservoir The EIP-8037 state gas reservoir (overflow from regular gas budget),
 *     undone on revert
 * @param stateGasSpillBurned EIP-8037 accumulated state gas that spilled from reverted child
 *     frames; NOT undone on revert (permanent burn counter for block accounting)
 */
public record TxValues(
    BlockHashLookup blockHashLookup,
    int maxStackSize,
    UndoSet<Address> warmedUpAddresses,
    UndoTable<Address, Bytes32, Boolean> warmedUpStorage,
    Address originator,
    Wei gasPrice,
    Wei blobGasPrice,
    BlockValues blockValues,
    Deque<MessageFrame> messageFrameStack,
    Address miningBeneficiary,
    Optional<List<VersionedHash>> versionedHashes,
    UndoTable<Address, Bytes32, Bytes32> transientStorage,
    UndoSet<Address> creates,
    UndoSet<Address> selfDestructs,
    UndoScalar<Long> gasRefunds,
    UndoScalar<Long> stateGasUsed,
    UndoScalar<Long> stateGasReservoir,
    long[] stateGasSpillBurned) {

  /**
   * Creates a new TxValues for the initial (depth-0) frame of a transaction. EIP-8037 gas tracking
   * fields are initialized to zero.
   *
   * @param blockHashLookup block hash lookup function
   * @param maxStackSize maximum stack size
   * @param warmedUpAddresses pre-warmed addresses
   * @param originator the transaction originator
   * @param gasPrice the gas price
   * @param blobGasPrice the blob gas price
   * @param blockValues the block values
   * @param miningBeneficiary the mining beneficiary
   * @param versionedHashes optional versioned hashes
   * @return a new TxValues instance
   */
  public static TxValues forTransaction(
      final BlockHashLookup blockHashLookup,
      final int maxStackSize,
      final UndoSet<Address> warmedUpAddresses,
      final Address originator,
      final Wei gasPrice,
      final Wei blobGasPrice,
      final BlockValues blockValues,
      final Address miningBeneficiary,
      final Optional<List<VersionedHash>> versionedHashes) {
    return new TxValues(
        blockHashLookup,
        maxStackSize,
        warmedUpAddresses,
        UndoTable.of(HashBasedTable.create()),
        originator,
        gasPrice,
        blobGasPrice,
        blockValues,
        new ArrayDeque<>(),
        miningBeneficiary,
        versionedHashes,
        UndoTable.of(HashBasedTable.create()),
        UndoSet.of(new HashSet<>()),
        UndoSet.of(new HashSet<>()),
        new UndoScalar<>(0L),
        new UndoScalar<>(0L),
        new UndoScalar<>(0L),
        new long[] {0L});
  }

  /**
   * For all data stored in this record, undo the changes since the mark.
   *
   * @param mark the mark to which it should be rolled back to
   */
  public void undoChanges(final long mark) {
    warmedUpAddresses.undo(mark);
    warmedUpStorage.undo(mark);
    transientStorage.undo(mark);
    creates.undo(mark);
    selfDestructs.undo(mark);
    gasRefunds.undo(mark);
    stateGasUsed.undo(mark);
    stateGasReservoir.undo(mark);
  }
}
