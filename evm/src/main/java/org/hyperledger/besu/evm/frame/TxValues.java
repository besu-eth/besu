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

import org.hyperledger.besu.collections.undo.UndoList;
import org.hyperledger.besu.collections.undo.UndoScalar;
import org.hyperledger.besu.collections.undo.UndoSet;
import org.hyperledger.besu.collections.undo.UndoTable;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

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
 * @param stateChanges EIP-8037: append-only log of state-change events recorded at the opcode site;
 *     consumed by frame-end aggregation. Undone on revert.
 * @param stateGasSpilledTotal EIP-8037: cumulative state-gas <em>spillover</em> from {@link
 *     MessageFrame#consumeStateGas} (the portion that drained {@code gasRemaining} after the
 *     reservoir was empty). UndoScalar — rolls back with {@link #undoChanges} alongside the
 *     reservoir; the {@code stateGasSpilledLost} counter records the rolled-back delta separately
 *     since {@code gasRemaining} itself is not restored on rollback.
 * @param stateGasSpilledLost EIP-8037: cumulative spillover that was rolled back from {@link
 *     #stateGasUsed} via {@link #undoChanges} but whose gas-left consumption survives the rollback.
 *     Mutable counter that persists across rollbacks. Decremented by reservoir credits that
 *     effectively offset the lost spillover (top-level error handler). Block-gas accounting
 *     attributes this to the state dimension instead of letting it leak into regular gas.
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
    UndoList<StateChange> stateChanges,
    UndoScalar<Long> stateGasSpilledTotal,
    AtomicLong stateGasSpilledLost) {

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
        new UndoList<>(new ArrayList<>()),
        new UndoScalar<>(0L),
        new AtomicLong());
  }

  /**
   * For all data stored in this record, undo the changes since the mark.
   *
   * @param mark the mark to which it should be rolled back to
   */
  public void undoChanges(final long mark) {
    final long spilledBefore = stateGasSpilledTotal.get();
    warmedUpAddresses.undo(mark);
    warmedUpStorage.undo(mark);
    transientStorage.undo(mark);
    creates.undo(mark);
    selfDestructs.undo(mark);
    gasRefunds.undo(mark);
    stateGasUsed.undo(mark);
    stateGasReservoir.undo(mark);
    stateChanges.undo(mark);
    stateGasSpilledTotal.undo(mark);
    // Spillover that just rolled back from `stateGasUsed` still consumed `gasRemaining`
    // (gas_left isn't restored on rollback). Stash the delta non-undoably so block-gas
    // accounting can attribute it to the state dimension.
    stateGasSpilledLost.addAndGet(Math.max(0L, spilledBefore - stateGasSpilledTotal.get()));
  }
}
