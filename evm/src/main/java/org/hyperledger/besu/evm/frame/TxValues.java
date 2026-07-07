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
 * Transaction-lifetime values shared across all message frames of a transaction.
 *
 * <p>Holds the immutable per-transaction inputs (originator, gas price, block values, …) and the
 * mutable counters whose state must persist across frame boundaries: the {@link UndoScalar} /
 * {@link UndoSet} / {@link UndoTable} fields are rolled back on revert via {@link #undoChanges}.
 */
public class TxValues {

  private final BlockHashLookup blockHashLookup;
  private final int maxStackSize;
  private final UndoSet<Address> warmedUpAddresses;
  private final UndoTable<Address, Bytes32, Boolean> warmedUpStorage;
  private final Address originator;
  private final Wei gasPrice;
  private final Wei blobGasPrice;
  private final BlockValues blockValues;
  private final Deque<MessageFrame> messageFrameStack;
  private final Address miningBeneficiary;
  private final Optional<List<VersionedHash>> versionedHashes;
  private final UndoTable<Address, Bytes32, Bytes32> transientStorage;
  private final UndoSet<Address> creates;
  private final UndoSet<Address> selfDestructs;
  private final UndoScalar<Long> gasRefunds;
  private final UndoScalar<Long> stateGasUsed;
  private final UndoScalar<Long> stateGasReservoir;

  TxValues(
      final BlockHashLookup blockHashLookup,
      final int maxStackSize,
      final UndoSet<Address> warmedUpAddresses,
      final UndoTable<Address, Bytes32, Boolean> warmedUpStorage,
      final Address originator,
      final Wei gasPrice,
      final Wei blobGasPrice,
      final BlockValues blockValues,
      final Deque<MessageFrame> messageFrameStack,
      final Address miningBeneficiary,
      final Optional<List<VersionedHash>> versionedHashes,
      final UndoTable<Address, Bytes32, Bytes32> transientStorage,
      final UndoSet<Address> creates,
      final UndoSet<Address> selfDestructs,
      final UndoScalar<Long> gasRefunds,
      final UndoScalar<Long> stateGasUsed,
      final UndoScalar<Long> stateGasReservoir) {
    this.blockHashLookup = blockHashLookup;
    this.maxStackSize = maxStackSize;
    this.warmedUpAddresses = warmedUpAddresses;
    this.warmedUpStorage = warmedUpStorage;
    this.originator = originator;
    this.gasPrice = gasPrice;
    this.blobGasPrice = blobGasPrice;
    this.blockValues = blockValues;
    this.messageFrameStack = messageFrameStack;
    this.miningBeneficiary = miningBeneficiary;
    this.versionedHashes = versionedHashes;
    this.transientStorage = transientStorage;
    this.creates = creates;
    this.selfDestructs = selfDestructs;
    this.gasRefunds = gasRefunds;
    this.stateGasUsed = stateGasUsed;
    this.stateGasReservoir = stateGasReservoir;
  }

  /**
   * Creates a new TxValues for the initial (depth-0) frame of a transaction. Intrinsic EIP-8037
   * charges that should be in effect at frame entry are passed in via {@code initialStateGasUsed}
   * and {@code initialStateGasReservoir} so the frame is constructed with its final values and no
   * post-hoc setters / undo-mark advances are required.
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
   * @param initialStateGasUsed cumulative state gas charged at frame entry (intrinsic state gas)
   * @param initialStateGasReservoir state-gas reservoir balance at frame entry
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
      final Optional<List<VersionedHash>> versionedHashes,
      final long initialStateGasUsed,
      final long initialStateGasReservoir) {
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
        new UndoScalar<>(initialStateGasUsed),
        new UndoScalar<>(initialStateGasReservoir));
  }

  /**
   * For all data stored in this object, undo the changes since the mark.
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

  /**
   * Returns the block hash lookup function.
   *
   * @return the block hash lookup function
   */
  public BlockHashLookup blockHashLookup() {
    return blockHashLookup;
  }

  /**
   * Returns the maximum stack size.
   *
   * @return the maximum stack size
   */
  public int maxStackSize() {
    return maxStackSize;
  }

  /**
   * Returns the set of warmed-up addresses.
   *
   * @return the warmed-up addresses
   */
  public UndoSet<Address> warmedUpAddresses() {
    return warmedUpAddresses;
  }

  /**
   * Returns the warmed-up storage slots.
   *
   * @return the warmed-up storage slots
   */
  public UndoTable<Address, Bytes32, Boolean> warmedUpStorage() {
    return warmedUpStorage;
  }

  /**
   * Returns the transaction originator.
   *
   * @return the transaction originator
   */
  public Address originator() {
    return originator;
  }

  /**
   * Returns the transaction gas price.
   *
   * @return the transaction gas price
   */
  public Wei gasPrice() {
    return gasPrice;
  }

  /**
   * Returns the transaction blob gas price.
   *
   * @return the transaction blob gas price
   */
  public Wei blobGasPrice() {
    return blobGasPrice;
  }

  /**
   * Returns the block values.
   *
   * @return the block values
   */
  public BlockValues blockValues() {
    return blockValues;
  }

  /**
   * Returns the message frame stack for the transaction.
   *
   * @return the message frame stack
   */
  public Deque<MessageFrame> messageFrameStack() {
    return messageFrameStack;
  }

  /**
   * Returns the mining beneficiary address.
   *
   * @return the mining beneficiary
   */
  public Address miningBeneficiary() {
    return miningBeneficiary;
  }

  /**
   * Returns the optional versioned hashes.
   *
   * @return the versioned hashes
   */
  public Optional<List<VersionedHash>> versionedHashes() {
    return versionedHashes;
  }

  /**
   * Returns the transient storage.
   *
   * @return the transient storage
   */
  public UndoTable<Address, Bytes32, Bytes32> transientStorage() {
    return transientStorage;
  }

  /**
   * Returns the set of addresses created in this transaction.
   *
   * @return the creates set
   */
  public UndoSet<Address> creates() {
    return creates;
  }

  /**
   * Returns the set of addresses self-destructed in this transaction.
   *
   * @return the self-destructs set
   */
  public UndoSet<Address> selfDestructs() {
    return selfDestructs;
  }

  /**
   * Returns the accumulated regular-gas refund counter.
   *
   * @return the gas refunds
   */
  public UndoScalar<Long> gasRefunds() {
    return gasRefunds;
  }

  /**
   * Returns the cumulative state gas used (EIP-8037).
   *
   * @return the state gas used counter
   */
  public UndoScalar<Long> stateGasUsed() {
    return stateGasUsed;
  }

  /**
   * Returns the state-gas reservoir balance (EIP-8037).
   *
   * @return the state gas reservoir
   */
  public UndoScalar<Long> stateGasReservoir() {
    return stateGasReservoir;
  }
}
