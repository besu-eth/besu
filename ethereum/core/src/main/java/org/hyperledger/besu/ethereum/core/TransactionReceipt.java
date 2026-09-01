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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.mainnet.TransactionReceiptType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.apache.tuweni.bytes.Bytes;

/**
 * A transaction receipt, containing information pertaining a transaction execution.
 *
 * <p>Transaction receipts have two different formats: state root-encoded and status-encoded. The
 * difference between these two formats is that the state root-encoded transaction receipt contains
 * the state root for world state after the transaction has been processed (e.g. not invalid) and
 * the status-encoded transaction receipt instead has contains the status of the transaction (e.g. 1
 * for success and 0 for failure). The other transaction receipt fields are the same for both
 * formats: logs, logs bloom, and cumulative gas used in the block. The TransactionReceiptType
 * attribute is the best way to check which format has been used.
 */
public class TransactionReceipt implements org.hyperledger.besu.plugin.data.TransactionReceipt {

  private static final int NONEXISTENT = -1;

  private final TransactionType transactionType;
  private final Hash stateRoot;
  private final long cumulativeGasUsed;
  private final List<Log> logs;
  private final LogsBloomFilter bloomFilter;
  private final int status;
  private final TransactionReceiptType transactionReceiptType;
  private final Optional<Bytes> revertReason;
  // EIP-8141 frame transaction receipts: the fee payer and the per-frame receipts.
  private final Optional<Address> maybePayer;
  private final Optional<List<FrameReceipt>> maybeFrameReceipts;

  /**
   * Creates an instance of a state root-encoded transaction receipt.
   *
   * @param stateRoot the state root for the world state after the transaction has been processed
   * @param cumulativeGasUsed the total amount of gas consumed in the block after this transaction
   * @param logs the logs generated within the transaction
   * @param revertReason the revert reason for a failed transaction (if applicable)
   */
  public TransactionReceipt(
      final Hash stateRoot,
      final long cumulativeGasUsed,
      final List<Log> logs,
      final Optional<Bytes> revertReason) {
    this(
        TransactionType.FRONTIER,
        stateRoot,
        NONEXISTENT,
        cumulativeGasUsed,
        logs,
        LogsBloomFilter.builder().insertLogs(logs).build(),
        revertReason);
  }

  public TransactionReceipt(
      final TransactionType transactionType,
      final Hash stateRoot,
      final long cumulativeGasUsed,
      final List<Log> logs,
      final LogsBloomFilter bloomFilter,
      final Optional<Bytes> revertReason) {
    this(
        transactionType,
        stateRoot,
        NONEXISTENT,
        cumulativeGasUsed,
        logs,
        bloomFilter,
        revertReason);
  }

  /**
   * Creates an instance of a status-encoded transaction receipt.
   *
   * @param status the status code for the transaction (1 for success and 0 for failure)
   * @param cumulativeGasUsed the total amount of gas consumed in the block after this transaction
   * @param logs the logs generated within the transaction
   * @param revertReason the revert reason for a failed transaction (if applicable)
   */
  public TransactionReceipt(
      final int status,
      final long cumulativeGasUsed,
      final List<Log> logs,
      final Optional<Bytes> revertReason) {
    this(
        TransactionType.FRONTIER,
        null,
        status,
        cumulativeGasUsed,
        logs,
        LogsBloomFilter.builder().insertLogs(logs).build(),
        revertReason);
  }

  public TransactionReceipt(
      final TransactionType transactionType,
      final int status,
      final long cumulativeGasUsed,
      final List<Log> logs,
      final LogsBloomFilter bloomFilter,
      final Optional<Bytes> revertReason) {
    this(transactionType, null, status, cumulativeGasUsed, logs, bloomFilter, revertReason);
  }

  public TransactionReceipt(
      final TransactionType transactionType,
      final int status,
      final long cumulativeGasUsed,
      final List<Log> logs,
      final Optional<Bytes> maybeRevertReason) {
    this(
        transactionType,
        status,
        cumulativeGasUsed,
        logs,
        LogsBloomFilter.builder().insertLogs(logs).build(),
        maybeRevertReason);
  }

  /**
   * Creates an EIP-8141 frame transaction receipt. The receipt payload has no transaction-level
   * status; the status held here is derived (1 when every frame succeeded) for interfaces that need
   * a single value. The logs are the concatenation of the frame logs, in frame order.
   *
   * @param payer the account that paid the transaction fees
   * @param cumulativeGasUsed the total gas consumed in the block after this transaction
   * @param frameReceipts one receipt per frame, in frame order
   */
  public TransactionReceipt(
      final Address payer, final long cumulativeGasUsed, final List<FrameReceipt> frameReceipts) {
    this(
        TransactionType.FRAME,
        null,
        deriveFrameStatus(frameReceipts),
        cumulativeGasUsed,
        concatenateFrameLogs(frameReceipts),
        null,
        Optional.empty(),
        Optional.of(payer),
        Optional.of(frameReceipts));
  }

  private static int deriveFrameStatus(final List<FrameReceipt> frameReceipts) {
    return frameReceipts.stream().allMatch(fr -> fr.status() == FrameReceipt.STATUS_SUCCESS)
        ? 1
        : 0;
  }

  private static List<Log> concatenateFrameLogs(final List<FrameReceipt> frameReceipts) {
    return frameReceipts.stream().flatMap(fr -> fr.logs().stream()).toList();
  }

  /** Private constructor used by all public constructors. */
  private TransactionReceipt(
      final TransactionType transactionType,
      final Hash stateRoot,
      final int status,
      final long cumulativeGasUsed,
      final List<Log> logs,
      final LogsBloomFilter bloomFilter,
      final Optional<Bytes> revertReason) {
    this(
        transactionType,
        stateRoot,
        status,
        cumulativeGasUsed,
        logs,
        bloomFilter,
        revertReason,
        Optional.empty(),
        Optional.empty());
  }

  /** Private constructor used by all public constructors. */
  private TransactionReceipt(
      final TransactionType transactionType,
      final Hash stateRoot,
      final int status,
      final long cumulativeGasUsed,
      final List<Log> logs,
      final LogsBloomFilter bloomFilter,
      final Optional<Bytes> revertReason,
      final Optional<Address> maybePayer,
      final Optional<List<FrameReceipt>> maybeFrameReceipts) {
    this.transactionType = transactionType;
    this.stateRoot = stateRoot;
    this.cumulativeGasUsed = cumulativeGasUsed;
    this.status = status;
    this.logs = logs;
    this.bloomFilter =
        bloomFilter == null ? LogsBloomFilter.builder().insertLogs(logs).build() : bloomFilter;
    this.transactionReceiptType =
        stateRoot == null ? TransactionReceiptType.STATUS : TransactionReceiptType.ROOT;
    this.revertReason = revertReason;
    this.maybePayer = maybePayer;
    this.maybeFrameReceipts = maybeFrameReceipts;
  }

  /**
   * Returns the transaction type
   *
   * @return the transaction type
   */
  public TransactionType getTransactionType() {
    return transactionType;
  }

  /**
   * Returns the state root for a state root-encoded transaction receipt
   *
   * @return the state root if the transaction receipt is state root-encoded; otherwise {@code null}
   */
  public Hash getStateRoot() {
    return stateRoot;
  }

  /**
   * Returns the total amount of gas consumed in the block after the transaction has been processed.
   *
   * @return the total amount of gas consumed in the block after the transaction has been processed
   */
  @Override
  public long getCumulativeGasUsed() {
    return cumulativeGasUsed;
  }

  /**
   * Returns the logs generated by the transaction.
   *
   * @return the logs generated by the transaction
   */
  @Override
  public List<? extends org.hyperledger.besu.plugin.data.Log> getLogs() {
    return logs.stream().map(LogsWrapper::new).collect(Collectors.toList());
  }

  /**
   * Returns the logs generated by the transaction.
   *
   * @return the logs generated by the transaction
   */
  public List<Log> getLogsList() {
    return logs;
  }

  /**
   * Returns the logs bloom filter for the logs generated by the transaction
   *
   * @return the logs bloom filter for the logs generated by the transaction
   */
  @Override
  public LogsBloomFilter getBloomFilter() {
    return bloomFilter;
  }

  /**
   * Returns the status code for the status-encoded transaction receipt
   *
   * @return the status code if the transaction receipt is status-encoded; otherwise {@code -1}
   */
  @Override
  public int getStatus() {
    return status;
  }

  public TransactionReceiptType getTransactionReceiptType() {
    return transactionReceiptType;
  }

  @Override
  public Optional<Bytes> getRevertReason() {
    return revertReason;
  }

  /**
   * EIP-8141: the account that paid the transaction fees.
   *
   * @return the payer, present only for frame transaction receipts
   */
  public Optional<Address> getPayer() {
    return maybePayer;
  }

  /**
   * EIP-8141: the per-frame receipts.
   *
   * @return the frame receipts, present only for frame transaction receipts
   */
  public Optional<List<FrameReceipt>> getFrameReceipts() {
    return maybeFrameReceipts;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof TransactionReceipt other)) {
      return false;
    }
    return logs.equals(other.getLogsList())
        && Objects.equals(stateRoot, other.stateRoot)
        && cumulativeGasUsed == other.getCumulativeGasUsed()
        && status == other.status
        && maybePayer.map(Address::getBytes).equals(other.maybePayer.map(Address::getBytes))
        && maybeFrameReceipts.equals(other.maybeFrameReceipts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logs, stateRoot, cumulativeGasUsed);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("stateRoot", stateRoot)
        .add("cumulativeGasUsed", cumulativeGasUsed)
        .add("logs", logs)
        .add("bloomFilter", bloomFilter)
        .add("status", status)
        .add("transactionReceiptType", transactionReceiptType)
        .toString();
  }
}
