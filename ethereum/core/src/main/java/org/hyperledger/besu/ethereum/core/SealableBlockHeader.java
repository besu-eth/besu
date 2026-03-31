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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** A block header capable of being sealed. */
public class SealableBlockHeader extends ProcessableBlockHeader {
  protected final Hash ommersHash;

  protected final Hash stateRoot;

  protected final Hash transactionsRoot;

  protected final Hash receiptsRoot;

  protected final LogsBloomFilter logsBloom;

  protected final long gasUsed;

  protected final Bytes extraData;

  protected final Hash withdrawalsRoot;

  protected final Hash requestsHash;

  protected final Hash blockAccessListHash;

  protected final Long blobGasUsed;

  protected final BlobGas excessBlobGas;

  protected SealableBlockHeader(
      final Hash parentHash,
      final Hash ommersHash,
      final Address coinbase,
      final Hash stateRoot,
      final Hash transactionsRoot,
      final Hash receiptsRoot,
      final LogsBloomFilter logsBloom,
      final Difficulty difficulty,
      final long number,
      final long gasLimit,
      final long gasUsed,
      final long timestamp,
      final Bytes extraData,
      final Wei baseFee,
      final Bytes32 mixHashOrPrevRandao,
      final Hash withdrawalsRoot,
      final Long blobGasUsed,
      final BlobGas excessBlobGas,
      final Bytes32 parentBeaconBlockRoot,
      final Hash requestsHash,
      final Hash blockAccessListHash,
      final Long slotNumber) {
    super(
        parentHash,
        coinbase,
        difficulty,
        number,
        gasLimit,
        timestamp,
        baseFee,
        mixHashOrPrevRandao,
        parentBeaconBlockRoot,
        slotNumber);
    this.ommersHash = ommersHash;
    this.stateRoot = stateRoot;
    this.transactionsRoot = transactionsRoot;
    this.withdrawalsRoot = withdrawalsRoot;
    this.receiptsRoot = receiptsRoot;
    this.requestsHash = requestsHash;
    this.blockAccessListHash = blockAccessListHash;
    this.logsBloom = logsBloom;
    this.gasUsed = gasUsed;
    this.extraData = extraData;
    this.blobGasUsed = blobGasUsed;
    this.excessBlobGas = excessBlobGas;
  }

  /**
   * Returns the block ommers list hash.
   *
   * @return the block ommers list hash
   */
  public Hash getOmmersHash() {
    return ommersHash;
  }

  /**
   * Returns the block world state root hash.
   *
   * @return the block world state root hash
   */
  public Hash getStateRoot() {
    return stateRoot;
  }

  /**
   * Returns the block transaction root hash.
   *
   * @return the block transaction root hash
   */
  public Hash getTransactionsRoot() {
    return transactionsRoot;
  }

  /**
   * Returns the block transaction receipt root hash.
   *
   * @return the block transaction receipt root hash
   */
  public Hash getReceiptsRoot() {
    return receiptsRoot;
  }

  /**
   * Returns the block logs bloom filter.
   *
   * @return the block logs bloom filter
   */
  public LogsBloomFilter getLogsBloom() {
    return logsBloom;
  }

  /**
   * Returns the total gas consumed by the executing the block.
   *
   * @return the total gas consumed by the executing the block
   */
  public long getGasUsed() {
    return gasUsed;
  }

  /**
   * Returns the unparsed extra data field.
   *
   * @return the raw bytes of the extra data field
   */
  public Bytes getExtraData() {
    return extraData;
  }

  /**
   * Returns the block withdrawals root hash.
   *
   * @return the block withdrawals root hash
   */
  public Optional<Hash> getWithdrawalsRoot() {
    return Optional.ofNullable(withdrawalsRoot);
  }

  /**
   * Returns the block requests hash.
   *
   * @return the block requests hash
   */
  public Optional<Hash> getRequestsHash() {
    return Optional.ofNullable(requestsHash);
  }

  /**
   * Returns the bal hash.
   *
   * @return the bal hash
   */
  public Optional<Hash> getBalHash() {
    return Optional.ofNullable(blockAccessListHash);
  }

  /**
   * Returns the blob gas used if available.
   *
   * @return the blob gas used if available.
   */
  public Optional<Long> getBlobGasUsed() {
    return Optional.ofNullable(blobGasUsed);
  }

  /**
   * Returns the excess blob gas used if available.
   *
   * @return the excess blob gas used if available.
   */
  public Optional<BlobGas> getExcessBlobGas() {
    return Optional.ofNullable(excessBlobGas);
  }

  /**
   * Returns the execution witness if available.
   *
   * @return the execution witness if available.
   */
  public Optional<Object> getExecutionWitness() {
    return Optional.empty();
  }
}
