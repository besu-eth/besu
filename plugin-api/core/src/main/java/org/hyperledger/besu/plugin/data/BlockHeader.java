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
package org.hyperledger.besu.plugin.data;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Quantity;
import org.hyperledger.besu.plugin.Unstable;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * The minimum set of data for a BlockHeader, as defined in the <a href=
 * "https://ethereum.github.io/yellowpaper/paper.pdf">Ethereum Yellow Paper</a>.
 */
public interface BlockHeader extends ProcessableBlockHeader {

  /**
   * The Keccak 256-bit hash of the ommers list portion of this block.
   *
   * @return The Keccak 256-bit hash of the ommers list portion of this block.
   */
  Hash getOmmersHash();

  /**
   * The Keccak 256-bit hash of the root node of the state trie, after all transactions are executed
   * and finalizations applied.
   *
   * @return The Keccak 256-bit hash of the root node of the state trie, after all transactions are
   *     executed and finalizations applied.
   */
  Hash getStateRoot();

  /**
   * The Keccak 256-bit hash of the root node of the trie structure populated with each transaction
   * in the transactions list portion of the block.
   *
   * @return The Keccak 256-bit hash of the root node of the trie structure populated with each
   *     transaction in the transactions list portion of the block.
   */
  Hash getTransactionsRoot();

  /**
   * The Keccak 256-bit hash of the root node of the trie structure populated with the receipts of
   * each transaction in the transactions list portion of the block.
   *
   * @return The Keccak 256-bit hash of the root node of the trie structure populated with the
   *     receipts of each transaction in the transactions list portion of the block.
   */
  Hash getReceiptsRoot();

  /**
   * The Bloom filter composed from indexable information (logger address and log topics) contained
   * in each log entry from the receipt of each transaction in the transactions list.
   *
   * @return The Bloom filter composed from indexable information (logger address and log topics)
   *     contained in each log entry from the receipt of each transaction in the transactions list.
   */
  LogsBloomFilter getLogsBloom();

  /**
   * A scalar value equal to the total gas used in transactions in this block.
   *
   * @return A scalar value equal to the total gas used in transactions in this block.
   */
  long getGasUsed();

  /**
   * An arbitrary byte array containing data relevant to this block. This must be 32 bytes or fewer.
   *
   * @return An arbitrary byte array containing data relevant to this block. This must be 32 bytes
   *     or fewer.
   */
  Bytes getExtraData();

  /**
   * A 256-bit hash which, combined with the nonce, proves that a sufficient amount of computation
   * has been carried out on this block.
   *
   * @return A 256-bit hash which, combined with the nonce, proves that a sufficient amount of
   *     computation has been carried out on this block.
   */
  Hash getMixHash();

  /**
   * A 64-bit value which, combined with the mixhash, proves that a sufficient amount of computation
   * has been carried out on this block.
   *
   * @return A 64-bit value which, combined with the mixhash, proves that a sufficient amount of
   *     computation has been carried out on this block.
   */
  long getNonce();

  /**
   * The Keccak 256-bit hash of this header.
   *
   * @return The Keccak 256-bit hash of this header.
   */
  Hash getBlockHash();

  /**
   * The Keccak 256-bit hash of the root node of the trie structure populated with each withdrawal
   * in the withdrawals list portion of the block.
   *
   * @return The Keccak 256-bit hash of the root node of the trie structure populated with each
   *     withdrawal in the withdrawal list portion of the block.
   */
  Optional<? extends Hash> getWithdrawalsRoot();

  /**
   * The Keccak 256-bit hash of the root node of the trie structure populated with each request in
   * the request list portion of the block.
   *
   * @return The Keccak 256-bit hash of the root node of the trie structure populated with each
   *     request in the request list portion of the block.
   */
  @Unstable
  Optional<? extends Hash> getRequestsHash();

  /**
   * The balHash of this header.
   *
   * @return The balHash of this header.
   */
  @Unstable
  Optional<? extends Hash> getBalHash();

  /**
   * The excess_blob_gas of this header.
   *
   * @return The excess_blob_gas of this header.
   */
  Optional<? extends Quantity> getExcessBlobGas();

  /**
   * The blob_gas_used of this header.
   *
   * @return The blob_gas_used of this header.
   */
  Optional<? extends Long> getBlobGasUsed();

  /**
   * Returns a human-readable string for logging.
   *
   * @return the log string.
   */
  default String toLogString() {
    return getNumber() + " (" + getBlockHash() + ")";
  }
}
