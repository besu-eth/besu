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
package org.hyperledger.besu.plugin.services.trielogs;

import org.hyperledger.besu.datatypes.Hash;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** Trielog provider interface for a given block hash. */
public interface TrieLogProvider {
  /**
   * Saves the TrieLog layer for the given block hash.
   *
   * @param blockHash the block hash
   * @param blockNumber the block number
   * @param trieLog the associated TrieLog layer
   */
  void saveRawTrieLogLayer(final Hash blockHash, final long blockNumber, final Bytes trieLog);

  /**
   * Returns the TrieLog layer for the given block hash.
   *
   * @param blockHash the block hash
   * @return the TrieLog layer for the given block hash
   * @param <T> the type of the TrieLog
   */
  <T extends TrieLog.LogTuple<?>> Optional<TrieLog> getTrieLogLayer(final Hash blockHash);

  /**
   * Get the raw TrieLog layer for the given block hash.
   *
   * @param blockHash the block hash
   * @return the raw TrieLog layer bytes for the given block hash
   */
  Optional<Bytes> getRawTrieLogLayer(final Hash blockHash);

  /**
   * Returns the TrieLog layer for the given block number.
   *
   * @param blockNumber the block number
   * @return the TrieLog layer for the given block number
   * @param <T> the type of the TrieLog
   */
  <T extends TrieLog.LogTuple<?>> Optional<TrieLog> getTrieLogLayer(final long blockNumber);

  /**
   * Get the raw TrieLog layer for the given block number.
   *
   * @param blockNumber the block number
   * @return the raw TrieLog layer bytes for the given block number
   */
  Optional<Bytes> getRawTrieLogLayer(final long blockNumber);

  /**
   * Returns the TrieLog layers for the given block number range.
   *
   * @param fromBlockNumber the from block number
   * @param toBlockNumber the to block number
   * @return the TrieLog layers for the given block number range
   * @param <T> the type of the TrieLog
   */
  <T extends TrieLog.LogTuple<?>> List<TrieLogRangeTuple> getTrieLogsByRange(
      long fromBlockNumber, long toBlockNumber);

  /**
   * Block and TrieLog layer composition, used for returning a range of TrieLog layers.
   *
   * @param blockHash the block hash
   * @param blockNumber the block number
   * @param trieLog the associated TrieLog layer
   */
  record TrieLogRangeTuple(Hash blockHash, long blockNumber, TrieLog trieLog) {}
}
