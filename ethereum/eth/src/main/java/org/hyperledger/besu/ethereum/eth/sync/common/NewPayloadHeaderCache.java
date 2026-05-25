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
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.consensus.merge.NewPayloadListener;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Bounded cache of the most recent block headers delivered by the consensus layer via {@code
 * engine_newPayload}. Stores up to {@link #MAX_SIZE} headers, indexed both by hash and by block
 * number. The oldest block number is evicted when the cache grows past capacity.
 *
 * <p>Headers stored here are block-hash-verified (the hash matches the payload contents) but have
 * not been validated against the local chain; callers must treat them as untrusted.
 */
public class NewPayloadHeaderCache implements NewPayloadListener {

  /** 4 epochs of slots on mainnet — the snap-serving window. */
  public static final int MAX_SIZE = 128;

  private final NavigableMap<Long, BlockHeader> byNumber = new TreeMap<>();
  private final Map<Hash, BlockHeader> byHash = new HashMap<>();

  @Override
  public void onNewPayload(final BlockHeader header) {
    put(header);
  }

  /**
   * Insert the header into the cache. If the same hash is already present this is a no-op; if a
   * different header at the same block number is present it is replaced (reorg case). Evicts the
   * oldest block number when the cache exceeds {@link #MAX_SIZE}.
   *
   * @param header the header to cache
   */
  public synchronized void put(final BlockHeader header) {
    if (byHash.containsKey(header.getHash())) {
      return;
    }
    final BlockHeader displaced = byNumber.put(header.getNumber(), header);
    if (displaced != null) {
      byHash.remove(displaced.getHash());
    }
    byHash.put(header.getHash(), header);

    if (byNumber.size() > MAX_SIZE) {
      final Map.Entry<Long, BlockHeader> oldest = byNumber.pollFirstEntry();
      if (oldest != null) {
        byHash.remove(oldest.getValue().getHash());
      }
    }
  }

  /**
   * Returns the cached header for the given hash.
   *
   * @param hash the block hash to look up
   * @return the cached header for that hash, or empty
   */
  public synchronized Optional<BlockHeader> getByHash(final Hash hash) {
    return Optional.ofNullable(byHash.get(hash));
  }

  /**
   * Returns the cached header at the given block number.
   *
   * @param number the block number to look up
   * @return the cached header at that block number, or empty
   */
  public synchronized Optional<BlockHeader> getByNumber(final long number) {
    return Optional.ofNullable(byNumber.get(number));
  }

  /**
   * Returns the cached header with the lowest block number.
   *
   * @return the oldest cached header, or empty if the cache is empty
   */
  public synchronized Optional<BlockHeader> oldest() {
    return byNumber.isEmpty() ? Optional.empty() : Optional.of(byNumber.firstEntry().getValue());
  }

  /**
   * Returns the cached header with the highest block number.
   *
   * @return the newest cached header, or empty if the cache is empty
   */
  public synchronized Optional<BlockHeader> newest() {
    return byNumber.isEmpty() ? Optional.empty() : Optional.of(byNumber.lastEntry().getValue());
  }

  /**
   * Returns the number of headers currently cached.
   *
   * @return the cache size
   */
  public synchronized int size() {
    return byNumber.size();
  }
}
