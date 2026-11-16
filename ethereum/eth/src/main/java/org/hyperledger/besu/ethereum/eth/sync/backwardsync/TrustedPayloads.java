/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Blocks (and their block access lists) the consensus client delivered with newPayload, kept in
 * memory so they are not downloaded again. Bounded by size; the lowest block numbers are evicted
 * first.
 */
class TrustedPayloads {
  private record Entry(Block block, Optional<BlockAccessList> blockAccessList, long size) {}

  private final long maxBytes;
  private final Map<Hash, Entry> entries = new HashMap<>();
  private long bytes;

  TrustedPayloads(final long maxBytes) {
    this.maxBytes = maxBytes;
  }

  synchronized void add(final Block block, final Optional<BlockAccessList> blockAccessList) {
    final Entry existing = entries.get(block.getHash());
    if (existing != null) {
      if (existing.blockAccessList().isPresent() || blockAccessList.isEmpty()) {
        return;
      }
      remove(existing);
    }
    final Entry entry = new Entry(block, blockAccessList, size(block, blockAccessList));
    entries.put(block.getHash(), entry);
    bytes += entry.size();
    while (bytes > maxBytes && !entries.isEmpty()) {
      remove(
          entries.values().stream()
              .min(Comparator.comparingLong(e -> e.block().getHeader().getNumber()))
              .orElseThrow());
    }
  }

  synchronized Optional<Block> block(final Hash hash) {
    return Optional.ofNullable(entries.get(hash)).map(Entry::block);
  }

  synchronized Optional<BlockAccessList> blockAccessList(final Hash hash) {
    return Optional.ofNullable(entries.get(hash)).flatMap(Entry::blockAccessList);
  }

  synchronized void removeUpTo(final long number) {
    entries.values().removeIf(e -> e.block().getHeader().getNumber() <= number);
    bytes = entries.values().stream().mapToLong(Entry::size).sum();
  }

  synchronized void clear() {
    entries.clear();
    bytes = 0;
  }

  private void remove(final Entry entry) {
    entries.remove(entry.block().getHash());
    bytes -= entry.size();
  }

  private static long size(final Block block, final Optional<BlockAccessList> blockAccessList) {
    return (long) block.getSize()
        + blockAccessList.flatMap(BlockAccessList::rawRlp).map(Bytes::size).orElse(0);
  }
}
