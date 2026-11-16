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
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

/**
 * Headers the backward sync has downloaded but not imported yet, persisted so the download survives
 * a restart. They are kept out of the blockchain storage: the engine API treats the presence of a
 * header there as "imported".
 *
 * <p>The headers are keyed by number; a hash index in the chain segment, which also holds the
 * metadata, finds them by hash.
 */
class PendingChain {
  private static final byte LAYOUT = 2;
  private static final byte[] LAYOUT_KEY = key("layout");
  private static final byte[] LOWEST_KEY = key("lowest");
  private static final byte[] HIGHEST_KEY = key("highest");
  private static final byte[] LINKED_TOP_KEY = key("linkedTop");

  private final KeyValueStorage headers;
  private final KeyValueStorage blocks;
  private final KeyValueStorage chain;
  private final BlockHeaderFunctions headerFunctions;

  static PendingChain create(
      final StorageProvider storageProvider, final BlockHeaderFunctions headerFunctions) {
    return new PendingChain(
        storageProvider.getStorageBySegmentIdentifier(
            KeyValueSegmentIdentifier.BACKWARD_SYNC_HEADERS),
        storageProvider.getStorageBySegmentIdentifier(
            KeyValueSegmentIdentifier.BACKWARD_SYNC_BLOCKS),
        storageProvider.getStorageBySegmentIdentifier(
            KeyValueSegmentIdentifier.BACKWARD_SYNC_CHAIN),
        headerFunctions);
  }

  @VisibleForTesting
  PendingChain(
      final KeyValueStorage headers,
      final KeyValueStorage blocks,
      final KeyValueStorage chain,
      final BlockHeaderFunctions headerFunctions) {
    this.headers = headers;
    this.blocks = blocks;
    this.chain = chain;
    this.headerFunctions = headerFunctions;
    final boolean currentLayout =
        chain.get(LAYOUT_KEY).filter(v -> v.length == 1 && v[0] == LAYOUT).isPresent();
    if (!currentLayout) {
      clear();
    }
  }

  synchronized Optional<BlockHeader> header(final long number) {
    return headers.get(numberKey(number)).map(this::decode);
  }

  synchronized Optional<BlockHeader> header(final Hash hash) {
    return chain
        .get(hash.getBytes().toArrayUnsafe())
        .flatMap(number -> header(decodeNumber(number)))
        .filter(header -> header.getHash().equals(hash));
  }

  synchronized OptionalLong linkedTop() {
    return metadata(LINKED_TOP_KEY);
  }

  synchronized void store(final Collection<BlockHeader> toStore) {
    if (toStore.isEmpty()) {
      return;
    }
    final Map<Long, BlockHeader> written = new HashMap<>();
    final KeyValueStorageTransaction headerTx = headers.startTransaction();
    final KeyValueStorageTransaction chainTx = chain.startTransaction();
    long lowest = metadata(LOWEST_KEY).orElse(Long.MAX_VALUE);
    long highest = metadata(HIGHEST_KEY).orElse(Long.MIN_VALUE);
    for (final BlockHeader header : toStore) {
      final long number = header.getNumber();
      final Optional<BlockHeader> replaced =
          Optional.ofNullable(written.get(number)).or(() -> header(number));
      replaced
          .filter(old -> !old.getHash().equals(header.getHash()))
          .ifPresent(old -> chainTx.remove(old.getHash().getBytes().toArrayUnsafe()));
      headerTx.put(numberKey(number), encode(header));
      chainTx.put(header.getHash().getBytes().toArrayUnsafe(), numberKey(number));
      written.put(number, header);
      lowest = Math.min(lowest, number);
      highest = Math.max(highest, number);
    }
    chainTx.put(LOWEST_KEY, numberKey(lowest));
    chainTx.put(HIGHEST_KEY, numberKey(highest));
    headerTx.commit();
    chainTx.commit();
  }

  synchronized void setLinkedTop(final long number) {
    final KeyValueStorageTransaction tx = chain.startTransaction();
    tx.put(LINKED_TOP_KEY, numberKey(number));
    tx.commit();
  }

  /** Deletes the headers above {@code number} and lowers the linked top to at most it. */
  synchronized void removeAbove(final long number) {
    final OptionalLong lowest = metadata(LOWEST_KEY);
    final OptionalLong highest = metadata(HIGHEST_KEY);
    final KeyValueStorageTransaction chainTx = chain.startTransaction();
    if (lowest.isPresent() && highest.isPresent() && highest.getAsLong() > number) {
      delete(Math.max(lowest.getAsLong(), number + 1), highest.getAsLong());
      if (number < lowest.getAsLong()) {
        chainTx.remove(LOWEST_KEY);
        chainTx.remove(HIGHEST_KEY);
      } else {
        chainTx.put(HIGHEST_KEY, numberKey(number));
      }
    }
    final OptionalLong linkedTop = linkedTop();
    if (linkedTop.isPresent() && linkedTop.getAsLong() > number) {
      chainTx.put(LINKED_TOP_KEY, numberKey(number));
    }
    chainTx.commit();
  }

  /** Deletes the headers from the lowest one up to and including {@code number}. */
  synchronized void removeUpTo(final long number) {
    final OptionalLong lowest = metadata(LOWEST_KEY);
    final OptionalLong highest = metadata(HIGHEST_KEY);
    if (lowest.isEmpty() || highest.isEmpty() || lowest.getAsLong() > number) {
      return;
    }
    delete(lowest.getAsLong(), Math.min(number, highest.getAsLong()));
    final KeyValueStorageTransaction chainTx = chain.startTransaction();
    if (number >= highest.getAsLong()) {
      chainTx.remove(LOWEST_KEY);
      chainTx.remove(HIGHEST_KEY);
    } else {
      chainTx.put(LOWEST_KEY, numberKey(number + 1));
    }
    chainTx.commit();
  }

  synchronized void clear() {
    headers.clear();
    blocks.clear();
    chain.clear();
    final KeyValueStorageTransaction tx = chain.startTransaction();
    tx.put(LAYOUT_KEY, new byte[] {LAYOUT});
    tx.commit();
  }

  synchronized boolean isEmpty() {
    return metadata(LOWEST_KEY).isEmpty();
  }

  private void delete(final long from, final long to) {
    final KeyValueStorageTransaction headerTx = headers.startTransaction();
    final KeyValueStorageTransaction chainTx = chain.startTransaction();
    for (long number = from; number <= to; number++) {
      final byte[] key = numberKey(number);
      headers
          .get(key)
          .map(this::decode)
          .ifPresent(header -> chainTx.remove(header.getHash().getBytes().toArrayUnsafe()));
      headerTx.remove(key);
    }
    headerTx.commit();
    chainTx.commit();
  }

  private OptionalLong metadata(final byte[] key) {
    return chain
        .get(key)
        .map(value -> OptionalLong.of(decodeNumber(value)))
        .orElseGet(OptionalLong::empty);
  }

  private BlockHeader decode(final byte[] bytes) {
    return BlockHeader.readFrom(RLP.input(Bytes.wrap(bytes)), headerFunctions);
  }

  private static byte[] encode(final BlockHeader header) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    header.writeTo(output);
    return output.encoded().toArrayUnsafe();
  }

  private static byte[] numberKey(final long number) {
    return Bytes.ofUnsignedLong(number).toArrayUnsafe();
  }

  private static long decodeNumber(final byte[] bytes) {
    return Bytes.wrap(bytes).toLong();
  }

  private static byte[] key(final String name) {
    return name.getBytes(StandardCharsets.UTF_8);
  }
}
