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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import org.apache.tuweni.bytes.Bytes;

/**
 * The headers of a sync by block number, on disk until their blocks are imported. They stay out of
 * the blockchain, which would otherwise report the head as known before its block is imported.
 */
class HeaderStore {

  private final KeyValueStorage storage;
  private final BlockHeaderFunctions blockHeaderFunctions;

  HeaderStore(final KeyValueStorage storage, final BlockHeaderFunctions blockHeaderFunctions) {
    this.storage = storage;
    this.blockHeaderFunctions = blockHeaderFunctions;
  }

  void putAll(final List<BlockHeader> headers) {
    final KeyValueStorageTransaction transaction = storage.startTransaction();
    headers.forEach(
        header -> transaction.put(key(header.getNumber()), RLP.encode(header::writeTo).toArray()));
    transaction.commit();
  }

  Optional<BlockHeader> get(final long number) {
    return storage
        .get(key(number))
        .map(rlp -> BlockHeader.readFrom(RLP.input(Bytes.wrap(rlp)), blockHeaderFunctions));
  }

  /** Returns the headers of blocks {@code first} to {@code last}, all expected to be stored. */
  List<BlockHeader> getRange(final long first, final long last) {
    return LongStream.rangeClosed(first, last).mapToObj(this::getStored).toList();
  }

  private BlockHeader getStored(final long number) {
    return get(number).orElseThrow(() -> new IllegalStateException("No header #" + number));
  }

  void clear() {
    storage.clear();
  }

  private static byte[] key(final long number) {
    return Bytes.ofUnsignedLong(number).toArrayUnsafe();
  }
}
