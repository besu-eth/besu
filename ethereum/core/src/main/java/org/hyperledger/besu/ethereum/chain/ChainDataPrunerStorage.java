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
package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class ChainDataPrunerStorage {
  private static final Bytes CHAIN_PRUNING_MARK_KEY =
      Bytes.wrap("pruningMark".getBytes(StandardCharsets.UTF_8));
  private static final Bytes BAL_PRUNING_MARK_KEY =
      Bytes.wrap("balPruningMark".getBytes(StandardCharsets.UTF_8));

  private static final Bytes VARIABLES_PREFIX = Bytes.of(1);
  private static final Bytes FORK_BLOCKS_PREFIX = Bytes.of(2);

  private final KeyValueStorage storage;

  public ChainDataPrunerStorage(final KeyValueStorage storage) {
    this.storage = storage;
  }

  public KeyValueStorageTransaction startTransaction() {
    return this.storage.startTransaction();
  }

  public Optional<Long> getChainPruningMark() {
    return get(VARIABLES_PREFIX, CHAIN_PRUNING_MARK_KEY)
        .map(UInt256::fromBytes)
        .map(UInt256::toLong);
  }

  public Optional<Long> getBalPruningMark() {
    return get(VARIABLES_PREFIX, BAL_PRUNING_MARK_KEY).map(UInt256::fromBytes).map(UInt256::toLong);
  }

  public Collection<Hash> getForkBlocks(final long blockNumber) {
    return get(FORK_BLOCKS_PREFIX, UInt256.valueOf(blockNumber))
        .map(bytes -> RLP.input(bytes).readList(in -> bytesToHash(in.readBytes32())))
        .orElse(Lists.newArrayList());
  }

  public synchronized void addForkBlock(final long blockNumber, final Hash blockHash) {
    final KeyValueStorageTransaction transaction = startTransaction();
    final Collection<Hash> forkBlocks = getForkBlocks(blockNumber);
    forkBlocks.add(blockHash);
    setForkBlocks(transaction, blockNumber, forkBlocks);
    transaction.commit();
  }

  public void setChainPruningMark(
      final KeyValueStorageTransaction transaction, final long pruningMark) {
    set(transaction, VARIABLES_PREFIX, CHAIN_PRUNING_MARK_KEY, UInt256.valueOf(pruningMark));
  }

  public void setBalPruningMark(
      final KeyValueStorageTransaction transaction, final long balPruningMark) {
    set(transaction, VARIABLES_PREFIX, BAL_PRUNING_MARK_KEY, UInt256.valueOf(balPruningMark));
  }

  private void setForkBlocks(
      final KeyValueStorageTransaction transaction,
      final long blockNumber,
      final Collection<Hash> forkBlocks) {
    set(
        transaction,
        FORK_BLOCKS_PREFIX,
        UInt256.valueOf(blockNumber),
        RLP.encode(o -> o.writeList(forkBlocks, (val, out) -> out.writeBytes(val.getBytes()))));
  }

  public void removeForkBlocks(
      final KeyValueStorageTransaction transaction, final long blockNumber) {
    remove(transaction, FORK_BLOCKS_PREFIX, UInt256.valueOf(blockNumber));
  }

  private Optional<Bytes> get(final Bytes prefix, final Bytes key) {
    return storage.get(Bytes.concatenate(prefix, key).toArrayUnsafe()).map(Bytes::wrap);
  }

  private void set(
      final KeyValueStorageTransaction transaction,
      final Bytes prefix,
      final Bytes key,
      final Bytes value) {
    transaction.put(Bytes.concatenate(prefix, key).toArrayUnsafe(), value.toArrayUnsafe());
  }

  private void remove(
      final KeyValueStorageTransaction transaction, final Bytes prefix, final Bytes key) {
    transaction.remove(Bytes.concatenate(prefix, key).toArrayUnsafe());
  }

  private Hash bytesToHash(final Bytes bytes) {
    return Hash.wrap(Bytes32.wrap(bytes, 0));
  }
}
