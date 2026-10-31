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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.StorageFailures;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A trie writer that fails the first commit holding a node at some location, as RocksDB fails a
 * commit it could not check, which a retry may commit.
 */
final class FailingTrieWriter implements SyncStateStorage.TrieWriter {

  private final SyncStateStorage.TrieWriter writer;
  private final Predicate<Bytes> nodeLocation;
  private final AtomicBoolean failed;
  private boolean holdsFailingNode;

  private FailingTrieWriter(
      final SyncStateStorage.TrieWriter writer,
      final Predicate<Bytes> nodeLocation,
      final AtomicBoolean failed) {
    this.writer = writer;
    this.nodeLocation = nodeLocation;
    this.failed = failed;
  }

  /** Returns a spy of the storage whose trie writers fail that commit, setting {@code failed}. */
  static SyncStateStorage failingOnce(
      final SyncStateStorage storage,
      final Predicate<Bytes> nodeLocation,
      final AtomicBoolean failed) {
    final SyncStateStorage failing = spy(storage);
    doAnswer(
            invocation ->
                new FailingTrieWriter(
                    (SyncStateStorage.TrieWriter) invocation.callRealMethod(),
                    nodeLocation,
                    failed))
        .when(failing)
        .trieWriter();
    return failing;
  }

  @Override
  public void accountNode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
    writer.accountNode(location, hash, rlp);
  }

  @Override
  public void storageNode(
      final Bytes32 accountHash, final Bytes location, final Bytes32 hash, final Bytes rlp) {
    holdsFailingNode |= nodeLocation.test(location);
    writer.storageNode(accountHash, location, hash, rlp);
  }

  @Override
  public void commit() {
    if (holdsFailingNode && failed.compareAndSet(false, true)) {
      writer.abort();
      throw StorageFailures.notCommitted();
    }
    holdsFailingNode = false;
    writer.commit();
  }

  @Override
  public void abort() {
    writer.abort();
  }
}
