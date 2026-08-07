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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the Updater lifecycle drives the TrieNodeStrategy capture hooks: flush before the
 * composed transaction commits on commit paths that commit it; discard on paths that don't.
 */
class BonsaiWorldStateKeyValueStorageUpdaterCaptureTest {

  /** Records hook invocations; delegates trie-node ops to the plain bonsai strategy. */
  private static final class RecordingStrategy extends BonsaiTrieNodeStrategy {
    final List<String> events = new ArrayList<>();

    @Override
    public void flushCaptures(
        final SegmentedKeyValueStorage storage, final SegmentedKeyValueStorageTransaction tx) {
      events.add("flush");
    }

    @Override
    public void discardCaptures(final SegmentedKeyValueStorageTransaction tx) {
      events.add("discard");
    }
  }

  /** Composed tx wrapper that records its own commit so ordering vs. flush is observable. */
  private static final class RecordingTx implements SegmentedKeyValueStorageTransaction {
    final SegmentedKeyValueStorageTransaction delegate;
    final List<String> events;

    RecordingTx(final SegmentedKeyValueStorageTransaction delegate, final List<String> events) {
      this.delegate = delegate;
      this.events = events;
    }

    @Override
    public void put(
        final org.hyperledger.besu.plugin.services.storage.SegmentIdentifier segment,
        final byte[] key,
        final byte[] value) {
      delegate.put(segment, key, value);
    }

    @Override
    public void remove(
        final org.hyperledger.besu.plugin.services.storage.SegmentIdentifier segment,
        final byte[] key) {
      delegate.remove(segment, key);
    }

    @Override
    public void commit() {
      events.add("composed-commit");
      delegate.commit();
    }

    @Override
    public void rollback() {
      events.add("composed-rollback");
      delegate.rollback();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  private SegmentedKeyValueStorage worldStorage;
  private RecordingStrategy strategy;

  @BeforeEach
  void setUp() {
    worldStorage = new SegmentedInMemoryKeyValueStorage();
    strategy = new RecordingStrategy();
  }

  private BonsaiWorldStateKeyValueStorage.Updater updater() {
    final RecordingTx composedTx =
        new RecordingTx(worldStorage.startTransaction(), strategy.events);
    return new BonsaiWorldStateKeyValueStorage.Updater(
        composedTx,
        trieLogTx(),
        null, // flatDbStrategy unused by the lifecycle paths under test
        worldStorage,
        strategy);
  }

  /**
   * Any real, committable KeyValueStorageTransaction works here — the assertions only observe hook
   * ordering on the composed tx. Check how existing BonsaiWorldStateKeyValueStorage tests construct
   * their trieLogStorage transaction and copy that fixture; the adapter below is the usual pattern
   * (verify the constructor signature against services/kvstore).
   */
  private org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction trieLogTx() {
    return new org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter(
            org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                .TRIE_LOG_STORAGE,
            new SegmentedInMemoryKeyValueStorage())
        .startTransaction();
  }

  @Test
  void commitFlushesBeforeComposedCommit() {
    updater().commit();
    assertThat(strategy.events).containsExactly("flush", "composed-commit");
  }

  @Test
  void commitComposedOnlyFlushesBeforeComposedCommit() {
    updater().commitComposedOnly();
    assertThat(strategy.events).containsExactly("flush", "composed-commit");
  }

  @Test
  void commitTrieLogOnlyDiscardsAndNeverCommitsComposed() {
    updater().commitTrieLogOnly();
    assertThat(strategy.events).containsExactly("discard");
  }

  @Test
  void rollbackDiscardsBeforeComposedRollback() {
    updater().rollback();
    assertThat(strategy.events).containsExactly("discard", "composed-rollback");
  }
}
