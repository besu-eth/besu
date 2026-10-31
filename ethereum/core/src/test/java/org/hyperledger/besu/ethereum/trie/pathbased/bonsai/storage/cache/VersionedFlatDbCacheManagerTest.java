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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager.TrieNode;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VersionedFlatDbCacheManagerTest {

  private VersionedFlatDbCacheManager cacheManager;

  @BeforeEach
  void setUp() {
    cacheManager = new VersionedFlatDbCacheManager(100, 100, new NoOpMetricsSystem());
  }

  @AfterEach
  void tearDown() throws Exception {
    cacheManager.close();
  }

  @Test
  void emptyFetcherResult_leavesMissesUnresolvedAndDoesNotCache() {
    final Bytes key = Bytes.of(1);
    final List<Optional<Bytes>> results =
        cacheManager.getMultipleFromCacheOrStorage(
            ACCOUNT_INFO_STATE, List.of(key), 0L, keys -> List.of());

    assertThat(results).hasSize(1);
    assertThat(results.get(0)).isNull();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isFalse();
  }

  @Test
  void sizeMismatchedFetcherResult_leavesMissesUnresolvedAndDoesNotCache() {
    final Bytes keyA = Bytes.of(1);
    final Bytes keyB = Bytes.of(2);
    final List<Optional<Bytes>> results =
        cacheManager.getMultipleFromCacheOrStorage(
            ACCOUNT_INFO_STATE, List.of(keyA, keyB), 0L, keys -> List.of(Optional.of(Bytes.of(9))));

    assertThat(results).containsExactly(null, null);
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyA)).isFalse();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyB)).isFalse();
  }

  @Test
  void nullFetcherSlots_areSkippedAndNotCached() {
    final Bytes keyA = Bytes.of(1);
    final Bytes keyB = Bytes.of(2);
    final Bytes keyC = Bytes.of(3);
    final Bytes valueC = Bytes.of(30);

    final List<Optional<Bytes>> results =
        cacheManager.getMultipleFromCacheOrStorage(
            ACCOUNT_INFO_STATE,
            List.of(keyA, keyB, keyC),
            0L,
            keys -> {
              final List<Optional<Bytes>> fetched = new ArrayList<>(keys.size());
              fetched.add(null);
              fetched.add(Optional.empty());
              fetched.add(Optional.of(valueC));
              return fetched;
            });

    assertThat(results.get(0)).isNull();
    assertThat(results.get(1)).isEmpty();
    assertThat(results.get(2)).contains(valueC);

    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyA)).isFalse();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyB)).isTrue();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyC)).isTrue();
    assertThat(cacheManager.getCachedValue(ACCOUNT_INFO_STATE, keyB))
        .hasValueSatisfying(cv -> assertThat(cv.isRemoval()).isTrue());
    assertThat(cacheManager.getCachedValue(ACCOUNT_INFO_STATE, keyC))
        .hasValueSatisfying(cv -> assertThat(cv.getValue()).isEqualTo(valueC));
  }

  @Test
  void trieNodes_areServedByHashUntilCleared() {
    final Bytes accountNode = Bytes.fromHexString("0xc58320aaaa01");
    final Bytes storageNode = Bytes.fromHexString("0xc58320bbbb02");
    final Bytes32 accountNodeHash = Bytes32.wrap(Hash.hash(accountNode).getBytes());
    final Bytes32 storageNodeHash = Bytes32.wrap(Hash.hash(storageNode).getBytes());

    cacheManager.putTrieNodes(
        List.of(
            new TrieNode(accountNodeHash, accountNode),
            new TrieNode(storageNodeHash, storageNode)));
    await()
        .untilAsserted(
            () -> assertThat(cacheManager.getCacheSize(TRIE_BRANCH_STORAGE)).isEqualTo(2));

    assertThat(cacheManager.getTrieNode(accountNodeHash)).contains(accountNode);
    assertThat(cacheManager.getTrieNode(storageNodeHash)).contains(storageNode);

    cacheManager.clear(TRIE_BRANCH_STORAGE);

    assertThat(cacheManager.getCacheSize(TRIE_BRANCH_STORAGE)).isZero();
    assertThat(cacheManager.getTrieNode(accountNodeHash)).isEmpty();
    assertThat(cacheManager.getTrieNode(storageNodeHash)).isEmpty();
  }

  @Test
  void trieNodes_batchQueuedBeforeClearIsDropped() throws Exception {
    final Bytes node = Bytes.fromHexString("0xc58320aaaa01");
    final Bytes32 nodeHash = Bytes32.wrap(Hash.hash(node).getBytes());
    final Bytes queuedNode = Bytes.fromHexString("0xc58320bbbb02");
    final Bytes32 queuedNodeHash = Bytes32.wrap(Hash.hash(queuedNode).getBytes());
    final Bytes laterNode = Bytes.fromHexString("0xc58320cccc03");
    final Bytes32 laterNodeHash = Bytes32.wrap(Hash.hash(laterNode).getBytes());

    // keeps the maintenance worker busy until released, so the next batch stays queued
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final List<TrieNode> blockingBatch =
        new AbstractList<>() {
          @Override
          public TrieNode get(final int index) {
            return new TrieNode(nodeHash, node);
          }

          @Override
          public int size() {
            return 1;
          }

          @Override
          public void forEach(final Consumer<? super TrieNode> action) {
            started.countDown();
            try {
              release.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            super.forEach(action);
          }
        };
    cacheManager.putTrieNodes(blockingBatch);
    started.await();
    cacheManager.putTrieNodes(List.of(new TrieNode(queuedNodeHash, queuedNode)));

    final Thread clear = new Thread(() -> cacheManager.clear(TRIE_BRANCH_STORAGE));
    clear.start();
    await().until(() -> clear.getState() == Thread.State.BLOCKED || !clear.isAlive());
    release.countDown();
    clear.join();

    cacheManager.putTrieNodes(List.of(new TrieNode(laterNodeHash, laterNode)));
    await().until(() -> cacheManager.getTrieNode(laterNodeHash).isPresent());
    assertThat(cacheManager.getTrieNode(nodeHash)).isEmpty();
    assertThat(cacheManager.getTrieNode(queuedNodeHash)).isEmpty();
  }

  @Test
  void trieNodes_putAfterCloseIsDropped() {
    final Bytes node = Bytes.fromHexString("0xc58320aaaa01");
    final Bytes32 nodeHash = Bytes32.wrap(Hash.hash(node).getBytes());
    cacheManager.close();

    cacheManager.putTrieNodes(List.of(new TrieNode(nodeHash, node)));

    assertThat(cacheManager.getTrieNode(nodeHash)).isEmpty();
  }
}
