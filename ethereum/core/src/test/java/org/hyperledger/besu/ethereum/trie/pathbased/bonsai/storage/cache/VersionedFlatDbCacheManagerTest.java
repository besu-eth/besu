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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class VersionedFlatDbCacheManagerTest {

  private VersionedFlatDbCacheManager cacheManager;

  /** Dedicated threads: the common pool may be too small to run two blocking commits at once. */
  private final ExecutorService committers = Executors.newCachedThreadPool();

  @BeforeEach
  void setUp() {
    cacheManager = new VersionedFlatDbCacheManager(100, 100, new NoOpMetricsSystem());
  }

  @AfterEach
  void tearDown() throws Exception {
    committers.shutdownNow();
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

  // --- read-path inserts racing a commit, a clear or a disable/enable ---

  @Test
  void readThatOverlapsCommitPublish_doesNotInsertItsValue() {
    final Bytes key = Bytes.of(4);
    final long readerVersion = cacheManager.getCurrentVersion();

    // The reader passed the bypass check, then a commit started while it was loading from storage
    // (storage not yet committed, version not yet bumped): the value it loaded may be stale and
    // must not be cached, otherwise it can survive the publish if the new entry is evicted.
    try {
      final Optional<Bytes> result =
          cacheManager.getFromCacheOrStorage(
              ACCOUNT_INFO_STATE,
              key,
              readerVersion,
              () -> {
                cacheManager.beginCommitCacheBypass();
                return Optional.of(Bytes.of(1));
              });
      assertThat(result).contains(Bytes.of(1));
    } finally {
      cacheManager.endCommitCacheBypass();
    }

    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isFalse();
  }

  @Test
  void batchReadThatOverlapsCommitPublish_doesNotInsertItsValues() {
    final Bytes key = Bytes.of(5);
    final long readerVersion = cacheManager.getCurrentVersion();

    try {
      final List<Optional<Bytes>> results =
          cacheManager.getMultipleFromCacheOrStorage(
              ACCOUNT_INFO_STATE,
              List.of(key),
              readerVersion,
              keys -> {
                cacheManager.beginCommitCacheBypass();
                return List.of(Optional.of(Bytes.of(1)));
              });
      assertThat(results).containsExactly(Optional.of(Bytes.of(1)));
    } finally {
      cacheManager.endCommitCacheBypass();
    }

    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isFalse();
  }

  @Test
  void readSpanningDisableCommitEnable_doesNotInsert() {
    final Bytes key = Bytes.of(6);
    final long readerVersion = cacheManager.getCurrentVersion();

    // the reader starts while enabled; while it loads, the cache is disabled, a commit lands
    // (nothing published) and the cache is enabled again before the reader tries to insert
    cacheManager.getFromCacheOrStorage(
        ACCOUNT_INFO_STATE,
        key,
        readerVersion,
        () -> {
          cacheManager.disable();
          cacheManager.commitAndPublish(() -> {}, v -> {});
          cacheManager.enable();
          return Optional.of(Bytes.of(1));
        });

    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isFalse();
  }

  // --- commitAndPublish ---

  @ParameterizedTest(name = "enabled={0}")
  @ValueSource(booleans = {true, false})
  void commitAndPublish_bypassesReadsAndPublishesAtNewVersion(final boolean enabled) {
    if (!enabled) {
      cacheManager.disable();
    }
    final Bytes key = Bytes.of(7);
    final long before = cacheManager.getCurrentVersion();
    final AtomicLong published = new AtomicLong(-1);

    cacheManager.commitAndPublish(
        () -> assertThat(cacheManager.isCommitCacheBypassActive()).isTrue(),
        version -> {
          assertThat(cacheManager.isCommitCacheBypassActive()).isTrue();
          published.set(version);
          cacheManager.putInCache(ACCOUNT_INFO_STATE, key, Bytes.of(1), version);
        });

    assertThat(cacheManager.isCommitCacheBypassActive()).isFalse();
    // the version advances even when disabled, so reads spanning an enable are still rejected
    assertThat(published.get()).isEqualTo(before + 1).isEqualTo(cacheManager.getCurrentVersion());
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isEqualTo(enabled);
  }

  @Test
  void commitAndPublish_failedStorageCommitPublishesNothingAndReleasesBypass() {
    final long before = cacheManager.getCurrentVersion();
    final AtomicBoolean published = new AtomicBoolean();

    assertThatThrownBy(
            () ->
                cacheManager.commitAndPublish(
                    () -> {
                      throw new IllegalStateException("storage commit failed");
                    },
                    v -> published.set(true)))
        .isInstanceOf(IllegalStateException.class);

    assertThat(published).isFalse();
    assertThat(cacheManager.getCurrentVersion()).isEqualTo(before);
    assertThat(cacheManager.isCommitCacheBypassActive()).isFalse();
  }

  @Test
  void commitAndPublish_serializesConcurrentCommitsWhenEnabled() throws Exception {
    final CountDownLatch releaseFirst = new CountDownLatch(1);
    final CompletableFuture<Void> first = holdCommitUntil(releaseFirst);
    try {
      final AtomicBoolean secondCommitted = new AtomicBoolean();
      final Thread second =
          new Thread(() -> cacheManager.commitAndPublish(() -> secondCommitted.set(true), v -> {}));
      second.start();

      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> second.getState() == Thread.State.BLOCKED);
      assertThat(secondCommitted).isFalse();

      releaseFirst.countDown();
      first.get(5, TimeUnit.SECONDS);
      second.join(TimeUnit.SECONDS.toMillis(5));
      assertThat(secondCommitted).isTrue();
    } finally {
      releaseFirst.countDown();
    }
  }

  @Test
  void commitAndPublish_doesNotSerializeCommitsWhenDisabled() throws Exception {
    cacheManager.disable();
    final CountDownLatch releaseFirst = new CountDownLatch(1);
    final CompletableFuture<Void> first = holdCommitUntil(releaseFirst);
    try {
      // completes while the first commit is still inside its storage commit
      CompletableFuture.runAsync(() -> cacheManager.commitAndPublish(() -> {}, v -> {}), committers)
          .get(5, TimeUnit.SECONDS);
      assertThat(first).isNotDone();
    } finally {
      releaseFirst.countDown();
    }
    first.get(5, TimeUnit.SECONDS);
  }

  /** Starts a commit whose storage commit blocks until {@code release}; returns once inside it. */
  private CompletableFuture<Void> holdCommitUntil(final CountDownLatch release)
      throws InterruptedException {
    final CountDownLatch inStorageCommit = new CountDownLatch(1);
    final CompletableFuture<Void> commit =
        CompletableFuture.runAsync(
            () ->
                cacheManager.commitAndPublish(
                    () -> {
                      inStorageCommit.countDown();
                      try {
                        release.await();
                      } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                    },
                    v -> {}),
            committers);
    assertThat(inStorageCommit.await(5, TimeUnit.SECONDS)).isTrue();
    return commit;
  }

  // --- invalidateAll / enable / disable ---

  @Test
  void invalidateAll_advancesVersionAndDropsEntries() {
    final Bytes key = Bytes.of(8);
    cacheManager.putInCache(ACCOUNT_INFO_STATE, key, Bytes.of(1), cacheManager.getCurrentVersion());
    final long before = cacheManager.getCurrentVersion();
    final AtomicLong newVersion = new AtomicLong(-1);

    cacheManager.invalidateAll(newVersion::set);

    assertThat(newVersion.get()).isEqualTo(before + 1).isEqualTo(cacheManager.getCurrentVersion());
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isFalse();
  }

  @Test
  void disabled_readsGoToStorageAndNothingIsCached() {
    final Bytes key = Bytes.of(9);
    final Bytes removedKey = Bytes.of(10);
    cacheManager.putInCache(ACCOUNT_INFO_STATE, key, Bytes.of(1), cacheManager.getCurrentVersion());

    cacheManager.disable();

    assertThat(cacheManager.isEnabled()).isFalse();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isFalse();
    final long version = cacheManager.getCurrentVersion();
    assertThat(
            cacheManager.getFromCacheOrStorage(
                ACCOUNT_INFO_STATE, key, version, () -> Optional.of(Bytes.of(2))))
        .contains(Bytes.of(2));
    assertThat(
            cacheManager.getMultipleFromCacheOrStorage(
                ACCOUNT_INFO_STATE,
                List.of(key),
                version,
                keys -> List.of(Optional.of(Bytes.of(3)))))
        .containsExactly(Optional.of(Bytes.of(3)));
    cacheManager.putInCache(ACCOUNT_INFO_STATE, key, Bytes.of(4), version);
    cacheManager.removeFromCache(ACCOUNT_INFO_STATE, removedKey, version);

    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isFalse();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, removedKey)).isFalse();
  }

  @Test
  void enable_restoresCaching() {
    final Bytes key = Bytes.of(11);
    cacheManager.disable();
    cacheManager.enable();

    assertThat(cacheManager.isEnabled()).isTrue();
    cacheManager.getFromCacheOrStorage(
        ACCOUNT_INFO_STATE, key, cacheManager.getCurrentVersion(), () -> Optional.of(Bytes.of(1)));
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isTrue();
  }
}
