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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiArchiveReadFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiArchiveWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFullFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiPartialFlatDbStrategy;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Matrix coverage for {@code getMultipleFlat} across storage shapes, flat-db modes, key sets, open
 * vs closed, and versioned-cache warming / no-poison behaviour.
 *
 * <pre>
 * Result contract:
 *   Optional.of(v)     = present value
 *   Optional.empty()   = key absent (full mode after a real fetch)
 *   null slot          = unresolved miss (strategy no-op; cache leaves index null)
 *   List.of()          = whole-op no-op (closed storage / archive layer)
 * </pre>
 *
 * <p>Builder API:
 *
 * <pre>
 * Fixture.builder()
 *   .mode(FULL|PARTIAL|ARCHIVE)
 *   .shape(HEAD|SNAPSHOT|LAYER|ARCHIVE_LAYER)
 *   .keys(ALL_PRESENT|ALL_ABSENT|MIX|CACHE_HIT_MIX)
 *   .closeSubject()          // optional
 *   .poisonSharedCache()     // archive-layer: put present into HEAD cache before call
 *   .build()
 * </pre>
 */
class BonsaiGetMultipleFlatMatrixTest {

  private Fixture fixture;

  @AfterEach
  void tearDown() throws Exception {
    if (fixture != null) {
      fixture.close();
      fixture = null;
    }
  }

  // --- Matrix: empty cache, open subject ---

  static Stream<Arguments> openEmptyCacheCases() {
    return Stream.of(
        // FULL: real fetch + warm (including absences as removals)
        caseOf("FULL/HEAD/MIX", Mode.FULL, Shape.HEAD, KeySet.MIX),
        caseOf("FULL/HEAD/ALL_PRESENT", Mode.FULL, Shape.HEAD, KeySet.ALL_PRESENT),
        caseOf("FULL/HEAD/ALL_ABSENT", Mode.FULL, Shape.HEAD, KeySet.ALL_ABSENT),
        caseOf("FULL/SNAPSHOT/MIX", Mode.FULL, Shape.SNAPSHOT, KeySet.MIX),
        caseOf("FULL/LAYER/MIX", Mode.FULL, Shape.LAYER, KeySet.MIX),
        // PARTIAL / ARCHIVE strategy: List.of() → null slots, no poison
        caseOf("PARTIAL/HEAD/MIX", Mode.PARTIAL, Shape.HEAD, KeySet.MIX),
        caseOf("PARTIAL/HEAD/ALL_PRESENT", Mode.PARTIAL, Shape.HEAD, KeySet.ALL_PRESENT),
        caseOf("PARTIAL/HEAD/ALL_ABSENT", Mode.PARTIAL, Shape.HEAD, KeySet.ALL_ABSENT),
        // Layer over a strategy no-op parent: null slots must survive the layer merge
        caseOf("PARTIAL/LAYER/MIX", Mode.PARTIAL, Shape.LAYER, KeySet.MIX),
        caseOf("PARTIAL/SNAPSHOT/MIX", Mode.PARTIAL, Shape.SNAPSHOT, KeySet.MIX),
        caseOf("ARCHIVE/HEAD/MIX", Mode.ARCHIVE, Shape.HEAD, KeySet.MIX),
        caseOf("ARCHIVE/HEAD/ALL_PRESENT", Mode.ARCHIVE, Shape.HEAD, KeySet.ALL_PRESENT),
        // Archive layer: whole-op List.of(), even if shared HEAD cache is poisoned
        caseOf("ARCHIVE/ARCHIVE_LAYER/MIX", Mode.ARCHIVE, Shape.ARCHIVE_LAYER, KeySet.MIX));
  }

  static Stream<Arguments> openCacheHitMixCases() {
    return Stream.of(
        caseOf("FULL/HEAD/CACHE_HIT_MIX", Mode.FULL, Shape.HEAD, KeySet.CACHE_HIT_MIX),
        caseOf("PARTIAL/HEAD/CACHE_HIT_MIX", Mode.PARTIAL, Shape.HEAD, KeySet.CACHE_HIT_MIX),
        caseOf("ARCHIVE/HEAD/CACHE_HIT_MIX", Mode.ARCHIVE, Shape.HEAD, KeySet.CACHE_HIT_MIX));
  }

  static Stream<Arguments> closedCases() {
    return Stream.of(
        caseOf("FULL/SNAPSHOT closed", Mode.FULL, Shape.SNAPSHOT, KeySet.MIX, true, false),
        caseOf("FULL/LAYER closed", Mode.FULL, Shape.LAYER, KeySet.MIX, true, false),
        caseOf(
            "ARCHIVE/ARCHIVE_LAYER closed",
            Mode.ARCHIVE,
            Shape.ARCHIVE_LAYER,
            KeySet.MIX,
            true,
            false));
  }

  private static Arguments caseOf(
      final String name, final Mode mode, final Shape shape, final KeySet keys) {
    return caseOf(name, mode, shape, keys, false, shape == Shape.ARCHIVE_LAYER);
  }

  private static Arguments caseOf(
      final String name,
      final Mode mode,
      final Shape shape,
      final KeySet keys,
      final boolean closeSubject,
      final boolean poisonSharedCache) {
    return Arguments.of(
        Named.of(
            name,
            Fixture.builder()
                .mode(mode)
                .shape(shape)
                .keys(keys)
                .closeSubject(closeSubject)
                .poisonSharedCache(poisonSharedCache)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("openEmptyCacheCases")
  void open_emptyCache(final Fixture.Builder builder) throws Exception {
    fixture = builder.build();
    final List<Optional<Bytes>> values =
        fixture.subject.getMultipleFlat(ACCOUNT_INFO_STATE, fixture.queryKeys);

    if (fixture.shape == Shape.ARCHIVE_LAYER) {
      assertWholeOpEmpty(values);
      return;
    }

    if (fixture.mode == Mode.FULL) {
      assertFullFetch(values);
    } else {
      assertStrategyNoOpNulls(values);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("openCacheHitMixCases")
  void open_cacheHitPlusMiss(final Fixture.Builder builder) throws Exception {
    fixture = builder.build();
    final List<Optional<Bytes>> values =
        fixture.subject.getMultipleFlat(ACCOUNT_INFO_STATE, fixture.queryKeys);

    if (fixture.mode == Mode.FULL) {
      assertThat(values)
          .containsExactly(Optional.of(Fixture.MISS_VALUE), Optional.of(Fixture.HIT_VALUE));
      assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, Fixture.HIT.getBytes())).isTrue();
      assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, Fixture.MISS.getBytes())).isTrue();
    } else {
      assertThat(values).hasSize(2);
      assertThat(values.get(0)).isNull();
      assertThat(values.get(1)).contains(Fixture.HIT_VALUE);
      assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, Fixture.HIT.getBytes())).isTrue();
      assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, Fixture.MISS.getBytes())).isFalse();
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("closedCases")
  void closed_returnsEmptyList(final Fixture.Builder builder) throws Exception {
    fixture = builder.build();
    final List<Optional<Bytes>> values =
        fixture.subject.getMultipleFlat(ACCOUNT_INFO_STATE, fixture.queryKeys);
    assertWholeOpEmpty(values);
  }

  // --- Focused named cases (not worth expanding the matrix) ---

  @Test
  void fullHead_preservesOrderIncludingDuplicateKeys() throws Exception {
    fixture = Fixture.builder().mode(Mode.FULL).shape(Shape.HEAD).keys(KeySet.MIX).build();
    final byte[] present = Fixture.A.getBytes().toArray();
    final byte[] absent = Fixture.B.getBytes().toArray();

    final List<Optional<Bytes>> values =
        fixture.subject.getMultipleFlat(ACCOUNT_INFO_STATE, List.of(present, absent, present));

    assertThat(values).hasSize(3);
    assertThat(values.get(0)).contains(Fixture.A_VALUE);
    assertThat(values.get(1)).isEmpty();
    assertThat(values.get(2)).contains(Fixture.A_VALUE);
  }

  @Test
  void pinnedSnapshot_doesNotWarmWhenVersionBehindGlobal() throws Exception {
    fixture = Fixture.builder().mode(Mode.FULL).shape(Shape.HEAD).keys(KeySet.ALL_PRESENT).build();
    try (final BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(fixture.head)) {
      commitAccount(fixture.head, Hash.hash(Bytes.of(99)), Bytes.of(99));
      fixture.head.getCacheManager().clear(ACCOUNT_INFO_STATE);

      assertThat(snapshot.getCurrentVersion())
          .isLessThan(fixture.head.getCacheManager().getCurrentVersion());

      final List<Optional<Bytes>> values =
          snapshot.getMultipleFlat(ACCOUNT_INFO_STATE, List.of(Fixture.A.getBytes().toArray()));
      assertThat(values).containsExactly(Optional.of(Fixture.A_VALUE));
      assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, Fixture.A.getBytes())).isFalse();
    }
  }

  @Test
  void noOpCache_returnsStorageWithoutWarming() throws Exception {
    fixture =
        Fixture.builder()
            .mode(Mode.FULL)
            .shape(Shape.HEAD)
            .keys(KeySet.ALL_PRESENT)
            .crossBlockCache(false)
            .build();
    final List<Optional<Bytes>> values =
        fixture.subject.getMultipleFlat(
            ACCOUNT_INFO_STATE, List.of(Fixture.A.getBytes().toArray()));
    assertThat(values).containsExactly(Optional.of(Fixture.A_VALUE));
    assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, Fixture.A.getBytes())).isFalse();
    assertThat(fixture.head.getCacheManager()).isSameAs(FlatDbCacheManager.NO_OP_CACHE);
  }

  @Test
  void emptyKeys_returnsEmptyList() throws Exception {
    fixture = Fixture.builder().mode(Mode.FULL).shape(Shape.LAYER).keys(KeySet.MIX).build();
    assertThat(fixture.subject.getMultipleFlat(ACCOUNT_INFO_STATE, List.of())).isEmpty();
    assertThat(fixture.head.getMultipleFlat(ACCOUNT_INFO_STATE, List.of())).isEmpty();
  }

  /**
   * The no-op cache manager must expand a strategy no-op ({@code List.of()}) into key-aligned null
   * slots, exactly like {@code VersionedFlatDbCacheManager}. Returning {@code List.of()} here would
   * signal a whole-op no-op and break callers that index by position.
   */
  @Test
  void noOpCache_strategyNoOp_returnsAlignedNullSlots() throws Exception {
    fixture =
        Fixture.builder()
            .mode(Mode.PARTIAL)
            .shape(Shape.HEAD)
            .keys(KeySet.MIX)
            .crossBlockCache(false)
            .build();
    assertThat(fixture.head.getCacheManager()).isSameAs(FlatDbCacheManager.NO_OP_CACHE);

    final List<Optional<Bytes>> values =
        fixture.subject.getMultipleFlat(ACCOUNT_INFO_STATE, fixture.queryKeys);

    assertThat(values).hasSize(fixture.queryKeys.size());
    assertThat(values).containsOnlyNulls();
  }

  /**
   * {@link BonsaiArchiveWorldStateLayerStorage} is the one parent that answers a whole-op {@code
   * List.of()} to an open child. The child must leave those slots unresolved instead of indexing
   * into a shorter list.
   */
  @Test
  void layerOverNoOpParent_leavesMissesUnresolvedInsteadOfThrowing() throws Exception {
    fixture =
        Fixture.builder()
            .mode(Mode.ARCHIVE)
            .shape(Shape.ARCHIVE_LAYER)
            .keys(KeySet.MIX)
            .poisonSharedCache(false)
            .build();

    try (final BonsaiWorldStateLayerStorage layer =
        new BonsaiWorldStateLayerStorage(fixture.subject)) {
      final List<Optional<Bytes>> values =
          layer.getMultipleFlat(ACCOUNT_INFO_STATE, fixture.queryKeys);

      assertThat(values).hasSize(fixture.queryKeys.size());
      assertThat(values).containsOnlyNulls();
      for (final Hash key : fixture.queriedHashes()) {
        assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, key.getBytes())).isFalse();
      }
    }
  }

  /**
   * Overlay hits never go through the parent fetch, so they must not warm the shared head cache.
   * Parent misses in the same call still fetch and cache as usual.
   */
  @Test
  void fullLayer_overlayValueAndTombstone_doNotWarmHeadCache() throws Exception {
    fixture = Fixture.builder().mode(Mode.FULL).shape(Shape.LAYER).keys(KeySet.MIX).build();
    final BonsaiWorldStateLayerStorage layer = (BonsaiWorldStateLayerStorage) fixture.subject;
    final Hash overlayPresent = Hash.hash(Bytes.of(10));
    final Hash overlayTombstone = Hash.hash(Bytes.of(11));
    final Bytes overlayValue = Bytes.of(10, 20, 30);

    final var updater = layer.updater();
    updater.putAccountInfoState(overlayPresent, overlayValue);
    updater.removeAccountInfoState(overlayTombstone);
    updater.commit();

    fixture.head.getCacheManager().clear(ACCOUNT_INFO_STATE);

    final List<Optional<Bytes>> values =
        layer.getMultipleFlat(
            ACCOUNT_INFO_STATE,
            List.of(
                overlayPresent.getBytes().toArray(),
                overlayTombstone.getBytes().toArray(),
                Fixture.A.getBytes().toArray(),
                Fixture.B.getBytes().toArray()));

    assertThat(values)
        .containsExactly(
            Optional.of(overlayValue),
            Optional.empty(),
            Optional.of(Fixture.A_VALUE),
            Optional.empty());
    assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, overlayPresent.getBytes()))
        .as("overlay value must not warm shared head cache")
        .isFalse();
    assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, overlayTombstone.getBytes()))
        .as("overlay tombstone must not warm shared head cache")
        .isFalse();
    assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, Fixture.A.getBytes()))
        .as("parent hits still warm the head cache")
        .isTrue();
    assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, Fixture.B.getBytes()))
        .as("parent absences still warm as removals")
        .isTrue();
  }

  private void assertWholeOpEmpty(final List<Optional<Bytes>> values) {
    assertThat(values).isEmpty();
    for (final Hash key : fixture.queriedHashes()) {
      // Archive-layer / closed must not warm previously-uncached keys.
      if (!fixture.wasPreCached(key)) {
        assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, key.getBytes())).isFalse();
      }
    }
  }

  private void assertFullFetch(final List<Optional<Bytes>> values) {
    assertThat(values).hasSize(fixture.queryKeys.size());
    for (int i = 0; i < fixture.queryKeys.size(); i++) {
      final Hash key = fixture.queriedHashes().get(i);
      final Bytes expected = fixture.seededValue(key);
      if (expected != null) {
        assertThat(values.get(i)).contains(expected);
        assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, key.getBytes())).isTrue();
        assertThat(fixture.head.getCachedValue(ACCOUNT_INFO_STATE, key.getBytes()))
            .hasValueSatisfying(
                cv -> {
                  assertThat(cv.getValue()).isEqualTo(expected);
                  assertThat(cv.isRemoval()).isFalse();
                });
      } else {
        assertThat(values.get(i)).isEmpty();
        assertThat(values.get(i)).isNotNull();
        assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, key.getBytes())).isTrue();
        assertThat(fixture.head.getCachedValue(ACCOUNT_INFO_STATE, key.getBytes()))
            .hasValueSatisfying(cv -> assertThat(cv.isRemoval()).isTrue());
      }
    }
  }

  private void assertStrategyNoOpNulls(final List<Optional<Bytes>> values) {
    assertThat(values).hasSize(fixture.queryKeys.size());
    for (int i = 0; i < fixture.queryKeys.size(); i++) {
      final Hash key = fixture.queriedHashes().get(i);
      assertThat(values.get(i)).isNull();
      assertThat(fixture.head.isCached(ACCOUNT_INFO_STATE, key.getBytes())).isFalse();
    }
    // Scalar path still resolves committed flat data when strategy batch is a no-op.
    if (fixture.mode == Mode.PARTIAL) {
      for (final Hash key : fixture.queriedHashes()) {
        final Bytes expected = fixture.seededValue(key);
        if (expected != null) {
          assertThat(fixture.head.getAccount(key)).contains(expected);
        }
      }
    }
  }

  private static void commitAccount(
      final BonsaiWorldStateKeyValueStorage head, final Hash account, final Bytes value) {
    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountInfoState(account, value);
    u.commit();
  }

  enum Mode {
    FULL,
    PARTIAL,
    ARCHIVE
  }

  enum Shape {
    HEAD,
    SNAPSHOT,
    LAYER,
    ARCHIVE_LAYER
  }

  enum KeySet {
    /** Two keys, both committed. */
    ALL_PRESENT,
    /** Two keys, neither committed. */
    ALL_ABSENT,
    /** One committed + one absent. */
    MIX,
    /** Miss then hit: miss only in storage; hit pre-seeded in versioned cache. */
    CACHE_HIT_MIX
  }

  static final class Fixture implements AutoCloseable {
    static final Hash A = Hash.hash(Bytes.of(1));
    static final Hash B = Hash.hash(Bytes.of(2));
    static final Hash HIT = Hash.hash(Bytes.of(3));
    static final Hash MISS = Hash.hash(Bytes.of(4));
    static final Bytes A_VALUE = Bytes.of(0x11);
    static final Bytes B_VALUE = Bytes.of(0x22);
    static final Bytes HIT_VALUE = Bytes.of(0x33);
    static final Bytes MISS_VALUE = Bytes.of(0x44);

    final Mode mode;
    final Shape shape;
    final KeySet keySet;
    final BonsaiWorldStateKeyValueStorage head;
    final BonsaiWorldStateKeyValueStorage subject;
    final List<byte[]> queryKeys;
    final List<Hash> queriedHashes;
    private final List<Hash> preCachedKeys;

    private Fixture(
        final Mode mode,
        final Shape shape,
        final KeySet keySet,
        final BonsaiWorldStateKeyValueStorage head,
        final BonsaiWorldStateKeyValueStorage subject,
        final List<byte[]> queryKeys,
        final List<Hash> queriedHashes,
        final List<Hash> preCachedKeys) {
      this.mode = mode;
      this.shape = shape;
      this.keySet = keySet;
      this.head = head;
      this.subject = subject;
      this.queryKeys = queryKeys;
      this.queriedHashes = queriedHashes;
      this.preCachedKeys = preCachedKeys;
    }

    List<Hash> queriedHashes() {
      return queriedHashes;
    }

    boolean wasPreCached(final Hash key) {
      return preCachedKeys.contains(key);
    }

    Bytes seededValue(final Hash key) {
      if (key.equals(A)) {
        return keySet == KeySet.ALL_ABSENT ? null : A_VALUE;
      }
      if (key.equals(B)) {
        return keySet == KeySet.ALL_PRESENT ? B_VALUE : null;
      }
      if (key.equals(HIT)) {
        return HIT_VALUE;
      }
      if (key.equals(MISS)) {
        return MISS_VALUE;
      }
      return null;
    }

    static Builder builder() {
      return new Builder();
    }

    @Override
    public void close() throws Exception {
      if (subject != head) {
        try {
          subject.close();
        } catch (final Exception ignored) {
          // already closed in closedCases
        }
      }
      if (head.getCacheManager() instanceof final Closeable closeable) {
        closeable.close();
      }
      head.close();
    }

    static final class Builder {
      private Mode mode = Mode.FULL;
      private Shape shape = Shape.HEAD;
      private KeySet keys = KeySet.MIX;
      private boolean closeSubject;
      private boolean poisonSharedCache;
      private boolean crossBlockCache = true;

      Builder mode(final Mode mode) {
        this.mode = mode;
        return this;
      }

      Builder shape(final Shape shape) {
        this.shape = shape;
        return this;
      }

      Builder keys(final KeySet keys) {
        this.keys = keys;
        return this;
      }

      Builder closeSubject() {
        return closeSubject(true);
      }

      Builder closeSubject(final boolean closeSubject) {
        this.closeSubject = closeSubject;
        return this;
      }

      Builder poisonSharedCache() {
        return poisonSharedCache(true);
      }

      Builder poisonSharedCache(final boolean poisonSharedCache) {
        this.poisonSharedCache = poisonSharedCache;
        return this;
      }

      Builder crossBlockCache(final boolean enabled) {
        this.crossBlockCache = enabled;
        return this;
      }

      Fixture build() throws Exception {
        final BonsaiWorldStateKeyValueStorage head = createHead(mode, crossBlockCache);
        final List<Hash> preCached = new ArrayList<>();

        switch (keys) {
          case ALL_PRESENT -> {
            commitAccount(head, A, A_VALUE);
            commitAccount(head, B, B_VALUE);
          }
          case ALL_ABSENT -> {
            // nothing committed
          }
          case MIX -> commitAccount(head, A, A_VALUE);
          case CACHE_HIT_MIX -> {
            commitAccount(head, HIT, HIT_VALUE);
            commitAccount(head, MISS, MISS_VALUE);
          }
        }
        head.getCacheManager().clear(ACCOUNT_INFO_STATE);

        if (keys == KeySet.CACHE_HIT_MIX) {
          head.getCacheManager()
              .putInCache(ACCOUNT_INFO_STATE, HIT.getBytes(), HIT_VALUE, head.getCurrentVersion());
          preCached.add(HIT);
        }

        if (poisonSharedCache) {
          head.getCacheManager()
              .putInCache(ACCOUNT_INFO_STATE, A.getBytes(), A_VALUE, head.getCurrentVersion());
          preCached.add(A);
        }

        BonsaiWorldStateKeyValueStorage subject = wrapShape(head, shape);
        if (closeSubject) {
          subject.close();
        }

        final List<Hash> queried =
            switch (keys) {
              case ALL_PRESENT, ALL_ABSENT, MIX -> List.of(A, B);
              case CACHE_HIT_MIX -> List.of(MISS, HIT);
            };
        final List<byte[]> queryKeys = queried.stream().map(h -> h.getBytes().toArray()).toList();

        return new Fixture(mode, shape, keys, head, subject, queryKeys, queried, preCached);
      }

      private static BonsaiWorldStateKeyValueStorage wrapShape(
          final BonsaiWorldStateKeyValueStorage head, final Shape shape) {
        return switch (shape) {
          case HEAD -> head;
          case SNAPSHOT -> new BonsaiSnapshotWorldStateKeyValueStorage(head);
          case LAYER -> new BonsaiWorldStateLayerStorage(head);
          case ARCHIVE_LAYER -> new BonsaiArchiveWorldStateLayerStorage(head);
        };
      }

      private static BonsaiWorldStateKeyValueStorage createHead(
          final Mode mode, final boolean crossBlock) {
        return switch (mode) {
          case FULL -> {
            final var h =
                new BonsaiWorldStateKeyValueStorage(
                    new InMemoryKeyValueStorageProvider(),
                    new NoOpMetricsSystem(),
                    dataConfig(crossBlock, true));
            h.upgradeToFullFlatDbMode();
            assertThat(h.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);
            assertThat(h.getFlatDbStrategy()).isInstanceOf(BonsaiFullFlatDbStrategy.class);
            yield h;
          }
          case PARTIAL -> {
            final var h =
                new BonsaiWorldStateKeyValueStorage(
                    new InMemoryKeyValueStorageProvider(),
                    new NoOpMetricsSystem(),
                    dataConfig(crossBlock, false));
            assertThat(h.getFlatDbMode()).isEqualTo(FlatDbMode.PARTIAL);
            assertThat(h.getFlatDbStrategy()).isInstanceOf(BonsaiPartialFlatDbStrategy.class);
            yield h;
          }
          case ARCHIVE -> {
            final var config =
                ImmutableDataStorageConfiguration.builder()
                    .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
                    .extraStorageConfiguration(
                        ImmutableExtraStorageConfiguration.builder()
                            .unstable(
                                ImmutableExtraStorageConfiguration.Unstable.builder()
                                    .bonsaiCrossBlockCacheEnabled(crossBlock)
                                    .build())
                            .build())
                    .build();
            final var metrics = new NoOpMetricsSystem();
            final var underlying =
                new BonsaiWorldStateKeyValueStorage(
                    new InMemoryKeyValueStorageProvider(), metrics, config);
            final var archiveProvider =
                new BonsaiArchiveReadFlatDbStrategyProvider(metrics, config);
            archiveProvider.loadFlatDbStrategy(underlying.getComposedWorldStateStorage());
            final var h =
                new BonsaiWorldStateKeyValueStorage(
                    archiveProvider,
                    underlying.getComposedWorldStateStorage(),
                    underlying.getTrieLogStorage(),
                    underlying.getCacheManager(),
                    underlying.getCurrentVersion());
            assertThat(h.getFlatDbStrategy()).isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
            yield h;
          }
        };
      }

      private static ImmutableDataStorageConfiguration dataConfig(
          final boolean crossBlock, final boolean fullFlat) {
        return ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .extraStorageConfiguration(
                ImmutableExtraStorageConfiguration.builder()
                    .unstable(
                        ImmutableExtraStorageConfiguration.Unstable.builder()
                            .fullFlatDbEnabled(fullFlat)
                            .bonsaiCrossBlockCacheEnabled(crossBlock)
                            .build())
                    .build())
            .build();
      }
    }
  }
}
