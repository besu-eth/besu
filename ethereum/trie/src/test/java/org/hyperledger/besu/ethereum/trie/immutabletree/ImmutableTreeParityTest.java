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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.CompactEncoding.bytesToPath;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.Proof;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks that immutable trees produce the same roots, reads, proofs and stored nodes as Besu's
 * {@link StoredMerklePatriciaTrie}, both when built from empty and on top of a stored trie.
 */
class ImmutableTreeParityTest {

  private static final ForkJoinPool POOL = new ForkJoinPool(4);
  private static final NodeLoader NO_STORAGE = (location, hash) -> Optional.empty();

  @AfterAll
  static void shutdown() {
    POOL.shutdownNow();
  }

  /** Exposes Besu's deferred put visitor, which {@code putDeferred} does not use. */
  private static final class BesuTrie extends StoredMerklePatriciaTrie<Bytes, Bytes> {
    BesuTrie(final NodeLoader loader, final Bytes32 root) {
      super(loader, root, Function.identity(), Function.identity());
    }

    void merge(final Bytes key, final Function<Optional<Bytes>, Optional<Bytes>> merger) {
      put(key, deferredVisitor(merger));
    }

    PathNodeVisitor<Bytes> deferredVisitor(
        final Function<Optional<Bytes>, Optional<Bytes>> merger) {
      return getDeferredPutVisitor(merger);
    }
  }

  private enum Op {
    PUT,
    REMOVE,
    MERGE
  }

  private record Change(Op op, Bytes key, Bytes value, int mergeKind) {

    Function<Optional<Bytes>, Optional<Bytes>> merger(final AtomicInteger calls) {
      return prior -> {
        calls.incrementAndGet();
        return switch (mergeKind) {
          case 0 -> Optional.empty();
          case 1 -> prior;
          default ->
              Optional.of(
                  prior
                      .map(v -> v.size() < 40 ? Bytes.concatenate(v, Bytes.of(1)) : v.slice(1))
                      .orElse(value));
        };
      };
    }

    TreeUpdate toUpdate(final AtomicInteger calls) {
      return switch (op) {
        case PUT -> TreeUpdate.put(bytesToPath(key), value);
        case REMOVE -> TreeUpdate.remove(bytesToPath(key));
        case MERGE -> TreeUpdate.merge(bytesToPath(key), merger(calls));
      };
    }

    void applyTo(final BesuTrie besu, final AtomicInteger calls) {
      switch (op) {
        case PUT -> besu.put(key, value);
        case REMOVE -> besu.remove(key);
        case MERGE -> besu.merge(key, merger(calls));
      }
    }
  }

  private static Bytes randomBytes(final Random random, final int size) {
    final byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return Bytes.wrap(bytes);
  }

  private static Bytes randomValue(final Random random) {
    // 1..40 bytes: nodes below and above the 32 byte inline threshold
    return randomBytes(random, 1 + random.nextInt(40));
  }

  private static Change randomChange(final Random random, final List<Bytes> keys) {
    final Bytes key = keys.get(random.nextInt(keys.size()));
    final int roll = random.nextInt(10);
    final Op op = roll < 6 ? Op.PUT : roll < 9 ? Op.REMOVE : Op.MERGE;
    return new Change(op, key, randomValue(random), random.nextInt(3));
  }

  private static List<Bytes> hashedKeys(final Random random, final int count) {
    final List<Bytes> keys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      keys.add(randomBytes(random, 32));
    }
    return keys;
  }

  /** Short keys of varying length, so that some keys are prefixes of others. */
  private static List<Bytes> shortKeys(final Random random, final int count) {
    final List<Bytes> keys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final Bytes key = randomBytes(random, 1 + random.nextInt(3));
      // small alphabet: many shared prefixes
      final byte[] bytes = key.toArray();
      for (int j = 0; j < bytes.length; j++) {
        bytes[j] = (byte) (bytes[j] & 0x33);
      }
      keys.add(Bytes.wrap(bytes));
    }
    return keys;
  }

  private static TreeNode applySingle(
      final TreeSession session,
      final TreeNode root,
      final Change change,
      final AtomicInteger calls) {
    return change.toUpdate(calls).applyTo(session, root, 0);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5})
  void singleOperationsMatchBesuWithHashedKeys(final int seed) {
    assertSingleOperationParity(new Random(seed), hashedKeys(new Random(seed * 31L), 300), 3000);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8})
  void singleOperationsMatchBesuWithPrefixKeys(final int seed) {
    // Keys that are prefixes of each other put values into branches: Besu's rules for those,
    // including the ones that leave non-canonical nodes, must be reproduced exactly
    assertSingleOperationParity(new Random(seed), shortKeys(new Random(seed * 17L), 40), 1500);
  }

  private void assertSingleOperationParity(
      final Random random, final List<Bytes> keys, final int operations) {
    final BesuTrie besu = new BesuTrie(NO_STORAGE, MerkleTrie.EMPTY_TRIE_NODE_HASH);
    final TreeSession session = TreeSession.create(NO_STORAGE, 1);
    final AtomicInteger myCalls = new AtomicInteger();
    final AtomicInteger besuCalls = new AtomicInteger();
    TreeNode root = EmptyTreeNode.INSTANCE;
    int failures = 0;
    for (int i = 0; i < operations; i++) {
      final Change change = randomChange(random, keys);
      // Besu's rules can leave non-canonical nodes behind for keys that are prefixes of others,
      // on which later operations fail its own assertions. Parity means failing on the same ones.
      Throwable besuFailure = null;
      try {
        change.applyTo(besu, besuCalls);
      } catch (final AssertionError | RuntimeException e) {
        besuFailure = e;
      }
      Throwable myFailure = null;
      try {
        root = applySingle(session, root, change, myCalls);
      } catch (final AssertionError | RuntimeException e) {
        myFailure = e;
      }
      if (besuFailure == null) {
        assertThat(myFailure).describedAs("operation %s: %s", i, change).isNull();
      } else {
        assertThat(myFailure)
            .describedAs("operation %s: %s, Besu failed with %s", i, change, besuFailure)
            .isNotNull();
        failures++;
        // a failed merge may or may not have called its function before failing
        myCalls.set(besuCalls.get());
      }
      assertThat(root.hash())
          .describedAs("root after %s: %s", i, change)
          .isEqualTo(besu.getRootHash());
      if (i % 100 == 0) {
        for (final Bytes key : keys) {
          assertThat(TreeOps.get(session, root, bytesToPath(key))).isEqualTo(besu.get(key));
        }
      }
    }
    assertThat(myCalls.get()).isEqualTo(besuCalls.get());
    assertThat(failures).isLessThan(operations);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6})
  void batchesMatchBesu(final int seed) {
    final Random random = new Random(seed);
    final List<Bytes> keys = hashedKeys(random, 2000);
    final BesuTrie besu = new BesuTrie(NO_STORAGE, MerkleTrie.EMPTY_TRIE_NODE_HASH);
    TreeNode sequential = EmptyTreeNode.INSTANCE;
    TreeNode parallel = EmptyTreeNode.INSTANCE;
    for (int round = 0; round < 12; round++) {
      final Map<Bytes, Change> batch = new HashMap<>();
      final int size = round == 0 ? 1500 : 1 + random.nextInt(round % 3 == 0 ? 600 : 40);
      for (int i = 0; i < size; i++) {
        final Change change = randomChange(random, keys);
        batch.put(change.key(), change);
      }
      final AtomicInteger besuCalls = new AtomicInteger();
      final AtomicInteger sequentialCalls = new AtomicInteger();
      final AtomicInteger parallelCalls = new AtomicInteger();
      batch.values().forEach(change -> change.applyTo(besu, besuCalls));

      sequential =
          BatchUpdater.apply(
              TreeSession.create(NO_STORAGE, round),
              sequential,
              sortedUpdates(batch, sequentialCalls),
              null);
      parallel =
          BatchUpdater.apply(
              TreeSession.create(NO_STORAGE, round),
              parallel,
              sortedUpdates(batch, parallelCalls),
              POOL);

      assertThat(sequential.hash()).describedAs("round %s", round).isEqualTo(besu.getRootHash());
      assertThat(parallel.hash()).describedAs("round %s", round).isEqualTo(besu.getRootHash());
      assertThat(sequentialCalls.get()).isEqualTo(besuCalls.get());
      assertThat(parallelCalls.get()).isEqualTo(besuCalls.get());
    }
  }

  @Test
  void removingEverythingInABatchLeavesTheEmptyTree() {
    final Random random = new Random(42);
    final List<Bytes> keys = hashedKeys(random, 500);
    final Map<Bytes, Change> puts = new HashMap<>();
    final Map<Bytes, Change> removes = new HashMap<>();
    keys.forEach(
        key -> {
          puts.put(key, new Change(Op.PUT, key, randomValue(random), 0));
          removes.put(key, new Change(Op.REMOVE, key, null, 0));
        });
    final AtomicInteger calls = new AtomicInteger();
    TreeNode root =
        BatchUpdater.apply(
            TreeSession.create(NO_STORAGE, 1),
            EmptyTreeNode.INSTANCE,
            sortedUpdates(puts, calls),
            POOL);
    root =
        BatchUpdater.apply(
            TreeSession.create(NO_STORAGE, 2), root, sortedUpdates(removes, calls), POOL);
    assertThat(root).isSameAs(EmptyTreeNode.INSTANCE);
    assertThat(root.hash()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);
  }

  private static TreeUpdate[] sortedUpdates(
      final Map<Bytes, Change> batch, final AtomicInteger calls) {
    return batch.values().stream()
        .map(change -> change.toUpdate(calls))
        .sorted((a, b) -> a.path().compareTo(b.path()))
        .toArray(TreeUpdate[]::new);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void diskBackedUpdatesMatchBesuAndPersistTheSameNodes(final int seed) {
    final Random random = new Random(seed);
    final List<Bytes> keys = hashedKeys(random, 3000);

    // A stored trie, committed by Besu
    final PathBasedNodeStore besuStore = new PathBasedNodeStore();
    final BesuTrie besu = new BesuTrie(besuStore, MerkleTrie.EMPTY_TRIE_NODE_HASH);
    for (final Bytes key : keys.subList(0, 2000)) {
      besu.put(key, randomValue(random));
    }
    besu.commit(besuStore);
    Bytes32 besuRoot = besu.getRootHash();
    final PathBasedNodeStore myStore = besuStore.copy();

    // Opening loads the root only
    myStore.resetReads();
    TreeSession session = TreeSession.create(myStore, 1);
    TreeNode root = session.loadRoot(besuRoot);
    assertThat(myStore.reads()).isEqualTo(1);
    assertThat(root).isInstanceOf(BranchTreeNode.class);
    for (int i = 0; i < 16; i++) {
      assertThat(((BranchTreeNode) root).child(i)).isInstanceOf(StoredTreeNode.class);
    }

    for (int round = 0; round < 8; round++) {
      final Map<Bytes, Change> batch = new HashMap<>();
      final int size = 1 + random.nextInt(round % 2 == 0 ? 60 : 5);
      for (int i = 0; i < size; i++) {
        final Change change = randomChange(random, keys);
        batch.put(change.key(), change);
      }
      // Besu reopens its trie from storage, as Bonsai does every block
      final BesuTrie reopened = new BesuTrie(besuStore, besuRoot);
      final AtomicInteger besuCalls = new AtomicInteger();
      batch.values().forEach(change -> change.applyTo(reopened, besuCalls));
      reopened.commit(besuStore);
      besuRoot = reopened.getRootHash();

      // The tree carries over in memory, partly loaded, like a cached root
      myStore.resetReads();
      final AtomicInteger myCalls = new AtomicInteger();
      root =
          BatchUpdater.apply(
              session, root, sortedUpdates(batch, myCalls), round % 2 == 0 ? POOL : null);
      assertThat(root.hash()).describedAs("round %s", round).isEqualTo(besuRoot);
      assertThat(myCalls.get()).isEqualTo(besuCalls.get());
      // Only nodes on updated paths (and flattened siblings) are loaded
      assertThat(myStore.reads()).isLessThanOrEqualTo(batch.size() * 12);

      TreeOps.commit(session, root, myStore, round % 2 == 0 ? POOL : null);
      session = session.renew();

      // Both storages hold the same nodes at every reachable location
      assertThat(myStore.reachableNodes(besuRoot)).isEqualTo(besuStore.reachableNodes(besuRoot));
    }
  }

  @Test
  void singleKeyUpdatesOnStoredTrieKeepSiblingsStored() {
    final Random random = new Random(7);
    final List<Bytes> keys = hashedKeys(random, 1000);
    final PathBasedNodeStore store = new PathBasedNodeStore();
    final BesuTrie besu = new BesuTrie(store, MerkleTrie.EMPTY_TRIE_NODE_HASH);
    keys.forEach(key -> besu.put(key, randomValue(random)));
    besu.commit(store);

    final TreeSession session = TreeSession.create(store, 1);
    TreeNode root = session.loadRoot(besu.getRootHash());
    final Bytes value = randomValue(random);
    root = TreeOps.put(session, root, bytesToPath(keys.get(0)), 0, value);
    besu.put(keys.get(0), value);
    root = TreeOps.remove(session, root, bytesToPath(keys.get(1)), 0);
    besu.remove(keys.get(1));
    assertThat(root.hash()).isEqualTo(besu.getRootHash());

    final int[] counts = countNodes(root);
    // two paths materialized, everything else a placeholder
    assertThat(counts[0]).isLessThan(30);
    assertThat(counts[1]).isGreaterThan(20);
  }

  /** Returns {materialized, stored} node counts reachable from root. */
  private static int[] countNodes(final TreeNode root) {
    final int[] counts = new int[2];
    countNodes(root, counts);
    return counts;
  }

  private static void countNodes(final TreeNode node, final int[] counts) {
    switch (node) {
      case StoredTreeNode ignored -> counts[1]++;
      case BranchTreeNode branch -> {
        counts[0]++;
        for (int i = 0; i < 16; i++) {
          countNodes(branch.child(i), counts);
        }
      }
      case ExtensionTreeNode extension -> {
        counts[0]++;
        countNodes(extension.child(), counts);
      }
      case LeafTreeNode ignored -> counts[0]++;
      case EmptyTreeNode ignored -> {}
    }
  }

  @Test
  void commitWritesOnlyTheNodesTheSessionCreated() {
    final Random random = new Random(11);
    final List<Bytes> keys = hashedKeys(random, 2000);
    final PathBasedNodeStore store = new PathBasedNodeStore();
    TreeSession session = TreeSession.create(store, 1);
    TreeNode root = EmptyTreeNode.INSTANCE;
    final Map<Bytes, Change> batch = new HashMap<>();
    keys.forEach(key -> batch.put(key, new Change(Op.PUT, key, randomValue(random), 0)));
    root = BatchUpdater.apply(session, root, sortedUpdates(batch, new AtomicInteger()), POOL);
    final AtomicInteger writes = new AtomicInteger();
    TreeOps.commit(
        session,
        root,
        (l, h, v) -> {
          writes.incrementAndGet();
          store.store(l, h, v);
        },
        null);
    assertThat(writes.get()).isGreaterThan(2000);

    session = session.renew();
    final Bytes value = randomValue(random);
    root = TreeOps.put(session, root, bytesToPath(keys.get(5)), 0, value);
    writes.set(0);
    TreeOps.commit(
        session,
        root,
        (l, h, v) -> {
          writes.incrementAndGet();
          store.store(l, h, v);
        },
        null);
    // one leaf and the branches above it
    assertThat(writes.get()).isBetween(2, 8);

    final BesuTrie besu = new BesuTrie(store, root.hash());
    for (final Bytes key : keys) {
      assertThat(besu.get(key)).isEqualTo(TreeOps.get(session, root, bytesToPath(key)));
    }
  }

  @Test
  void decodedNodesReencodeToTheStoredBytes() {
    final Random random = new Random(3);
    final PathBasedNodeStore store = new PathBasedNodeStore();
    final BesuTrie besu = new BesuTrie(store, MerkleTrie.EMPTY_TRIE_NODE_HASH);
    // short keys and values give inline children as well as hashed ones
    shortKeys(random, 200)
        .forEach(key -> besu.put(key, randomBytes(random, 1 + random.nextInt(3))));
    hashedKeys(random, 300).forEach(key -> besu.put(key, randomValue(random)));
    besu.commit(store);

    final Map<Bytes, Bytes> nodes = store.reachableNodes(besu.getRootHash());
    assertThat(nodes).hasSizeGreaterThan(300);
    nodes.forEach(
        (location, rlp) -> {
          final Bytes32 hash = org.hyperledger.besu.crypto.Hash.keccak256(rlp);
          final TreeNode decoded = TreeNodeCodec.decode(location, hash, rlp, 1);
          final MaterializedTreeNode materialized = (MaterializedTreeNode) decoded;
          assertThat(materialized.computeEncoding()).isEqualTo(rlp);
          assertThat(decoded.hash()).isEqualTo(hash);
          assertPlaceholderLocations(materialized, location);
        });
  }

  private static void assertPlaceholderLocations(final TreeNode node, final Bytes location) {
    if (node instanceof BranchTreeNode branch) {
      for (int i = 0; i < 16; i++) {
        final Bytes childLocation = Bytes.concatenate(location, Bytes.of(i));
        if (branch.child(i) instanceof StoredTreeNode stored) {
          assertThat(stored.location()).isEqualTo(childLocation);
        } else {
          assertPlaceholderLocations(branch.child(i), childLocation);
        }
      }
    } else if (node instanceof ExtensionTreeNode extension) {
      final Bytes childLocation = Bytes.concatenate(location, extension.path());
      if (extension.child() instanceof StoredTreeNode stored) {
        assertThat(stored.location()).isEqualTo(childLocation);
      } else {
        assertPlaceholderLocations(extension.child(), childLocation);
      }
    }
  }

  @Test
  void updatesLeaveEarlierRootsUnchanged() {
    final Random random = new Random(5);
    final List<Bytes> keys = hashedKeys(random, 400);
    final PathBasedNodeStore store = new PathBasedNodeStore();
    final List<TreeNode> roots = new ArrayList<>();
    final List<Map<Bytes32, Bytes>> contents = new ArrayList<>();
    final Map<Bytes32, Bytes> expected = new TreeMap<>();
    TreeNode root = EmptyTreeNode.INSTANCE;
    for (int step = 0; step < 30; step++) {
      final Map<Bytes, Change> batch = new HashMap<>();
      for (int i = 0; i < 50; i++) {
        final Change change = randomChange(random, keys);
        batch.put(change.key(), change);
      }
      final AtomicInteger calls = new AtomicInteger();
      root =
          BatchUpdater.apply(
              TreeSession.create(store, step), root, sortedUpdates(batch, calls), POOL);
      for (final Change change : batch.values()) {
        final Bytes32 key = Bytes32.wrap(change.key());
        final Optional<Bytes> prior = Optional.ofNullable(expected.get(key));
        final Optional<Bytes> next =
            switch (change.op()) {
              case PUT -> Optional.of(change.value());
              case REMOVE -> Optional.empty();
              case MERGE -> change.merger(new AtomicInteger()).apply(prior);
            };
        next.ifPresentOrElse(v -> expected.put(key, v), () -> expected.remove(key));
      }
      roots.add(root);
      contents.add(new TreeMap<>(expected));
    }
    for (int step = 0; step < roots.size(); step++) {
      final TreeNode old = roots.get(step);
      final TreeSession reader = TreeSession.create(store, 100);
      assertThat(TreeOps.entriesFrom(reader, old, Bytes32.ZERO, Integer.MAX_VALUE, true))
          .describedAs("content of root %s", step)
          .isEqualTo(contents.get(step));
      final BesuTrie fresh = new BesuTrie(NO_STORAGE, MerkleTrie.EMPTY_TRIE_NODE_HASH);
      contents.get(step).forEach(fresh::put);
      assertThat(old.hash()).isEqualTo(fresh.getRootHash());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void entriesFromAndProofsMatchBesu(final int seed) {
    final Random random = new Random(seed);
    final List<Bytes> keys = hashedKeys(random, 800);
    final PathBasedNodeStore store = new PathBasedNodeStore();
    final BesuTrie besu = new BesuTrie(store, MerkleTrie.EMPTY_TRIE_NODE_HASH);
    keys.forEach(key -> besu.put(key, randomValue(random)));
    besu.commit(store);
    final BesuTrie reopened = new BesuTrie(store, besu.getRootHash());
    final TreeSession session = TreeSession.create(store, 1);
    final TreeNode root = session.loadRoot(besu.getRootHash());

    for (int i = 0; i < 50; i++) {
      final Bytes32 start = Bytes32.wrap(randomBytes(random, 32));
      final int limit = 1 + random.nextInt(100);
      assertThat(TreeOps.entriesFrom(session, root, start, limit, i % 2 == 0))
          .isEqualTo(reopened.entriesFrom(start, limit));
    }
    assertThat(TreeOps.entriesFrom(session, root, Bytes32.ZERO, Integer.MAX_VALUE, false))
        .isEqualTo(reopened.entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE));

    for (int i = 0; i < 100; i++) {
      final Bytes key =
          i % 2 == 0 ? keys.get(random.nextInt(keys.size())) : randomBytes(random, 32);
      final Proof<Bytes> mine = TreeOps.proof(session, root, bytesToPath(key));
      final Proof<Bytes> expected = besu.getValueWithProof(key);
      assertThat(mine.getValue()).isEqualTo(expected.getValue());
      assertThat(mine.getProofRelatedNodes()).isEqualTo(expected.getProofRelatedNodes());
    }
  }

  @Test
  void placeholdersAreResolvedThroughTheSessionLoader() {
    final Random random = new Random(9);
    final List<Bytes> keys = hashedKeys(random, 500);
    final PathBasedNodeStore store = new PathBasedNodeStore();
    final BesuTrie besu = new BesuTrie(store, MerkleTrie.EMPTY_TRIE_NODE_HASH);
    keys.forEach(key -> besu.put(key, randomValue(random)));
    besu.commit(store);

    // A tree opened through one session and read through another: nodes hold no loader
    final TreeNode root = TreeSession.create(store, 1).loadRoot(besu.getRootHash());
    final AtomicInteger secondLoads = new AtomicInteger();
    final TreeSession second =
        TreeSession.create(
            (location, hash) -> {
              secondLoads.incrementAndGet();
              return store.getNode(location, hash);
            },
            2);
    assertThat(TreeOps.get(second, root, bytesToPath(keys.get(3))))
        .isEqualTo(besu.get(keys.get(3)));
    assertThat(secondLoads.get()).isPositive();
    // loaded into place: the same read does not load again
    final int loads = secondLoads.get();
    assertThat(TreeOps.get(second, root, bytesToPath(keys.get(3))))
        .isEqualTo(besu.get(keys.get(3)));
    assertThat(secondLoads.get()).isEqualTo(loads);
  }

  @Test
  void unusedVisitorIsBesusDeferredPut() {
    // guard against the test harness silently falling back to get + put
    final BesuTrie besu = new BesuTrie(NO_STORAGE, MerkleTrie.EMPTY_TRIE_NODE_HASH);
    final PathNodeVisitor<Bytes> visitor = besu.deferredVisitor(prior -> prior);
    assertThat(visitor.getClass().getSimpleName()).isEqualTo("DeferredPutVisitor");
  }
}
