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
package org.hyperledger.besu.ethereum.trie;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Hashes the account trie the way a block does: its new root on the account-trie pool, and the
 * preloader's walks on virtual threads.
 *
 * <p>The trie holds {@link #accounts} accounts, its nodes kept in memory by hash. One benchmark
 * operation is one block that changes {@link #touchedAccounts} of them:
 *
 * <ul>
 *   <li>{@link #stateRoot} commits the changes on Besu's account-trie fork-join pool, which hashes
 *       every node on their paths again.
 *   <li>{@link #preloadWalks} walks to each changed account in a virtual thread of its own, as the
 *       trie preloader does, and checks the hash of every node it loads, as a read from storage
 *       does.
 * </ul>
 *
 * <p>In a running Besu the preloader takes the upper levels of the trie from its cache without
 * checking them, so its walks check fewer nodes than these.
 */
@State(Scope.Benchmark)
// the trie keeps ~0.5 GB live, and a heap still growing around it spends the run in GC
@Fork(jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TrieHashingBenchmark {

  private static final int BLOCKS = 64;
  private static final ExecutorService VIRTUAL_POOL = Executors.newVirtualThreadPerTaskExecutor();
  private static final NodeUpdater DISCARD = (location, hash, value) -> {};

  @Param({"1000000"})
  public int accounts;

  @Param({"1000"})
  public int touchedAccounts;

  private final Map<Bytes32, Bytes> nodes = new ConcurrentHashMap<>();
  private Bytes32 rootHash;
  private Bytes32[][] blockKeys;
  private Bytes[][] blockValues;
  private int block;

  @Setup(Level.Trial)
  public void setUp() {
    final Random random = new Random(42);
    final Bytes32[] keys = new Bytes32[accounts];
    final MerkleTrie<Bytes, Bytes> trie =
        new ParallelStoredMerklePatriciaTrie<>(
            this::loadNode,
            Function.identity(),
            Function.identity(),
            BlockProcessingExecutors.accountTrieForkJoinPool());
    for (int i = 0; i < accounts; i++) {
      keys[i] = Bytes32.random(random);
      trie.put(keys[i], account(random));
    }
    trie.commit((location, hash, value) -> nodes.put(hash, value));
    rootHash = trie.getRootHash();

    blockKeys = new Bytes32[BLOCKS][touchedAccounts];
    blockValues = new Bytes[BLOCKS][touchedAccounts];
    for (int b = 0; b < BLOCKS; b++) {
      // distinct: stateRoot merges a repeated key, but preloadWalks would walk it twice
      final int[] picks = random.ints(0, accounts).distinct().limit(touchedAccounts).toArray();
      for (int i = 0; i < touchedAccounts; i++) {
        blockKeys[b][i] = keys[picks[i]];
        blockValues[b][i] = account(random);
      }
    }
  }

  @Benchmark
  public Bytes32 stateRoot() {
    final int b = nextBlock();
    final MerkleTrie<Bytes, Bytes> trie =
        new ParallelStoredMerklePatriciaTrie<>(
            this::loadNode,
            rootHash,
            Function.identity(),
            Function.identity(),
            BlockProcessingExecutors.accountTrieForkJoinPool());
    for (int i = 0; i < touchedAccounts; i++) {
      trie.put(blockKeys[b][i], blockValues[b][i]);
    }
    trie.commit(DISCARD);
    return trie.getRootHash();
  }

  @Benchmark
  public void preloadWalks(final Blackhole bh) {
    final Bytes32[] keys = blockKeys[nextBlock()];
    final CompletableFuture<?>[] walks = new CompletableFuture<?>[keys.length];
    for (int i = 0; i < keys.length; i++) {
      final Bytes32 key = keys[i];
      walks[i] = CompletableFuture.supplyAsync(() -> walk(key), VIRTUAL_POOL);
    }
    for (final CompletableFuture<?> walk : walks) {
      bh.consume(walk.join());
    }
  }

  private Optional<Bytes> walk(final Bytes32 key) {
    return new StoredMerklePatriciaTrie<Bytes, Bytes>(
            this::loadVerifiedNode, rootHash, Function.identity(), Function.identity())
        .get(key);
  }

  private int nextBlock() {
    block = (block + 1) % BLOCKS;
    return block;
  }

  private Optional<Bytes> loadNode(final Bytes location, final Bytes32 hash) {
    return Optional.ofNullable(nodes.get(hash));
  }

  // the check BonsaiWorldStateKeyValueStorage.getTrieNode makes; a failed one aborts the walk
  private Optional<Bytes> loadVerifiedNode(final Bytes location, final Bytes32 hash) {
    return loadNode(location, hash).filter(node -> Hash.hash(node).getBytes().equals(hash));
  }

  private static Bytes account(final Random random) {
    final PatriciaTrieAccountValue account =
        new PatriciaTrieAccountValue(
            random.nextInt(1_000),
            Wei.of(random.nextLong(Long.MAX_VALUE)),
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY);
    return RLP.encode(account::writeTo);
  }
}
