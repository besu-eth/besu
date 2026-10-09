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
package org.hyperledger.besu.ethereum.trie.benchmark;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.jcajce.provider.digest.Keccak;
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

/** Measures trie reads, updates, root hashing and commits, through the public trie API only. */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
public class MerklePatriciaTrieBenchmark {

  private static final int BATCH_COUNT = 32;

  @Param({"200000"})
  public int trieSize;

  @Param({"1000"})
  public int batchSize;

  private final Map<Bytes32, Bytes> storage = new HashMap<>();
  private final NodeLoader nodeLoader = (location, hash) -> Optional.ofNullable(storage.get(hash));
  private Bytes32 rootHash;
  private Bytes32[][] keyBatches;
  private Bytes[][] valueBatches;
  private int batch;

  @Setup(Level.Trial)
  public void setUp() throws NoSuchAlgorithmException {
    final Random random = new Random(42);
    final Bytes32[] keys = new Bytes32[trieSize];
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(nodeLoader, Function.identity(), Function.identity());
    for (int i = 0; i < trieSize; i++) {
      keys[i] = keccak(Bytes.ofUnsignedInt(i));
      trie.put(keys[i], randomValue(random));
    }
    trie.commit((location, hash, value) -> storage.put(hash, value));
    rootHash = trie.getRootHash();

    // updates mix existing keys (90%) and new keys (10%), like a block touching accounts
    keyBatches = new Bytes32[BATCH_COUNT][batchSize];
    valueBatches = new Bytes[BATCH_COUNT][batchSize];
    for (int b = 0; b < BATCH_COUNT; b++) {
      for (int i = 0; i < batchSize; i++) {
        keyBatches[b][i] =
            random.nextInt(10) == 0
                ? keccak(Bytes.ofUnsignedLong(trieSize + (long) b * batchSize + i))
                : keys[random.nextInt(trieSize)];
        valueBatches[b][i] = randomValue(random);
      }
    }
  }

  private static Bytes32 keccak(final Bytes input) throws NoSuchAlgorithmException {
    final MessageDigest digest = new Keccak.Digest256();
    return Bytes32.wrap(digest.digest(input.toArrayUnsafe()));
  }

  private static Bytes randomValue(final Random random) {
    // size of an RLP encoded account
    final byte[] value = new byte[70 + random.nextInt(10)];
    random.nextBytes(value);
    return Bytes.wrap(value);
  }

  private int nextBatch() {
    batch = (batch + 1) % BATCH_COUNT;
    return batch;
  }

  @Benchmark
  public void get(final Blackhole blackhole) {
    final MerkleTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            nodeLoader, rootHash, Function.identity(), Function.identity());
    for (final Bytes32 key : keyBatches[nextBatch()]) {
      blackhole.consume(trie.get(key));
    }
  }

  @Benchmark
  public void updateAndCommit(final Blackhole blackhole) {
    final MerkleTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            nodeLoader, rootHash, Function.identity(), Function.identity());
    applyAndCommit(trie, blackhole);
  }

  @Benchmark
  public void parallelUpdateAndCommit(final Blackhole blackhole) {
    final MerkleTrie<Bytes32, Bytes> trie =
        new ParallelStoredMerklePatriciaTrie<>(
            nodeLoader, rootHash, Function.identity(), Function.identity());
    applyAndCommit(trie, blackhole);
  }

  @Benchmark
  public Bytes32 inMemoryRootHash() {
    final MerkleTrie<Bytes32, Bytes> trie = new SimpleMerklePatriciaTrie<>(Function.identity());
    final int b = nextBatch();
    final Bytes32[] keys = keyBatches[b];
    final Bytes[] values = valueBatches[b];
    for (int i = 0; i < keys.length; i++) {
      trie.put(keys[i], values[i]);
    }
    return trie.getRootHash();
  }

  private void applyAndCommit(final MerkleTrie<Bytes32, Bytes> trie, final Blackhole blackhole) {
    final int b = nextBatch();
    final Bytes32[] keys = keyBatches[b];
    final Bytes[] values = valueBatches[b];
    for (int i = 0; i < keys.length; i++) {
      trie.put(keys[i], values[i]);
    }
    trie.commit(
        (location, hash, value) -> {
          blackhole.consume(location);
          blackhole.consume(hash);
          blackhole.consume(value);
        });
    blackhole.consume(trie.getRootHash());
  }
}
