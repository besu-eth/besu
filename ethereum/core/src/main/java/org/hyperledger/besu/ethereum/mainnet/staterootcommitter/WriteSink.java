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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Strategy for emitting deferred storage writes, or dropping them when storage is frozen.
 *
 * <p>Each write is exposed as a typed method so the frozen implementation can short-circuit
 * <em>before</em> the caller constructs a deferred-write lambda — preserving the original {@code if
 * (!storageFrozen)} optimization where frozen mode does no per-write work at all. The
 * persist-vs-frozen decision lives in one place (strategy selection) rather than scattered guards.
 */
sealed interface WriteSink permits PersistingSink, FrozenSink {

  void removeAccountInfoState(Hash addressHash);

  void putAccountInfoState(Hash addressHash, Bytes encoded);

  void removeStorageValueBySlotHash(Hash addressHash, Hash slotHash);

  void putStorageValueBySlotHash(Hash addressHash, Hash slotHash, UInt256 value);

  void removeCode(Hash accountHash, Hash priorCodeHash);

  void putCode(Hash accountHash, Hash codeHash, Bytes code);

  /**
   * Persist the pending trie nodes via {@code trie.commit}, mapping each node to a deferred write
   * with {@code writeOf}. When storage is frozen the commit traversal is skipped entirely, since
   * only the root hash (computed independently via {@link MerkleTrie#getRootHash()}) is needed.
   */
  void commitTrie(MerkleTrie<Bytes, Bytes> trie, TrieWriteOf writeOf);

  /** Whether this sink discards writes (i.e. storage is frozen). */
  boolean isFrozen();
}

/** Maps a committed trie node to the deferred write that persists it. */
@FunctionalInterface
interface TrieWriteOf {
  StateRootComputations.UpdaterWrite apply(Bytes location, Bytes32 hash, Bytes value);
}

/** Persisting strategy: routes writes to a lock-free queue. */
record PersistingSink(ConcurrentLinkedQueue<StateRootComputations.UpdaterWrite> writes)
    implements WriteSink {

  @Override
  public void removeAccountInfoState(final Hash addressHash) {
    writes.add(u -> u.removeAccountInfoState(addressHash));
  }

  @Override
  public void putAccountInfoState(final Hash addressHash, final Bytes encoded) {
    writes.add(u -> u.putAccountInfoState(addressHash, encoded));
  }

  @Override
  public void removeStorageValueBySlotHash(final Hash addressHash, final Hash slotHash) {
    writes.add(u -> u.removeStorageValueBySlotHash(addressHash, slotHash));
  }

  @Override
  public void putStorageValueBySlotHash(
      final Hash addressHash, final Hash slotHash, final UInt256 value) {
    writes.add(u -> u.putStorageValueBySlotHash(addressHash, slotHash, value));
  }

  @Override
  public void removeCode(final Hash accountHash, final Hash priorCodeHash) {
    writes.add(u -> u.removeCode(accountHash, priorCodeHash));
  }

  @Override
  public void putCode(final Hash accountHash, final Hash codeHash, final Bytes code) {
    writes.add(u -> u.putCode(accountHash, codeHash, code));
  }

  @Override
  public void commitTrie(final MerkleTrie<Bytes, Bytes> trie, final TrieWriteOf writeOf) {
    trie.commit((location, hash, value) -> writes.add(writeOf.apply(location, hash, value)));
  }

  @Override
  public boolean isFrozen() {
    return false;
  }
}

/** Frozen strategy: drops all writes and skips trie commit traversals. */
record FrozenSink() implements WriteSink {

  @Override
  public void removeAccountInfoState(final Hash addressHash) {
    // no-op: frozen mode discards writes
  }

  @Override
  public void putAccountInfoState(final Hash addressHash, final Bytes encoded) {
    // no-op: frozen mode discards writes
  }

  @Override
  public void removeStorageValueBySlotHash(final Hash addressHash, final Hash slotHash) {
    // no-op: frozen mode discards writes
  }

  @Override
  public void putStorageValueBySlotHash(
      final Hash addressHash, final Hash slotHash, final UInt256 value) {
    // no-op: frozen mode discards writes
  }

  @Override
  public void removeCode(final Hash accountHash, final Hash priorCodeHash) {
    // no-op: frozen mode discards writes
  }

  @Override
  public void putCode(final Hash accountHash, final Hash codeHash, final Bytes code) {
    // no-op: frozen mode discards writes
  }

  @Override
  public void commitTrie(final MerkleTrie<Bytes, Bytes> trie, final TrieWriteOf writeOf) {
    // no-op: frozen mode skips the commit traversal; only the root hash is needed
  }

  @Override
  public boolean isFrozen() {
    return true;
  }
}
