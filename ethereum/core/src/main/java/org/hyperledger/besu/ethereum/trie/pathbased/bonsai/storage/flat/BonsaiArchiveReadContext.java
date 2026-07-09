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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * A thread-bound, proof-scoped memo for the archive read contexts (the block-number suffix appended
 * to archive keys). During a single {@code eth_getProof} the trie-node and flat-DB read contexts
 * are constant, yet without this memo they are re-resolved from storage on every trie-node and
 * flat-DB lookup — each resolution being a locked in-memory (or RocksDB) {@code get} plus small
 * allocations. When a scope is open on the current thread the first resolution is cached and reused
 * for the rest of the proof; outside a scope (e.g. normal block processing) resolution happens
 * directly and behaviour is unchanged.
 */
public final class BonsaiArchiveReadContext {

  private BonsaiArchiveReadContext() {}

  /** A thread-bound scope; closing it clears the memo for the current thread. */
  public interface Scope extends AutoCloseable {
    /** A no-op scope, returned for reentrant opens (the outermost scope owns the memo). */
    Scope NO_OP = () -> {};

    @Override
    void close();
  }

  private static final class Memo {
    // null = not yet computed for this proof; present/empty Optional = computed result.
    private Optional<BonsaiContext> trieContext;
    private Optional<BonsaiContext> flatContext;
  }

  // Instance-per-thread memo, installed only for the duration of a proof. Cleared on scope close.
  private static final ThreadLocal<Memo> CURRENT = new ThreadLocal<>();

  /**
   * Opens a proof-scoped read-context memo on the current thread. Must be closed on the same
   * thread, ideally via try-with-resources. Reentrant: nested opens return {@link Scope#NO_OP}.
   *
   * @return the scope to close when the proof completes.
   */
  public static Scope open() {
    if (CURRENT.get() != null) {
      return Scope.NO_OP;
    }
    CURRENT.set(new Memo());
    return CURRENT::remove;
  }

  /**
   * Resolves the trie-node read context, reusing the memoized value when a scope is open.
   *
   * @param resolver computes the context from storage when not already cached.
   * @return the (possibly cached) trie-node read context.
   */
  public static Optional<BonsaiContext> trieReadContext(
      final Supplier<Optional<BonsaiContext>> resolver) {
    final Memo memo = CURRENT.get();
    if (memo == null) {
      return resolver.get();
    }
    if (memo.trieContext == null) {
      memo.trieContext = resolver.get();
    }
    return memo.trieContext;
  }

  /**
   * Resolves the flat-DB read context, reusing the memoized value when a scope is open.
   *
   * @param resolver computes the context from storage when not already cached.
   * @return the (possibly cached) flat-DB read context.
   */
  public static Optional<BonsaiContext> flatReadContext(
      final Supplier<Optional<BonsaiContext>> resolver) {
    final Memo memo = CURRENT.get();
    if (memo == null) {
      return resolver.get();
    }
    if (memo.flatContext == null) {
      memo.flatContext = resolver.get();
    }
    return memo.flatContext;
  }
}
