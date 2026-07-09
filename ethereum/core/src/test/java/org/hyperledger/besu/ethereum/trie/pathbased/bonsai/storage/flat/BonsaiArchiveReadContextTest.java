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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class BonsaiArchiveReadContextTest {

  @Test
  void resolverRunsEveryCallWhenNoScopeOpen() {
    final AtomicInteger trieCalls = new AtomicInteger();
    final AtomicInteger flatCalls = new AtomicInteger();

    for (int i = 0; i < 3; i++) {
      BonsaiArchiveReadContext.trieReadContext(
          () -> {
            trieCalls.incrementAndGet();
            return Optional.of(new BonsaiContext(10L));
          });
      BonsaiArchiveReadContext.flatReadContext(
          () -> {
            flatCalls.incrementAndGet();
            return Optional.of(new BonsaiContext(20L));
          });
    }

    assertThat(trieCalls.get()).isEqualTo(3);
    assertThat(flatCalls.get()).isEqualTo(3);
  }

  @Test
  void resolverRunsOncePerContextWithinScope() {
    final AtomicInteger trieCalls = new AtomicInteger();
    final AtomicInteger flatCalls = new AtomicInteger();

    try (var ignored = BonsaiArchiveReadContext.open()) {
      Optional<BonsaiContext> trie = Optional.empty();
      Optional<BonsaiContext> flat = Optional.empty();
      for (int i = 0; i < 5; i++) {
        trie =
            BonsaiArchiveReadContext.trieReadContext(
                () -> {
                  trieCalls.incrementAndGet();
                  return Optional.of(new BonsaiContext(10L));
                });
        flat =
            BonsaiArchiveReadContext.flatReadContext(
                () -> {
                  flatCalls.incrementAndGet();
                  return Optional.of(new BonsaiContext(20L));
                });
      }
      // trie and flat are memoized independently and return their own resolved values
      assertThat(trie).map(c -> c.getBlockNumber().orElseThrow()).contains(10L);
      assertThat(flat).map(c -> c.getBlockNumber().orElseThrow()).contains(20L);
    }

    assertThat(trieCalls.get()).isEqualTo(1);
    assertThat(flatCalls.get()).isEqualTo(1);
  }

  @Test
  void resolverRunsAgainAfterScopeCloses() {
    final AtomicInteger calls = new AtomicInteger();

    try (var ignored = BonsaiArchiveReadContext.open()) {
      BonsaiArchiveReadContext.trieReadContext(
          () -> {
            calls.incrementAndGet();
            return Optional.empty();
          });
    }
    // Outside the closed scope resolution happens directly again.
    BonsaiArchiveReadContext.trieReadContext(
        () -> {
          calls.incrementAndGet();
          return Optional.empty();
        });

    assertThat(calls.get()).isEqualTo(2);
  }

  @Test
  void nestedScopeDoesNotResetOuterMemo() {
    final AtomicInteger calls = new AtomicInteger();

    try (var outer = BonsaiArchiveReadContext.open()) {
      BonsaiArchiveReadContext.trieReadContext(
          () -> {
            calls.incrementAndGet();
            return Optional.of(new BonsaiContext(1L));
          });
      // A nested open() is a no-op; closing it must not clear the outer memo.
      try (var inner = BonsaiArchiveReadContext.open()) {
        assertThat(inner).isEqualTo(BonsaiArchiveReadContext.Scope.NO_OP);
      }
      BonsaiArchiveReadContext.trieReadContext(
          () -> {
            calls.incrementAndGet();
            return Optional.of(new BonsaiContext(1L));
          });
    }

    assertThat(calls.get()).isEqualTo(1);
  }
}
