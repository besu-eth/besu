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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.SyncFixture.WINDOW;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.SyncFixture.checkpoint;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

class SyncAttemptTest {

  private final SyncFixture node = new SyncFixture(23);
  private final TestChain chain = node.chain;

  @Test
  void syncsTheChainAndTheStateWhileTheHeadMoves() {
    chain.extend(12);
    final SyncAttempt attempt = attempt(8);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    node.stateRound();
    chain.extend(4);

    attempt.maybeMove();
    node.runUntil(synced::isDone);

    // four blocks behind the head, the bodies of the window up to it stored
    assertThat(synced).isCompletedWithValue(chain.header(12));
    assertThat(attempt.pivotMoves()).isOne();
    assertThat(attempt.progress()).isEqualTo(new SyncAttempt.Progress(1, 1, 1, 1));
    assertThat(attempt.progressLine())
        .isEqualTo("SNAP2 sync | state 100.0% | tries 100.0% | headers 100.0% | bodies 100.0%");
    assertThat(node.blockchain.getChainHeadHeader()).isEqualTo(chain.header(12));
    assertThat(node.bonsai.getWorldStateRootHash()).contains(chain.world(12).root());
    assertThat(node.bonsai.getWorldStateBlockHash()).contains(chain.header(12).getHash());
    for (long number = 1; number <= 12; number++) {
      assertThat(node.blockchain.getBlockHeader(number)).contains(chain.header(number));
      assertThat(node.blockchain.getBlockBody(chain.header(number).getHash()).isPresent())
          .isEqualTo(number > 12 - WINDOW);
    }
  }

  @Test
  void setsTheChainHeadOnceEveryHeaderIsStored() {
    chain.extend(10);
    final SyncAttempt attempt = attempt(6);
    final CompletableFuture<BlockHeader> synced = attempt.start();

    // the state and the tries, the header download left waiting
    node.runUntil(
        () -> node.bonsai.getWorldStateRootHash().isPresent(),
        () -> {
          node.stateRound();
          node.executor.runAll();
        });

    assertThat(synced).isNotDone();
    assertThat(node.blockchain.getChainHeadBlockNumber()).isZero();
    assertThat(attempt.stage()).isEqualTo(SyncAttempt.Stage.CHAIN);
    assertThat(attempt.progress().headers()).isZero();
    node.runUntil(synced::isDone);
    assertThat(synced).isCompletedWithValue(chain.header(6));
    assertThat(node.blockchain.getChainHeadHeader()).isEqualTo(chain.header(6));
  }

  @Test
  void restartsWhenTheBuiltRootDoesNotMatchThePivot() {
    chain.extend(10);
    final SyncAttempt attempt = attempt(6);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    // the whole state, the final build left waiting
    node.downloadState();
    // the flat state lost an account
    node.storage.write(batch -> batch.removeAccount(chain.world(6).accountLeaves().firstKey()));

    node.runUntil(synced::isDone);

    assertThat(restartCause(synced)).hasMessageContaining("does not match");
    assertThat(node.bonsai.getWorldStateRootHash()).isEmpty();
    assertThat(node.blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void restartsWhenTheStateDownloadFails() {
    chain.extend(12);
    final SyncAttempt attempt = attempt(8);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    node.stateRound();
    chain.extend(4);
    doThrow(new StorageException("disk failure")).when(node.storage).batch();

    attempt.maybeMove();

    assertThat(restartCause(synced)).isNotNull();
  }

  @Test
  void restartsWhenTheChainReorganisesBelowThePivot() {
    chain.extend(12);
    final SyncAttempt attempt = attempt(8);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    chain.reorg(6, 10);

    attempt.maybeMove();

    assertThat(restartCause(synced)).hasMessageContaining("reorganised");
  }

  @Test
  void endsOnceTheHeaderDownloadEndsForGood() {
    chain.extend(10);
    // a checkpoint of another chain at the same height, stored already
    final BlockHeader other =
        new BlockHeaderTestFixture().number(2).difficulty(Difficulty.ZERO).buildHeader();
    node.blockchain.unsafeStoreHeader(other, Difficulty.ZERO);
    final SyncAttempt attempt =
        new SyncAttempt(node.context(Optional.of(checkpoint(2, other.getHash()))), chain.header(6));

    final CompletableFuture<BlockHeader> synced = attempt.start();
    node.runUntil(synced::isDone);

    // before the state download ended
    assertThat(attempt.stage()).isEqualTo(SyncAttempt.Stage.STATE);
    assertThat(synced)
        .failsWithin(Duration.ZERO)
        .withThrowableThat()
        .havingRootCause()
        .isInstanceOf(WrongChainException.class);
  }

  @Test
  void leavesACancelWhileTheStorageTriesFinishToTheBuild() {
    chain.extend(10);
    final SyncAttempt attempt = attempt(6);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    node.downloadState();
    // the next writer is that of the storage tries the final build finishes first
    final List<Optional<WorldStateTrieBuilder>> builds = new ArrayList<>();
    doAnswer(
            invocation -> {
              if (builds.isEmpty()) {
                attempt.cancel();
                builds.add(attempt.trieBuilder());
              }
              return invocation.callRealMethod();
            })
        .when(node.storage)
        .trieWriter();

    node.runUntil(synced::isDone);

    // the build started, so that awaitStop waits for the end of this thread's writes
    assertThat(builds).singleElement().satisfies(build -> assertThat(build).isPresent());
    assertThat(synced)
        .failsWithin(Duration.ZERO)
        .withThrowableThat()
        .havingRootCause()
        .isInstanceOf(CancellationException.class);
    verify(node.storage, never()).saveWorldState(any(), any());
  }

  @Test
  void writesNothingOnceCancelled() throws InterruptedException {
    chain.extend(12);
    final SyncAttempt attempt = attempt(8);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    node.stateRound();

    attempt.cancel();
    attempt.awaitStop();
    clearInvocations(node.storage);
    for (int rounds = 0; rounds < 10; rounds++) {
      node.round();
    }

    assertThat(synced)
        .failsWithin(Duration.ZERO)
        .withThrowableThat()
        .havingRootCause()
        .isInstanceOf(CancellationException.class);
    verify(node.storage, never()).batch();
    verify(node.storage, never()).trieWriter();
    assertThat(node.blockchain.getChainHeadBlockNumber()).isZero();
  }

  private SyncAttempt attempt(final long pivot) {
    return new SyncAttempt(node.context(Optional.empty()), chain.header(pivot));
  }

  private static Throwable restartCause(final CompletableFuture<BlockHeader> synced) {
    assertThat(synced).isCompletedExceptionally();
    final Throwable failure = synced.handle((unused, error) -> error).join();
    return RestartException.in(failure)
        .orElseThrow(() -> new AssertionError("no restart in " + failure));
  }
}
