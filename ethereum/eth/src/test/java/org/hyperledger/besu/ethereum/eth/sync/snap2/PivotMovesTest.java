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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.TestTries;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Moves the pivot of a state download that runs on the test thread, the chain answering at once.
 */
class PivotMovesTest {

  private SyncFixture node = new SyncFixture(11);
  private final List<BlockHeader> storedHeaders = new ArrayList<>();
  // how the moves store headers, unless a test makes the writes fail
  private Consumer<List<BlockHeader>> storeHeaders = storedHeaders::addAll;
  private final List<RestartException> restarts = new ArrayList<>();
  private StateSync stateSync;

  @AfterEach
  void stopTheDownload() {
    if (stateSync != null) {
      stateSync.stop();
    }
  }

  @Test
  void movesThePivotOnceTheHeadIsFarEnough() {
    node.chain.extend(7);
    final PivotMoves moves = moves(2);

    moves.maybeMove();
    assertThat(pivot()).isEqualTo(2);

    node.chain.extend(1);
    moves.maybeMove();

    // four blocks behind the head, the headers and lists of the blocks passed stored
    assertThat(pivot()).isEqualTo(4);
    assertThat(storedHeaders).isEqualTo(node.chain.headers(3, 4));
    assertThat(node.blockchain.getBlockAccessList(node.chain.header(3).getHash())).isPresent();
    assertThat(node.blockchain.getBlockAccessList(node.chain.header(4).getHash())).isPresent();
    assertThat(moves.moves()).isOne();
    assertThat(builtRoot()).isEqualTo(node.chain.world(4).root());
  }

  @Test
  void movesAPivotPeersStoppedServing() {
    // enough state for many requests once peers stop serving it
    node = new SyncFixture(5, TestWorld.random(new Random(5), 1500, 4, 800));
    node.chain.extend(7);
    final PivotMoves moves = moves(2);
    // peers serve some of its state, then none of it
    node.stateRound();
    node.network.forget(node.chain.world(2).root());
    node.downloadState();
    assertThat(stateSync.isPivotStale()).isTrue();

    // five blocks ahead, not enough for a move of a pivot still served
    moves.maybeMove();

    assertThat(pivot()).isEqualTo(3);
    assertThat(builtRoot()).isEqualTo(node.chain.world(3).root());
  }

  @Test
  void movesInStepsWhileTheDownloadSendsNothing() {
    node.chain.extend(140);
    stateSync = spy(node.stateSync());
    final PivotMoves moves = startedMoves(2, node.fetcher);

    moves.maybeMove();

    // 128 blocks then the 6 left
    assertThat(pivot()).isEqualTo(136);
    assertThat(moves.moves()).isEqualTo(2);
    assertThat(storedHeaders).isEqualTo(node.chain.headers(3, 136));
    final InOrder order = inOrder(stateSync);
    order.verify(stateSync).pause();
    order.verify(stateSync, times(2)).moveTo(any(), any());
    order.verify(stateSync).resume();
    assertThat(builtRoot()).isEqualTo(node.chain.world(136).root());
  }

  @Test
  void restartsWhenTheChainReorganisesBelowThePivot() {
    node.chain.extend(4);
    final PivotMoves moves = moves(4);
    node.chain.reorg(2, 8);

    moves.maybeMove();

    assertThat(restarts).singleElement().extracting(Throwable::getMessage).asString().contains("4");
    assertThat(pivot()).isEqualTo(4);
    assertThat(storedHeaders).isEmpty();
  }

  @Test
  void restartsWhenAMoveFailsToApply() {
    node.chain.extend(8);
    final PivotMoves moves = moves(2);
    node.downloadState();
    doThrow(new StorageException("disk failure")).when(node.storage).batch();

    moves.maybeMove();

    assertThat(restarts).singleElement().extracting(Throwable::getCause).isNotNull();
    assertThat(moves.moves()).isZero();
  }

  @Test
  void storesTheListsOfAStepBeforeItsHeaders() {
    node.chain.extend(8);
    storeHeaders =
        headers -> {
          throw new StorageException("disk failure");
        };
    final PivotMoves moves = moves(2);

    moves.maybeMove();

    // the headers of the blocks passed failed to store, not their lists
    assertThat(node.blockchain.getBlockAccessList(node.chain.header(3).getHash())).isPresent();
    assertThat(node.blockchain.getBlockAccessList(node.chain.header(4).getHash())).isPresent();
    // tried again at the next check
    assertThat(restarts).isEmpty();
    assertThat(pivot()).isEqualTo(2);
  }

  @Test
  void settlesOnceTheMoveInProgressEnds() {
    node.chain.extend(8);
    final HeaderFetcher fetcher = spy(node.fetcher);
    final CompletableFuture<BlockHeader> head = new CompletableFuture<>();
    doReturn(head).doCallRealMethod().when(fetcher).byHash(any(), any());
    stateSync = node.stateSync();
    final PivotMoves moves = startedMoves(2, fetcher);
    moves.maybeMove();

    final CompletableFuture<Void> settled = moves.settle();

    assertThat(settled).isNotDone();
    // a head too close for a move ends it
    head.complete(node.chain.header(3));
    assertThat(settled).isCompleted();
    // the head is far enough, but the moves ended
    moves.maybeMove();
    assertThat(pivot()).isEqualTo(2);
  }

  @Test
  void catchesUpWithAHeadTooCloseForAMove() {
    node.chain.extend(7);
    final PivotMoves moves = moves(2);

    assertThat(moves.catchUp()).isCompleted();

    assertThat(pivot()).isEqualTo(3);
    assertThat(builtRoot()).isEqualTo(node.chain.world(3).root());
  }

  @Test
  void restartsACatchUpOnAPivotOffTheChainOfTheHead() {
    node.chain.extend(6);
    final PivotMoves moves = moves(5);
    node.chain.reorg(4, 3);

    final CompletableFuture<Void> catchUp = moves.catchUp();

    assertThat(catchUp).isCompletedExceptionally();
    // not tried again
    assertThat(node.waiting).isEmpty();
    assertThat(restarts).hasSize(1);
  }

  @Test
  void triesACatchUpAgainAfterAFailure() {
    node.chain.extend(7);
    final PivotMoves moves = moves(2);
    node.chain
        .sender()
        .fail(message -> message.getCode() == EthProtocolMessages.GET_BLOCK_ACCESS_LISTS);

    final CompletableFuture<Void> catchUp = moves.catchUp();

    assertThat(catchUp).isNotDone();
    node.chain.sender().fail(message -> false);
    node.wakeUp();
    assertThat(catchUp).isCompleted();
    assertThat(pivot()).isEqualTo(3);
  }

  @Test
  void movesNothingOnceStopped() {
    node.chain.extend(8);
    final PivotMoves moves = moves(2);

    moves.stop();
    moves.maybeMove();

    assertThat(moves.catchUp()).isCompletedExceptionally();
    assertThat(pivot()).isEqualTo(2);
    assertThat(storedHeaders).isEmpty();
  }

  // the moves of a download started at the pivot
  private PivotMoves moves(final long pivot) {
    stateSync = node.stateSync();
    return startedMoves(pivot, node.fetcher);
  }

  private PivotMoves startedMoves(final long pivot, final HeaderFetcher fetcher) {
    stateSync.start(node.chain.header(pivot));
    node.tasks.runAll();
    return new PivotMoves(
        stateSync,
        storeHeaders,
        node.blockchain,
        node.chain,
        fetcher,
        node.balFetcher,
        SyncFixture.MOVES,
        restarts::add,
        node.delays);
  }

  private long pivot() {
    return stateSync.pivot().header().getNumber();
  }

  // the root of the state downloaded and moved, once the download ends
  private Bytes32 builtRoot() {
    node.downloadState();
    stateSync.finishStorageTries();
    stateSync.stop();
    return TestTries.build(node.storage, stateSync::storageTrieLeft).root();
  }
}
