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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;

public class TrieLogPrunerTest {

  private BonsaiWorldStateKeyValueStorage worldState;
  private Blockchain blockchain;
  private final Consumer<Runnable> executeAsync = Runnable::run;

  @SuppressWarnings("BannedMethod")
  @BeforeEach
  public void setup() {
    Configurator.setLevel(LogManager.getLogger(TrieLogPruner.class).getName(), Level.TRACE);
    worldState = Mockito.mock(BonsaiWorldStateKeyValueStorage.class);
    blockchain = Mockito.mock(Blockchain.class);
    when(worldState.pruneTrieLog(any(Hash.class))).thenReturn(true);
  }

  @Test
  public void initialize_preloads_queue_and_prunes_orphaned_blocks() {
    // Given
    int loadingLimit = 2;
    final BlockDataGenerator generator = new BlockDataGenerator();
    final BlockHeader header1 = generator.header(1);
    final BlockHeader header2 = generator.header(2);
    when(worldState.streamTrieLogKeys(loadingLimit))
        .thenReturn(
            Stream.of(
                header1.getBlockHash().getBytes().toArray(),
                header2.getBlockHash().getBytes().toArray()));
    when(blockchain.getBlockHeader(header1.getBlockHash())).thenReturn(Optional.of(header1));
    when(blockchain.getBlockHeader(header2.getBlockHash())).thenReturn(Optional.empty());

    // When
    TrieLogPruner trieLogPruner =
        new TrieLogPruner(
            worldState, blockchain, executeAsync, 3, loadingLimit, false, new NoOpMetricsSystem());
    trieLogPruner.initialize();

    // Then
    verify(worldState, times(1)).streamTrieLogKeys(2);
    verify(worldState, times(1)).pruneTrieLog(header2.getBlockHash());
  }

  @Test
  public void preloadQueueWithTimeout_handles_timeout_during_streamTrieLogKeys() {
    // Given
    final int timeoutInSeconds = 1;
    final long timeoutInMillis = timeoutInSeconds * 1000;
    final int loadingLimit = 2;
    TrieLogPruner trieLogPruner =
        new TrieLogPruner(
            worldState, blockchain, executeAsync, 3, loadingLimit, false, new NoOpMetricsSystem());

    // Simulate a long-running operation
    when(worldState.streamTrieLogKeys(loadingLimit))
        .thenAnswer(new AnswersWithDelay(timeoutInMillis * 2, invocation -> Stream.empty()));

    // When
    long startTime = System.currentTimeMillis();
    trieLogPruner.preloadQueueWithTimeout(timeoutInSeconds);
    long elapsedTime = System.currentTimeMillis() - startTime;

    // Then
    assertThat(elapsedTime).isLessThan(timeoutInMillis * 2);
  }

  @Test
  public void preloadQueueWithTimeout_handles_timeout_during_getBlockHeader() {
    // Given
    final int timeoutInSeconds = 1;
    final long timeoutInMillis = timeoutInSeconds * 1000;
    TrieLogPruner trieLogPruner = setupPrunerAndFinalizedBlock(3, 1);

    // Simulate a long-running operation
    when(blockchain.getBlockHeader(any(Hash.class)))
        // delay on first invocation, then return empty
        .thenAnswer(new AnswersWithDelay(timeoutInMillis * 2, invocation -> Optional.empty()))
        .thenReturn(Optional.empty());

    // When
    long startTime = System.currentTimeMillis();
    trieLogPruner.preloadQueueWithTimeout(timeoutInSeconds);
    long elapsedTime = System.currentTimeMillis() - startTime;

    // Then
    assertThat(elapsedTime).isLessThan(timeoutInMillis * 2);
    verify(worldState, times(1)).pruneTrieLog(key(1));
    verify(worldState, times(1)).pruneTrieLog(key(2));
  }

  @Test
  public void trieLogs_pruned_in_reverse_order_within_pruning_window() {
    // Given

    // pruning window is below numBlocksToRetain and inside the pruningWindowSize offset.
    final long blocksToRetain = 3;
    final int pruningWindowSize = 2;
    when(blockchain.getChainHeadBlockNumber()).thenReturn(5L);
    when(worldState.pruneTrieLog(any(Hash.class))).thenReturn(true);
    // requireFinalizedBlock = false means this is not a PoS chain
    TrieLogPruner trieLogPruner =
        new TrieLogPruner(
            worldState,
            blockchain,
            executeAsync,
            blocksToRetain,
            pruningWindowSize,
            false,
            new NoOpMetricsSystem());

    trieLogPruner.addToPruneQueue(0, key(0)); // older block outside prune window
    trieLogPruner.addToPruneQueue(1, key(1)); // block inside the prune window
    trieLogPruner.addToPruneQueue(1, key(2)); // same block number (fork)
    trieLogPruner.addToPruneQueue(2, key(3)); // different block inside prune window
    trieLogPruner.addToPruneQueue(3, key(4)); // retained block
    trieLogPruner.addToPruneQueue(4, key(5)); // different retained block
    trieLogPruner.addToPruneQueue(5, key(6)); // another retained block

    // When
    int wasPruned = trieLogPruner.pruneFromQueue();

    // Then
    assertThat(wasPruned).isEqualTo(3);
    InOrder inOrder = Mockito.inOrder(worldState);
    inOrder.verify(worldState, times(1)).pruneTrieLog(key(3));
    inOrder.verify(worldState, times(1)).pruneTrieLog(key(1)); // forks in order
    inOrder.verify(worldState, times(1)).pruneTrieLog(key(2));

    // Subsequent run should add one more block, then prune two oldest remaining keys
    trieLogPruner.addToPruneQueue(6, key(6));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(6L);

    wasPruned = trieLogPruner.pruneFromQueue();

    assertThat(wasPruned).isEqualTo(2);
    inOrder.verify(worldState, times(1)).pruneTrieLog(key(4));
    inOrder.verify(worldState, times(1)).pruneTrieLog(key(0));
  }

  @Test
  public void retain_non_finalized_blocks() {
    // Given
    // finalizedBlockHeight < configuredRetainHeight
    final long finalizedBlockHeight = 1;
    final long configuredRetainHeight = 3;
    TrieLogPruner trieLogPruner =
        setupPrunerAndFinalizedBlock(configuredRetainHeight, finalizedBlockHeight);

    // When
    final int wasPruned = trieLogPruner.pruneFromQueue();

    // Then
    assertThat(wasPruned).isEqualTo(1);
    verify(worldState, times(1)).pruneTrieLog(key(1)); // should prune (finalized)
    verify(worldState, never()).pruneTrieLog(key(2)); // would prune but (NOT finalized)
    verify(worldState, never()).pruneTrieLog(key(3)); // would prune but (NOT finalized)
    verify(worldState, never()).pruneTrieLog(key(4)); // retained block (NOT finalized)
    verify(worldState, never()).pruneTrieLog(key(5)); // chain height (NOT finalized)
  }

  @Test
  public void boundary_test_when_configured_retain_equals_finalized_block() {
    // Given
    // finalizedBlockHeight == configuredRetainHeight
    final long finalizedBlockHeight = 2;
    final long configuredRetainHeight = 2;
    TrieLogPruner trieLogPruner =
        setupPrunerAndFinalizedBlock(configuredRetainHeight, finalizedBlockHeight);

    // When
    final int wasPruned = trieLogPruner.pruneFromQueue();

    // Then
    assertThat(wasPruned).isEqualTo(1);
    verify(worldState, times(1)).pruneTrieLog(key(1)); // should prune (finalized)
    verify(worldState, never()).pruneTrieLog(key(2)); // retained block (finalized)
    verify(worldState, never()).pruneTrieLog(key(3)); // retained block (NOT finalized)
    verify(worldState, never()).pruneTrieLog(key(4)); // retained block (NOT finalized)
    verify(worldState, never()).pruneTrieLog(key(5)); // chain height (NOT finalized)
  }

  @Test
  public void use_configured_retain_when_finalized_block_is_higher() {
    // Given
    // finalizedBlockHeight > configuredRetainHeight
    final long finalizedBlockHeight = 4;
    final long configuredRetainHeight = 3;
    final TrieLogPruner trieLogPruner =
        setupPrunerAndFinalizedBlock(configuredRetainHeight, finalizedBlockHeight);

    // When
    final int wasPruned = trieLogPruner.pruneFromQueue();

    // Then
    assertThat(wasPruned).isEqualTo(2);
    final InOrder inOrder = Mockito.inOrder(worldState);
    inOrder.verify(worldState, times(1)).pruneTrieLog(key(2)); // should prune (finalized)
    inOrder.verify(worldState, times(1)).pruneTrieLog(key(1)); // should prune (finalized)
    verify(worldState, never()).pruneTrieLog(key(3)); // retained block (finalized)
    verify(worldState, never()).pruneTrieLog(key(4)); // retained block (finalized)
    verify(worldState, never()).pruneTrieLog(key(5)); // chain height (NOT finalized)
  }

  @Test
  public void skip_pruning_when_finalized_block_required_but_not_present() {
    // This can occur at the start of PoS chains

    // Given
    when(blockchain.getFinalized()).thenReturn(Optional.empty());
    final long configuredRetainHeight = 2;
    final long chainHeight = 2;
    final long configuredRetainAboveHeight = configuredRetainHeight - 1;
    final long blocksToRetain = chainHeight - configuredRetainAboveHeight;
    final int pruningWindowSize = (int) chainHeight;
    when(blockchain.getChainHeadBlockNumber()).thenReturn(chainHeight);
    TrieLogPruner trieLogPruner =
        new TrieLogPruner(
            worldState,
            blockchain,
            executeAsync,
            blocksToRetain,
            pruningWindowSize,
            true,
            new NoOpMetricsSystem());

    trieLogPruner.addToPruneQueue(1, key(1));
    trieLogPruner.addToPruneQueue(2, key(2));

    // When
    final int wasPruned = trieLogPruner.pruneFromQueue();

    // Then
    assertThat(wasPruned).isEqualTo(0);
    verify(worldState, never()).pruneTrieLog(key(1)); // not finalized
    verify(worldState, never()).pruneTrieLog(key(2)); // not finalized
  }

  @Test
  public void do_not_count_trieLog_when_prune_fails_first_attempt() {
    // Given
    when(worldState.pruneTrieLog(key(2))).thenReturn(false);
    final long finalizedBlockHeight = 4;
    final long configuredRetainHeight = 4;
    final TrieLogPruner trieLogPruner =
        setupPrunerAndFinalizedBlock(configuredRetainHeight, finalizedBlockHeight);

    // When
    final int wasPruned = trieLogPruner.pruneFromQueue();

    // Then
    assertThat(wasPruned).isEqualTo(2);

    // Subsequent run should prune previously skipped trieLog
    when(worldState.pruneTrieLog(key(2))).thenReturn(true);
    assertThat(trieLogPruner.pruneFromQueue()).isEqualTo(1);
  }

  @Test
  public void onTrieLogAdded_should_prune() {
    // Given
    final TriggerableConsumer triggerableConsumer = new TriggerableConsumer();
    TrieLogPruner trieLogPruner =
        new TrieLogPruner(
            worldState, blockchain, triggerableConsumer, 0, 1, false, new NoOpMetricsSystem());
    assertThat(trieLogPruner.pruneFromQueue()).isEqualTo(0);

    final TrieLogLayer layer = new TrieLogLayer();
    layer.setBlockNumber(1L);
    layer.setBlockHash(key(1));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(1L);

    // When
    trieLogPruner.onTrieLogAdded(new TrieLogAddedEvent(layer));
    verify(worldState, never()).pruneTrieLog(key(1));
    triggerableConsumer.run();

    // Then
    verify(worldState, times(1)).pruneTrieLog(key(1));
  }

  @Test
  public void onTrieLogAdded_should_not_prune_when_no_blockNumber() {
    // Given
    TrieLogPruner trieLogPruner =
        new TrieLogPruner(
            worldState, blockchain, executeAsync, 0, 1, false, new NoOpMetricsSystem());
    assertThat(trieLogPruner.pruneFromQueue()).isEqualTo(0);

    final TrieLogLayer layer = new TrieLogLayer();
    layer.setBlockHash(key(1));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(1L);

    // When
    trieLogPruner.onTrieLogAdded(new TrieLogAddedEvent(layer));

    // Then
    verify(worldState, never()).pruneTrieLog(key(1));
  }

  private TrieLogPruner setupPrunerAndFinalizedBlock(
      final long configuredRetainHeight, final long finalizedBlockHeight) {
    final long chainHeight = 5;
    final long configuredRetainAboveHeight = configuredRetainHeight - 1;
    final long blocksToRetain = chainHeight - configuredRetainAboveHeight;
    final int pruningWindowSize = (int) chainHeight;

    final BlockHeader finalizedHeader = new BlockDataGenerator().header(finalizedBlockHeight);
    when(blockchain.getFinalized()).thenReturn(Optional.of(finalizedHeader.getBlockHash()));
    when(blockchain.getBlockHeader(finalizedHeader.getBlockHash()))
        .thenReturn(Optional.of(finalizedHeader));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(chainHeight);
    TrieLogPruner trieLogPruner =
        new TrieLogPruner(
            worldState,
            blockchain,
            executeAsync,
            blocksToRetain,
            pruningWindowSize,
            true,
            new NoOpMetricsSystem());

    trieLogPruner.addToPruneQueue(1, key(1));
    trieLogPruner.addToPruneQueue(2, key(2));
    trieLogPruner.addToPruneQueue(3, key(3));
    trieLogPruner.addToPruneQueue(4, key(4));
    trieLogPruner.addToPruneQueue(5, key(5));

    return trieLogPruner;
  }

  private Hash key(final int k) {
    return Hash.hash(Bytes.of(k));
  }

  private static class TriggerableConsumer implements Consumer<Runnable> {

    private Runnable runnable;

    @Override
    public void accept(final Runnable runnable) {
      this.runnable = runnable;
    }

    public void run() {
      runnable.run();
    }
  }
}
