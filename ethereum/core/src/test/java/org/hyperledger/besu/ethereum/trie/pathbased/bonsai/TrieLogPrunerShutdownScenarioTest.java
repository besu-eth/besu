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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogPruner;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogEvent;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Replays the production shutdown race around {@code engine_newPayload} + trie-log observers.
 *
 * <p>Persisting a frozen worldstate must not leave in-memory head half-updated when an observer
 * fails (historically {@link TrieLogPruner} on a stopped {@link EthScheduler}), otherwise a retry
 * can cascade into {@code MerkleTrieException} / worldstate heal.
 */
@ExtendWith(MockitoExtension.class)
public class TrieLogPrunerShutdownScenarioTest extends AbstractIsolationTests {

  private final Address testAddress = Address.fromHexString("0xdeadbeef");

  @Test
  public void buggyObserver_shutdownDuringNewPayloadPersist_doesNotLeaveWorldStateDirty() {
    // Simulates a failing trielog observer (pre-fix TrieLogPruner on stopped EthScheduler).
    archive
        .getTrieLogManager()
        .subscribe(
            (TrieLogEvent event) -> {
              throw new RejectedExecutionException(
                  "Task rejected from ThreadPoolExecutor[Terminated, pool size = 0]");
            });

    final Hash genesisHash = genesisState.getBlock().getHash();
    assertThat(
            ((BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage).getWorldStateBlockHash())
        .contains(genesisHash);

    // newPayload path: frozen worldstate (shouldUpdateHead=false)
    final BonsaiWorldState frozen =
        (BonsaiWorldState)
            archive
                .getWorldState(
                    withBlockHeaderAndNoUpdateNodeHead(genesisState.getBlock().getHeader()))
                .orElseThrow();
    assertThat(frozen.isStorageFrozen()).isTrue();

    final Block block = forTransactions(List.of(burnTransaction(sender1, 0L, testAddress)));

    // Persist still fails because the observer throws...
    final BlockProcessingResult result = processBlockOnly(frozen, block);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.causedBy()).isPresent();
    assertThat(result.causedBy().get())
        .isInstanceOf(RejectedExecutionException.class)
        .hasMessageContaining("Terminated");

    // ...but in-memory head must stay consistent (no half-updated worldstate).
    assertThat(
            ((BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage).getWorldStateBlockHash())
        .contains(genesisHash);
    assertThat(frozen.getWorldStateBlockHash()).isEqualTo(genesisHash);
    assertThat(frozen.getWorldStateRootHash())
        .isEqualTo(genesisState.getBlock().getHeader().getStateRoot());

    // Retry on the same object must not cascade into MerkleTrieException / heal.
    final BlockProcessingResult retry = processBlockOnly(frozen, block);
    assertThat(retry.isSuccessful()).isFalse();
    assertThat(retry.causedBy()).isPresent();
    assertThat(retry.causedBy().get()).isInstanceOf(RejectedExecutionException.class);
  }

  @Test
  public void fixedPruner_stoppedEthScheduler_newPayloadPersistSucceeds() {
    final EthScheduler scheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
    scheduler.stop();

    final TrieLogPruner pruner =
        new TrieLogPruner(
            (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage,
            blockchain,
            scheduler::executeServiceTask,
            512,
            1,
            false,
            new NoOpMetricsSystem());
    archive.getTrieLogManager().subscribe(pruner);

    final BonsaiWorldState frozen =
        (BonsaiWorldState)
            archive
                .getWorldState(
                    withBlockHeaderAndNoUpdateNodeHead(genesisState.getBlock().getHeader()))
                .orElseThrow();

    final Block block = forTransactions(List.of(burnTransaction(sender1, 0L, testAddress)));
    final BlockProcessingResult result = processBlockOnly(frozen, block);

    // With the fix, pruning rejection is swallowed → persist / newPayload complete normally
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.causedBy()).isEmpty();

    // Parent composed head still not advanced by frozen persist
    assertThat(
            ((BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage).getWorldStateBlockHash())
        .contains(genesisState.getBlock().getHash());

    // Trie log for the payload was still saved (newPayload's real side effect)
    assertThat(
            ((BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage)
                .getTrieLog(block.getHash()))
        .isPresent();
  }

  private BlockProcessingResult processBlockOnly(
      final MutableWorldState worldState, final Block block) {
    return protocolSchedule
        .getByBlockHeader(block.getHeader())
        .getBlockProcessor()
        .processBlock(protocolContext, blockchain, worldState, block);
  }
}
