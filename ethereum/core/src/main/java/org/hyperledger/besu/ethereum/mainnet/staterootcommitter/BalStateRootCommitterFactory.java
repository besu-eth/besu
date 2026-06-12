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
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.forest.ForestWorldStateArchive;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BalStateRootCalculator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedLayeredWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class BalStateRootCommitterFactory implements StateRootCommitterFactory {

  private final BalConfiguration balConfiguration;
  private final Executor balAsyncExecutor;

  public BalStateRootCommitterFactory(final BalConfiguration balConfiguration) {
    this(balConfiguration, ForkJoinPool.commonPool());
  }

  public BalStateRootCommitterFactory(
      final BalConfiguration balConfiguration, final Executor balAsyncExecutor) {
    this.balConfiguration = balConfiguration;
    this.balAsyncExecutor = balAsyncExecutor;
  }

  @Override
  public StateRootCommitter forBlock(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final Optional<BlockAccessList> maybeBal) {

    if (maybeBal.isEmpty()
        || !balConfiguration.isBalStateRootEnabled()
        || protocolContext.getWorldStateArchive() instanceof ForestWorldStateArchive
        || isTrieDisabled(protocolContext)) {
      return StateRootCommitter.SYNCHRONOUS;
    }

    final CompletableFuture<BalRootComputation> balFuture =
        BalStateRootCalculator.computeAsync(
            protocolContext, blockHeader, maybeBal.get(), balAsyncExecutor);

    return new BalCommitter(balFuture);
  }

  // The BAL-computed root is the authoritative source. The standard trie
  // computation (supplier) is never invoked.
  private static final class BalCommitter implements StateRootCommitter {

    private final CompletableFuture<BalRootComputation> balFuture;

    BalCommitter(final CompletableFuture<BalRootComputation> balFuture) {
      this.balFuture = balFuture;
    }

    @Override
    public Hash computeRoot(
        final Supplier<Hash> stateRootSupplier,
        final MutableWorldState worldState,
        final WorldStateKeyValueStorage.Updater stateUpdater,
        final BlockHeader blockHeader) {

      final BalRootComputation bal = awaitBal(balFuture);

      if (!blockHeader.getStateRoot().equals(bal.root())) {
        throw new IllegalStateException("BAL-computed root does not match block header state root");
      }

      importBalStateChanges(
          (PathBasedWorldState) worldState,
          (PathBasedWorldStateKeyValueStorage.Updater) stateUpdater,
          bal);
      return bal.root();
    }

    @Override
    public void cancel() {
      balFuture.cancel(true);
    }
  }

  private static void importBalStateChanges(
      final PathBasedWorldState worldState,
      final PathBasedWorldStateKeyValueStorage.Updater stateUpdater,
      final BalRootComputation bal) {

    final PathBasedWorldStateUpdateAccumulator balAccumulator = bal.accumulator();
    final PathBasedWorldStateUpdateAccumulator blockAccumulator = worldState.updater();
    blockAccumulator.importStateChangesFromSource(balAccumulator);

    if (!worldState.isStorageFrozen()) {
      final PathBasedLayeredWorldStateKeyValueStorage balStorage =
          (PathBasedLayeredWorldStateKeyValueStorage) balAccumulator.getWorldStateStorage();
      balStorage.mergeTo(stateUpdater.getWorldStateTransaction());
    }
  }

  private static BalRootComputation awaitBal(final CompletableFuture<BalRootComputation> future) {
    try {
      return future.get();
    } catch (final ExecutionException e) {
      throw new IllegalStateException("Failed to compute BAL state root", e.getCause());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for BAL state root", e);
    } catch (final CancellationException e) {
      throw new IllegalStateException("BAL state root computation was cancelled", e);
    } catch (final CompletionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof CancellationException ce) {
        throw new IllegalStateException("BAL state root computation was cancelled", ce);
      }
      throw new IllegalStateException(
          "Failed to compute BAL state root", cause != null ? cause : e);
    }
  }

  private static boolean isTrieDisabled(final ProtocolContext protocolContext) {
    return protocolContext.getWorldStateArchive() instanceof PathBasedWorldStateProvider provider
        && provider.getWorldStateSharedSpec().isTrieDisabled();
  }
}
