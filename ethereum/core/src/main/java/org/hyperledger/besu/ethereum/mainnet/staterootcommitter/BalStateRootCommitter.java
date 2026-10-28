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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.trie.common.StateRootMismatchException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Computes the state root from the BAL in the background; an {@link Engine} builds the tries. */
public final class BalStateRootCommitter implements StateRootCommitter {

  /** Computes the BAL state root on the tries of one trie branch. */
  public interface Engine {

    TrieBranchType getTrieBranchType();

    Result compute(
        BonsaiWorldState parent, BlockAccessListAccountLookup accountLookup, boolean storageFrozen);
  }

  /**
   * Result of the background trie computation.
   *
   * @param computation computed root and deferred KV writes (empty when storage is frozen)
   * @param storageRoots new per-account storage roots, patched into the EVM accumulator by {@link
   *     #compute}
   */
  public record Result(StateRootComputation computation, Map<Address, Hash> storageRoots) {

    static Result empty(final Hash root) {
      return new Result(StateRootComputations.pathBased(root, List.of()), Map.of());
    }
  }

  private final ProtocolContext protocolContext;
  private final BlockHeader blockHeader;
  private final BlockAccessListAccountLookup accountLookup;
  private final boolean storageFrozen;
  private final Engine engine;

  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  // Assigned by start(); null until then. start() must be called before compute()/cancel().
  private CompletableFuture<Result> backgroundComputation;

  public BalStateRootCommitter(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen,
      final Engine engine) {
    this.protocolContext = protocolContext;
    this.blockHeader = blockHeader;
    this.accountLookup = accountLookup;
    this.storageFrozen = storageFrozen;
    this.engine = engine;
  }

  /**
   * Launches the background BAL state root computation and returns this committer.
   *
   * <p>Separated from the constructor so the background thread is not started during object
   * construction — the constructor only captures its arguments, and the asynchronous work (which
   * calls back into this instance) only begins once construction is complete and {@code start()} is
   * invoked. Must be called exactly once before {@link #compute} or {@link #cancel}.
   */
  public BalStateRootCommitter start() {
    this.backgroundComputation =
        CompletableFuture.supplyAsync(
            () -> {
              try (BonsaiWorldState parent =
                  openParentWorldState(protocolContext, blockHeader, accountLookup)) {
                return runComputation(parent, accountLookup, storageFrozen);
              }
            },
            BlockProcessingExecutors.stateRootExecutor());
    return this;
  }

  @Override
  public TrieBranchType getTrieBranchType() {
    return engine.getTrieBranchType();
  }

  /** Cancels the background computation; {@link #compute} will throw if called afterwards. */
  @Override
  public void cancel() {
    cancelled.set(true);
    backgroundComputation.cancel(true);
  }

  /**
   * Waits for the background computation to finish, patches storage roots into the EVM accumulator,
   * and returns the {@link StateRootComputation} carrying the root hash and deferred KV writes.
   *
   * <p>The BAL-computed root is the authoritative source. If it does not match the block header
   * state root, a {@link StateRootMismatchException} is thrown.
   */
  @Override
  public StateRootComputation compute(
      final MutableWorldState worldState,
      final BlockHeader blockHeader,
      final WorldUpdater worldUpdater) {
    final Result result = awaitBackgroundComputation(backgroundComputation);
    final BonsaiWorldStateUpdateAccumulator accumulator =
        (BonsaiWorldStateUpdateAccumulator)
            Objects.requireNonNull(
                worldUpdater, "BAL state root committer requires a non-null WorldUpdater");
    result
        .storageRoots()
        .forEach(
            (address, newStorageRoot) -> {
              final var entry = accumulator.getAccountsToUpdate().get(address);
              if (entry != null && entry.getUpdated() != null) {
                entry.getUpdated().setStorageRoot(newStorageRoot);
              }
            });

    final Hash root = result.computation().root();
    if (blockHeader != null && !root.equals(blockHeader.getStateRoot())) {
      throw new StateRootMismatchException(blockHeader.getStateRoot(), root);
    }
    return result.computation();
  }

  private Result runComputation(
      final BonsaiWorldState worldState,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen) {
    if (accountLookup.isEmpty()) {
      return Result.empty(worldState.getWorldStateRootHash());
    }
    return engine.compute(worldState, accountLookup, storageFrozen);
  }

  private Result awaitBackgroundComputation(final CompletableFuture<Result> future) {
    // a cancelled computation says nothing about the block, the type keeps it from being recorded
    // as bad
    final Result result;
    try {
      result = future.get();
    } catch (final CancellationException e) {
      throw (CancellationException)
          new CancellationException("Background BAL state root computation was cancelled")
              .initCause(e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new IllegalStateException("Background BAL state root computation failed", cause);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while waiting for background BAL state root computation", e);
    }
    if (cancelled.get()) {
      throw new CancellationException("Background BAL state root computation was cancelled");
    }
    return result;
  }

  private BonsaiWorldState openParentWorldState(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final BlockAccessListAccountLookup accountLookup) {
    final Hash parentHash = blockHeader.getParentHash();
    final BlockHeader parentHeader =
        protocolContext
            .getBlockchain()
            .getBlockHeader(parentHash)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Parent %s of block %s not found",
                            parentHash, blockHeader.getBlockHash())));
    final WorldStateQueryParams queryParams =
        WorldStateQueryParams.newBuilder()
            .withBlockHeader(parentHeader)
            .withShouldWorldStateUpdateHead(false)
            // The BAL overlay is attached for consistency with other BAL world-state callers, but
            // this computation path does not read through the world-state accumulator: it replays
            // the BAL directly onto tries/storage (see Engine), so the overlay is not
            // consumed here. Kept so the parent is opened the same way as for EVM execution.
            .withBalOverlay(new BlockAccessListOverlay(accountLookup, Long.MAX_VALUE))
            .build();
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            protocolContext.getWorldStateArchive().getWorldState(queryParams).orElseThrow();
    worldState.disableCacheMerkleTrieLoader();
    return worldState;
  }
}
