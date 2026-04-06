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
import org.hyperledger.besu.ethereum.core.MutableWorldState;
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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class BalStateRootCommitterFactory implements StateRootCommitterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(BalStateRootCommitterFactory.class);

  private final BalConfiguration balConfiguration;

  public BalStateRootCommitterFactory(final BalConfiguration balConfiguration) {
    this.balConfiguration = balConfiguration;
  }

  @Override
  public StateRootCommitter forBlock(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final Optional<BlockAccessList> maybeBal) {

    if (maybeBal.isEmpty()
        || protocolContext.getWorldStateArchive() instanceof ForestWorldStateArchive
        || isTrieDisabled(protocolContext)) {
      return StateRootCommitter.SYNCHRONOUS;
    }

    final CompletableFuture<BalRootComputation> balFuture =
        BalStateRootCalculator.computeAsync(protocolContext, blockHeader, maybeBal.get());
    final Duration timeout = balConfiguration.getBalStateRootTimeout();

    if (balConfiguration.isBalStateRootTrusted()) {
      return new TrustedBalCommitter(balFuture, timeout);
    }
    return new VerifyingBalCommitter(
        balFuture, timeout, balConfiguration.isBalLenientOnStateRootMismatch());
  }

  // The BAL-computed root is the authoritative source. The standard trie
  // computation (supplier) is never invoked.
  private static final class TrustedBalCommitter implements StateRootCommitter {

    private final CompletableFuture<BalRootComputation> balFuture;
    private final Duration timeout;

    TrustedBalCommitter(
        final CompletableFuture<BalRootComputation> balFuture, final Duration timeout) {
      this.balFuture = balFuture;
      this.timeout = timeout;
    }

    @Override
    public Hash computeRoot(
        final Supplier<Hash> stateRootSupplier,
        final MutableWorldState worldState,
        final WorldStateKeyValueStorage.Updater stateUpdater,
        final BlockHeader blockHeader) {

      final BalRootComputation bal =
          awaitBal(balFuture, timeout, /* lenient= */ false)
              .orElseThrow(); // unreachable in strict mode

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

  // The standard trie computation is the source of truth. The BAL result
  // is awaited only to cross-check.
  private static final class VerifyingBalCommitter implements StateRootCommitter {

    private final CompletableFuture<BalRootComputation> balFuture;
    private final Duration timeout;
    private final boolean lenient;

    VerifyingBalCommitter(
        final CompletableFuture<BalRootComputation> balFuture,
        final Duration timeout,
        final boolean lenient) {
      this.balFuture = balFuture;
      this.timeout = timeout;
      this.lenient = lenient;
    }

    @Override
    public Hash computeRoot(
        final Supplier<Hash> stateRootSupplier,
        final MutableWorldState worldState,
        final WorldStateKeyValueStorage.Updater stateUpdater,
        final BlockHeader blockHeader) {

      final Hash syncRoot = stateRootSupplier.get();
      final Optional<BalRootComputation> maybeBal = awaitBal(balFuture, timeout, lenient);

      if (maybeBal.isEmpty()) {
        LOG.warn("BAL root unavailable (lenient mode); proceeding with computed root {}", syncRoot);
        return syncRoot;
      }

      final Hash balRoot = maybeBal.get().root();
      if (!syncRoot.equals(balRoot)) {
        final String msg =
            String.format("BAL root mismatch: computed %s vs BAL %s", syncRoot, balRoot);
        if (lenient) {
          LOG.error(msg);
        } else {
          throw new IllegalStateException(msg);
        }
      }
      return syncRoot;
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

  /**
   * Awaits the BAL computation result with the configured timeout. In lenient mode, failures are
   * logged and an empty Optional is returned. In strict mode, failures throw.
   */
  private static Optional<BalRootComputation> awaitBal(
      final CompletableFuture<BalRootComputation> future,
      final Duration timeout,
      final boolean lenient) {
    try {
      if (timeout.isNegative()) {
        return Optional.of(future.join());
      }
      return Optional.of(future.get(timeout.toNanos(), TimeUnit.NANOSECONDS));
    } catch (final TimeoutException e) {
      future.cancel(true);
      return handleFailure(lenient, "Timed out waiting " + timeout + " for BAL state root", e);
    } catch (final ExecutionException e) {
      return handleFailure(lenient, "Failed to compute BAL state root", e.getCause());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return handleFailure(lenient, "Interrupted while waiting for BAL state root", e);
    }
  }

  private static Optional<BalRootComputation> handleFailure(
      final boolean lenient, final String message, final Throwable cause) {
    if (lenient) {
      LOG.warn("{} (lenient mode); proceeding.", message, cause);
      return Optional.empty();
    }
    throw new IllegalStateException(message, cause);
  }

  private static boolean isTrieDisabled(final ProtocolContext protocolContext) {
    return protocolContext.getWorldStateArchive() instanceof PathBasedWorldStateProvider provider
        && provider.getWorldStateSharedSpec().isTrieDisabled();
  }
}
