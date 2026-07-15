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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiArchiveWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveReadContext;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveReadFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiArchiveWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveWorldStateProvider extends BonsaiWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveWorldStateProvider.class);

  private final BonsaiWorldStateKeyValueStorage archiveReadStorage;
  private final CodeCache codeCache;
  private final WorldStateConfig archiveWorldStateConfig;
  private final boolean stateProofsEnabled;
  private final long trieNodeCheckpointInterval;
  private volatile LongSupplier archiveMigrationProgressSupplier = () -> -1L;

  // Set for the duration of an eth_getProof: the accounts whose storage tries persist() must
  // materialise (the proved account when storage keys were requested; empty for an account-only
  // proof). Null when no proof is in flight — the rolled world state is then fully materialised, as
  // required by historical eth_call/eth_getBalance/traces that also roll via this provider.
  private static final ThreadLocal<Set<Address>> proofStorageRebuildAccounts = new ThreadLocal<>();

  public BonsaiArchiveWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final DataStorageConfiguration dataStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final CodeCache codeCache,
      final MetricsSystem metricsSystem) {
    super(
        worldStateKeyValueStorage,
        blockchain,
        dataStorageConfiguration.getPathBasedExtraStorageConfiguration(),
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        worldStateHealerSupplier,
        codeCache);
    this.codeCache = codeCache;
    this.archiveWorldStateConfig =
        WorldStateConfig.newBuilder(worldStateConfig).trieDisabled(true).build();
    this.stateProofsEnabled =
        dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getStateProofsEnabled();
    this.trieNodeCheckpointInterval =
        dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getArchiveTrieNodeCheckpointInterval();
    final BonsaiArchiveReadFlatDbStrategyProvider archiveProvider =
        new BonsaiArchiveReadFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    archiveProvider.loadFlatDbStrategy(worldStateKeyValueStorage.getComposedWorldStateStorage());
    this.archiveReadStorage =
        new BonsaiWorldStateKeyValueStorage(
            archiveProvider,
            worldStateKeyValueStorage.getComposedWorldStateStorage(),
            worldStateKeyValueStorage.getTrieLogStorage(),
            worldStateKeyValueStorage.getCacheManager(),
            worldStateKeyValueStorage.getCurrentVersion(),
            stateProofsEnabled
                ? new BonsaiArchiveTrieNodeStrategy(null) // reads archive; migrator owns all writes
                : new BonsaiTrieNodeStrategy());
  }

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    if (isHistoricalQuery(queryParams)) {
      if (stateProofsEnabled) {
        final Optional<BlockHeader> checkpointBlock =
            getCheckpointStateStartBlock(queryParams.getBlockHeader().getBlockHash());
        // Only use the archive proof path if the migration has fully processed the checkpoint
        // block. If migration is still behind the checkpoint, the archive flat-DB and trie-node
        // CFs won't have checkpoint data yet, causing spurious "nonces differ" failures.
        if (checkpointBlock.isPresent()
            && archiveMigrationProgressSupplier.getAsLong() >= checkpointBlock.get().getNumber()) {
          LOG.debug(
              "Returning archive proof state for block {} via checkpoint {}",
              queryParams.getBlockHeader().getNumber(),
              checkpointBlock.get().getNumber());
          return rollArchiveProofWorldStateToBlockHash(
                  newArchiveProofWorldState(worldStateConfig),
                  checkpointBlock.get(),
                  queryParams.getBlockHeader().getBlockHash())
              .map(MutableWorldState::freezeStorage);
        }
      }
      LOG.debug(
          "Returning archive state without verifying state root for block {}",
          queryParams.getBlockHeader().getNumber());
      return rollMutableArchiveStateToBlockHash(
          newFrozenArchiveWorldState(archiveWorldStateConfig),
          queryParams.getBlockHeader().getBlockHash());
    }
    return super.getWorldState(queryParams);
  }

  @Override
  public <U> Optional<U> getAccountProof(
      final org.hyperledger.besu.plugin.data.BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper) {
    // The archive read contexts (block-number key suffixes) are constant for the whole proof.
    // Open a proof-scoped memo so they're resolved from storage once rather than on every
    // trie-node and flat-DB read.
    //
    // Also reuse a single near-seek cursor per archive column family for the WHOLE proof. The base
    // getAccountProof only scopes the final proof traversal, but the bulk of the seekForPrev reads
    // happen earlier in getWorldState() — the roll-forward/backward across the checkpoint window and
    // the persist() that rebuilds the historical trie. Without a scope covering those, every
    // getNearestBefore there opens and closes its own RocksDB iterator (a superversion pin plus SST
    // metadata snapshot per call), which dominates proof latency on busy windows. Opening the scope
    // here on the shared archive storage makes the nested scope in super.getAccountProof a no-op.
    //
    // Record which storage tries persist() actually needs to rebuild for this proof: only the
    // proved account, and only when storage keys were requested (an account-only proof needs no
    // storage trie at all). The roll's resetStorageRootsToCheckpointForArchiveProof then skips
    // rebuilding every other changed account's storage trie — the dominant persist() cost on busy
    // windows. Cleared in the finally so a later non-proof roll on this (pooled) thread rebuilds
    // fully.
    proofStorageRebuildAccounts.set(
        accountStorageKeys.isEmpty() ? Set.of() : Set.of(accountAddress));
    try (var seekScope = archiveReadStorage.getComposedWorldStateStorage().openNearestSeekScope();
        var ignored = BonsaiArchiveReadContext.open()) {
      return super.getAccountProof(blockHeader, accountAddress, accountStorageKeys, mapper);
    } finally {
      proofStorageRebuildAccounts.remove();
    }
  }

  private BonsaiWorldState newFrozenArchiveWorldState(final WorldStateConfig config) {
    final BonsaiWorldState worldState =
        new BonsaiArchiveWorldState(this, archiveReadStorage, evmConfiguration, config, codeCache);
    // Freeze before persisting to ensure the historical block number does not affect the database
    worldState.freezeStorage();
    return worldState;
  }

  /**
   * Builds a world state for archive proof generation, backed by an in-memory {@link
   * BonsaiArchiveWorldStateLayerStorage} layer over the (read-only) archive storage. It is
   * deliberately NOT frozen: the subsequent {@code rollArchiveProofWorldStateToBlockHash} persist
   * must be able to write the rolled trie nodes into the layer (plain trie-branch keys) so the
   * proof can read the historical state. The writes land only in the discardable in-memory layer,
   * never the persistent archive CF.
   */
  private BonsaiWorldState newArchiveProofWorldState(final WorldStateConfig config) {
    final BonsaiArchiveWorldStateLayerStorage layerStorage =
        new BonsaiArchiveWorldStateLayerStorage(archiveReadStorage);
    return new BonsaiArchiveWorldState(this, layerStorage, evmConfiguration, config, codeCache);
  }

  private Optional<BlockHeader> getCheckpointStateStartBlock(final Hash targetHash) {
    return blockchain
        .getBlockHeader(targetHash)
        .map(BlockHeader::getNumber)
        .flatMap(
            targetNumber -> {
              final long ceilingCheckpoint =
                  (((targetNumber + trieNodeCheckpointInterval) / trieNodeCheckpointInterval)
                          * trieNodeCheckpointInterval)
                      - 1;
              final long floorCheckpoint =
                  (targetNumber / trieNodeCheckpointInterval) * trieNodeCheckpointInterval - 1;
              final long chosenCheckpoint;
              if (floorCheckpoint >= 0) {
                final long distanceToCeiling = ceilingCheckpoint - targetNumber;
                final long distanceToFloor = targetNumber - floorCheckpoint;
                chosenCheckpoint =
                    distanceToFloor <= distanceToCeiling ? floorCheckpoint : ceilingCheckpoint;
              } else {
                chosenCheckpoint = ceilingCheckpoint;
              }
              return blockchain
                  .getBlockHeaderSafe(chosenCheckpoint)
                  .or(() -> blockchain.getBlockHeaderSafe(blockchain.getChainHeadHash()));
            });
  }

  private Optional<MutableWorldState> rollArchiveProofWorldStateToBlockHash(
      final PathBasedWorldState mutableState,
      final BlockHeader checkpointBlock,
      final Hash targetBlockHash) {

    ((BonsaiWorldState) mutableState).resetWorldStateToCheckpoint(checkpointBlock);

    // The migrator archives checkpoint block C's trie nodes at suffix = the start of the window
    // that ends at C, i.e. (C / interval) * interval. Pin that suffix as the trie-node read
    // context under ARCHIVE_PROOF_BLOCK_NUMBER_KEY for the lifetime of this proof world state.
    // Without it, a roll-forward target in the window after C would read with context = target
    // block number, which resolves to the NEXT checkpoint's nodes (suffix C+1) wherever they
    // exist, shadowing checkpoint C's trie and failing proof traversal with "Unable to load trie
    // node value". WORLD_BLOCK_NUMBER_KEY is left at the block number so flat account/storage
    // reads keep their per-block semantics.
    final long checkpointWindowStart =
        (checkpointBlock.getNumber() / trieNodeCheckpointInterval) * trieNodeCheckpointInterval;
    final SegmentedKeyValueStorageTransaction trieContextTx =
        mutableState.getWorldStateStorage().getComposedWorldStateStorage().startTransaction();
    trieContextTx.put(
        TRIE_BRANCH_STORAGE,
        ARCHIVE_PROOF_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(checkpointWindowStart).toArrayUnsafe());
    trieContextTx.commit();

    if (targetBlockHash.equals(mutableState.blockHash())) {
      return Optional.of(mutableState);
    }

    try {
      final BlockHeader targetHeader =
          blockchain
              .getBlockHeaderSafe(targetBlockHash)
              .orElseThrow(
                  () ->
                      new MerkleTrieException("target block header not found: " + targetBlockHash));

      final PathBasedWorldStateUpdateAccumulator<?> diffBasedUpdater =
          (PathBasedWorldStateUpdateAccumulator<?>) mutableState.updater();
      // Only the account(s) this proof needs a storage trie for must have their storage rolled;
      // rolling every changed account's storage would read (then discard) the slots of the whole
      // window. Null outside a proof so historical eth_call/eth_getBalance still roll full storage.
      diffBasedUpdater.setArchiveProofStorageRollFilter(proofStorageRebuildAccounts.get());
      // Timing breakdown for the proof roll (DEBUG-only diagnostics): time spent loading and
      // deserializing each block's trie log vs. applying it to the accumulator vs. persist().
      long trieLogLoadNanos = 0L;
      long applyNanos = 0L;
      int rollSteps = 0;
      try {
        if (checkpointBlock.getNumber() < targetHeader.getNumber()) {
          // Roll forward: checkpoint is before target; apply each block's trie log in sequence.
          for (long blockNum = checkpointBlock.getNumber() + 1;
              blockNum <= targetHeader.getNumber();
              blockNum++) {
            final long n = blockNum;
            final Hash blockHash =
                blockchain
                    .getBlockHeader(blockNum)
                    .orElseThrow(() -> new MerkleTrieException("missing block header at " + n))
                    .getBlockHash();
            LOG.debug("Roll forward {}", blockHash);
            final long loadStart = System.nanoTime();
            final TrieLog trieLog =
                trieLogManager
                    .getTrieLogLayer(blockHash)
                    .orElseThrow(
                        () -> new MerkleTrieException("missing trie log for " + blockHash));
            final long applyStart = System.nanoTime();
            diffBasedUpdater.rollForward(trieLog);
            trieLogLoadNanos += applyStart - loadStart;
            applyNanos += System.nanoTime() - applyStart;
            rollSteps++;
          }
        } else {
          // Roll backward: checkpoint is after target; existing path.
          final Optional<BlockHeader> maybePersistedHeader =
              blockchain.getBlockHeaderSafe(mutableState.blockHash()).map(BlockHeader.class::cast);

          final List<TrieLog> rollBacks = new ArrayList<>();
          final long loadStart = System.nanoTime();
          if (maybePersistedHeader.isEmpty()) {
            trieLogManager.getTrieLogLayer(mutableState.blockHash()).ifPresent(rollBacks::add);
          } else {
            BlockHeader persistedHeader = maybePersistedHeader.get();
            Hash persistedBlockHash = persistedHeader.getBlockHash();
            while (persistedHeader.getNumber() > targetHeader.getNumber()) {
              LOG.debug("Rollback {}", persistedBlockHash);
              final Hash blockHashForLog = persistedBlockHash;
              rollBacks.add(
                  trieLogManager
                      .getTrieLogLayer(persistedBlockHash)
                      .orElseThrow(
                          () ->
                              new MerkleTrieException("missing trie log for " + blockHashForLog)));
              final Hash parentHash = persistedHeader.getParentHash();
              persistedHeader =
                  blockchain
                      .getBlockHeaderSafe(parentHash)
                      .orElseThrow(
                          () -> new MerkleTrieException("missing parent header for " + parentHash));
              persistedBlockHash = persistedHeader.getBlockHash();
            }
          }
          final long applyStart = System.nanoTime();
          for (final TrieLog rollBack : rollBacks) {
            LOG.debug("Attempting rollback of {}", rollBack.getBlockHash());
            diffBasedUpdater.rollBack(rollBack);
          }
          trieLogLoadNanos += applyStart - loadStart;
          applyNanos += System.nanoTime() - applyStart;
          rollSteps += rollBacks.size();
        }

        // After rolling in either direction the accumulator's updated.storageRoot is the
        // target block's storage root, whose trie nodes are NOT in the archive CF. Reset
        // each account's storageRoot to its prior (checkpoint) value so persist() rebuilds
        // the storage trie from the checkpoint's archived nodes and derives the target root
        // from the slot diffs in storagesToUpdate.
        if (diffBasedUpdater instanceof BonsaiWorldStateUpdateAccumulator bonsaiUpdater) {
          // proofStorageRebuildAccounts is null outside an eth_getProof, which rebuilds every
          // storage trie (full materialisation for historical eth_call/eth_getBalance/traces).
          bonsaiUpdater.resetStorageRootsToCheckpointForArchiveProof(
              proofStorageRebuildAccounts.get());
        }
        diffBasedUpdater.commit();

        final long persistStart = System.nanoTime();
        mutableState.persist(targetHeader);
        final long persistNanos = System.nanoTime() - persistStart;

        LOG.debug(
            "Archive proof roll timing block {}: steps={} trieLogLoad={}ms apply={}ms persist={}ms",
            targetHeader.getNumber(),
            rollSteps,
            trieLogLoadNanos / 1_000_000L,
            applyNanos / 1_000_000L,
            persistNanos / 1_000_000L);

        return Optional.of(mutableState);
      } catch (final MerkleTrieException re) {
        throw re;
      } catch (final Exception e) {
        diffBasedUpdater.reset();
        LOG.atInfo()
            .setMessage("Archive proof state rolling failed on {} for block hash {}: {}")
            .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
            .addArgument(targetBlockHash)
            .addArgument(e)
            .log();
        return Optional.empty();
      }
    } catch (final RuntimeException re) {
      LOG.info("Archive proof rolling failed for block hash {}", targetBlockHash, re);
      if (re instanceof MerkleTrieException) {
        throw re;
      }
      throw new MerkleTrieException("invalid archive proof rollback for " + targetBlockHash);
    }
  }

  /**
   * Sets the supplier used by {@code isHistoricalQuery} to check the highest block number that has
   * been migrated to Bonsai archive storage.
   *
   * <p>Until this is called, the default supplier returns {@code -1}, which denies all
   * archive-backed historical queries and falls back to trie-log rollback via {@code super}.
   *
   * @param supplier returns the highest block number available in Bonsai archive storage
   */
  public void setArchiveMigrationProgressSupplier(final LongSupplier supplier) {
    this.archiveMigrationProgressSupplier = supplier;
  }

  private boolean isHistoricalQuery(final WorldStateQueryParams queryParams) {
    final long queryBlock = queryParams.getBlockHeader().getNumber();
    return worldStateKeyValueStorage.getFlatDbMode().equals(FlatDbMode.ARCHIVE)
        && !queryParams.shouldWorldStateUpdateHead()
        && blockchain.getChainHeadHeader().getNumber() - queryBlock
            >= trieLogManager.getMaxLayersToLoad()
        && archiveMigrationProgressSupplier.getAsLong() >= queryBlock;
  }

  // Archive-specific rollback behaviour. There is no trie-log roll forward/backward, we just roll
  // back the state root, block hash and block number
  protected Optional<MutableWorldState> rollMutableArchiveStateToBlockHash(
      final PathBasedWorldState mutableState, final Hash blockHash) {
    LOG.trace(
        "Rolling mutable archive world state to block hash {}", blockHash.getBytes().toHexString());
    try {
      // Simply persist the block hash/number and state root for this archive state
      mutableState.persist(blockchain.getBlockHeader(blockHash).get());
      LOG.trace(
          "Archive rolling finished, {} now at {}",
          mutableState.getWorldStateStorage().getClass().getSimpleName(),
          blockHash);
      return Optional.of(mutableState);
    } catch (final MerkleTrieException re) {
      // need to throw to trigger the heal
      throw re;
    } catch (final Exception e) {
      LOG.atInfo()
          .setMessage("State rolling failed on {} for block hash {}: {}")
          .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
          .addArgument(blockHash)
          .addArgument(e)
          .log();
      return Optional.empty();
    }
  }
}
