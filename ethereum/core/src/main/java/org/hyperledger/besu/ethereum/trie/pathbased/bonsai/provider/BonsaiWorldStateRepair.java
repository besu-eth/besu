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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider;

import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitter;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.MissingCodeException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.TrieNodeDecoder;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repairs only dependencies proved to belong to an unchanged live base, never adopts a snapshot.
 */
final class BonsaiWorldStateRepair {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateRepair.class);

  private static final int MAX_TRANSITIONS = 64;
  private static final StateRootCommitter SERIAL_COMMITTER =
      new DefaultStateRootCommitter((worldState, address) -> address.addressHash(), Runnable::run);
  private final BonsaiWorldStateProvider archive;
  private final MissingTrieNodeFetcher fetcher;
  private final WorldStateRepairBudget budget;
  private final Map<Bytes, Patch> patches = new LinkedHashMap<>();

  BonsaiWorldStateRepair(
      final BonsaiWorldStateProvider archive,
      final MissingTrieNodeFetcher fetcher,
      final WorldStateRepairBudget budget) {
    this.archive = archive;
    this.fetcher = fetcher;
    this.budget = budget;
  }

  WorldStateRepairResult repair(final Hash blockHash, final Hash stateRoot) {
    try {
      checkBudget();
      if (!archive.getWorldStateSharedSpec().isStateful()
          || archive.getWorldStateSharedSpec().isTrieDisabled()) {
        return WorldStateRepairResult.UNSUPPORTED;
      }
      final BlockHeader target = header(blockHash);
      if (!target.getStateRoot().equals(stateRoot)) {
        return WorldStateRepairResult.UNAVAILABLE;
      }
      try (Base base = archive.withHeadWorldStateLock(this::capture)) {
        final Map<Hash, TrieLog> route = route(base.header, target, base.finalized);
        while (true) {
          checkBudget();
          try {
            trial(base, target, route);
            return archive.withHeadWorldStateLock(() -> publish(base));
          } catch (final MerkleTrieException missing) {
            stage(base, missing);
          }
        }
      }
    } catch (final Refused refused) {
      LOG.error("Failed to repair world state", refused);
      return refused.result;
    } catch (final MissingCodeException missingCode) {
      // The normal block-processing code healer remains authoritative. It writes live storage,
      // so it must not be called as a side effect of a private trie-node repair trial.
      LOG.error("Missing code while trying to repair world state", missingCode);
      return WorldStateRepairResult.UNSUPPORTED;
    } catch (final RuntimeException failure) {
      LOG.error("Unexpected error while trying to repair world state", failure);
      return WorldStateRepairResult.FAILED;
    }
  }

  private Base capture() {
    checkBudget();
    final Hash baseHash = archive.headWorldState.blockHash();
    final Hash baseRoot = archive.headWorldState.rootHash();
    final BlockHeader baseHeader = header(baseHash);
    if (!baseHeader.getStateRoot().equals(baseRoot)
        || !archive.blockchain.getChainHeadHash().equals(baseHash)) {
      throw new Refused(WorldStateRepairResult.SUPERSEDED);
    }
    final Optional<Hash> finalized = archive.blockchain.getFinalized();
    final BonsaiWorldStateKeyValueStorage live = archive.worldStateKeyValueStorage;
    if (live.getTrieNodeStrategy().getClass() != BonsaiTrieNodeStrategy.class) {
      throw new Refused(WorldStateRepairResult.UNSUPPORTED);
    }
    // Snapshot/layer commits must not warm or invalidate the shared head caches. The ordinary
    // Bonsai strategy has no commit hooks; other strategies require their own safety argument.
    final BonsaiWorldStateKeyValueStorage privateCacheView =
        new BonsaiWorldStateKeyValueStorage(
            live.getFlatDbStrategyProvider(),
            live.getComposedWorldStateStorage(),
            live.getTrieLogStorage(),
            FlatDbCacheManager.NO_OP_CACHE,
            live.getCurrentVersion(),
            new BonsaiTrieNodeStrategy());
    return new Base(
        baseHeader, finalized, new BonsaiSnapshotWorldStateKeyValueStorage(privateCacheView));
  }

  private Map<Hash, TrieLog> route(
      final BlockHeader base, final BlockHeader target, final Optional<Hash> finalized) {
    final long floor = finalized.map(this::header).map(BlockHeader::getNumber).orElse(0L);
    if (target.getNumber() < floor) {
      throw new Refused(WorldStateRepairResult.UNAVAILABLE);
    }
    final Map<Hash, TrieLog> logs = new LinkedHashMap<>();
    BlockHeader left = base;
    BlockHeader right = target;
    while (!left.getHash().equals(right.getHash())) {
      checkBudget();
      if (logs.size() >= MAX_TRANSITIONS || Math.min(left.getNumber(), right.getNumber()) < floor) {
        throw new Refused(WorldStateRepairResult.UNAVAILABLE);
      }
      if (left.getNumber() >= right.getNumber()) {
        logs.put(left.getHash(), log(left.getHash()));
        left = header(left.getParentHash());
      } else {
        logs.put(right.getHash(), log(right.getHash()));
        right = header(right.getParentHash());
      }
    }
    if (left.getNumber() < floor
        || (left.getNumber() == floor
            && finalized.isPresent()
            && !left.getHash().equals(finalized.get()))) {
      throw new Refused(WorldStateRepairResult.UNAVAILABLE);
    }
    return logs;
  }

  private TrieLog log(final Hash hash) {
    checkBudget();
    if (archive.trieLogManager.getTrieLogFactory().getClass() != BonsaiTrieLogFactory.class) {
      throw new Refused(WorldStateRepairResult.UNSUPPORTED);
    }
    final byte[] encoded =
        archive
            .worldStateKeyValueStorage
            .getTrieLog(hash)
            .orElseThrow(() -> new Refused(WorldStateRepairResult.UNAVAILABLE));
    // Reserve conservatively for encoded bytes plus decoded maps/accumulators before decoding.
    // This is admission accounting, not a claim of exact JVM heap usage. The initial storage
    // read materializes one database value; no provider API exposes its size without reading it.
    if (!budget.consumeStagedBytes(16L * encoded.length + 4096L)) {
      checkBudget();
    }
    final TrieLog decoded = archive.trieLogManager.getTrieLogFactory().deserialize(encoded);
    if (!decoded.getBlockHash().equals(hash)) {
      throw new Refused(WorldStateRepairResult.UNAVAILABLE);
    }
    checkBudget();
    return decoded;
  }

  private BlockHeader header(final Hash hash) {
    return archive
        .blockchain
        .getBlockHeader(hash)
        .orElseThrow(() -> new Refused(WorldStateRepairResult.UNAVAILABLE));
  }

  private void trial(final Base base, final BlockHeader target, final Map<Hash, TrieLog> route) {
    if (!budget.consumeTransitions(route.size())) {
      checkBudget();
    }
    try (final BonsaiWorldStateLayerStorage layer = new BudgetedLayer(base.storage)) {
      final BonsaiWorldStateKeyValueStorage.Updater updater = layer.updater();
      patches.forEach(
          (key, patch) ->
              updater.putTrieNode(key, Bytes32.wrap(patch.hash.getBytes()), patch.data));
      updater.commitComposedOnly();
      // Root metadata alone must not mask a missing root; test the patched private view.
      if (layer
          .getTrieNode(Bytes.EMPTY, Bytes32.wrap(base.header.getStateRoot().getBytes()))
          .isEmpty()) {
        throw new MerkleTrieException(
            "Missing live base root",
            Bytes32.wrap(base.header.getStateRoot().getBytes()),
            Bytes.EMPTY);
      }
      final TrieLogManager readOnlyLogs =
          new TrieLogManager(archive.blockchain, layer, MAX_TRANSITIONS, null) {
            @Override
            public Optional<TrieLog> getTrieLogLayer(final Hash hash) {
              checkBudget();
              return Optional.ofNullable(route.get(hash));
            }

            @Override
            public synchronized void saveTrieLog(
                final PathBasedWorldStateUpdateAccumulator<?> accumulator,
                final Hash root,
                final org.hyperledger.besu.plugin.data.BlockHeader block,
                final PathBasedWorldState worldState) {
              // Trial state must not publish shared trie logs, observer events, or cache entries.
            }
          };
      final BonsaiCodeCache codeCache = new BonsaiCodeCache();
      final BonsaiWorldStateProvider candidate =
          new BonsaiWorldStateProvider(
              new NoOpBonsaiWorldStateCacheManager(layer, archive.evmConfiguration, codeCache),
              ImmutableExtraStorageConfiguration.builder()
                  .maxLayersToLoad(MAX_TRANSITIONS + 1L)
                  .parallelStateRootComputationEnabled(false)
                  .build(),
              readOnlyLogs,
              layer,
              archive.blockchain,
              NoOpBonsaiCachedMerkleTrieLoader.INSTANCE,
              archive.evmConfiguration,
              codeCache);
      candidate.headWorldState =
          new BonsaiWorldState(
              candidate,
              layer,
              archive.evmConfiguration,
              WorldStateConfig.newBuilder(candidate.worldStateConfig)
                  .parallelStateRootComputationEnabled(false)
                  .build(),
              codeCache) {
            @Override
            protected StateRootCommitter resolveDefaultCommitter() {
              return (worldState, block, updater) -> {
                checkBudget();
                // DefaultComputation.clearStorage enumerates a whole account storage trie.
                // Such transitions are outside targeted repair, regardless of encoded log size.
                if (!((PathBasedWorldStateUpdateAccumulator<?>) updater)
                    .getStorageToClear()
                    .isEmpty()) {
                  throw new Refused(WorldStateRepairResult.UNSUPPORTED);
                }
                return computeTrialStateRoot(worldState, block, updater);
              };
            }
          };
      if (candidate.getWorldState(withBlockHeaderAndUpdateNodeHead(target)).isEmpty()) {
        throw new Refused(WorldStateRepairResult.UNAVAILABLE);
      }
      checkBudget();
    } catch (final RuntimeException failure) {
      throw failure;
    } catch (final Exception closeFailure) {
      throw new Refused(WorldStateRepairResult.FAILED);
    }
  }

  static StateRootComputation computeTrialStateRoot(
      final MutableWorldState worldState,
      final org.hyperledger.besu.plugin.data.BlockHeader block,
      final WorldUpdater updater) {
    try {
      return SERIAL_COMMITTER.compute(worldState, block, updater);
    } catch (final CompletionException failure) {
      Throwable cause = failure;
      while (cause instanceof CompletionException && cause.getCause() != null) {
        cause = cause.getCause();
      }
      // Even the direct executor reports storage-root failures through future.join(). Restore
      // these typed faults before the candidate provider's generic rolling-failure handling.
      if (cause instanceof MerkleTrieException missingTrieNode) {
        throw missingTrieNode;
      }
      if (cause instanceof MissingCodeException missingCode) {
        throw missingCode;
      }
      if (cause instanceof Refused refused) {
        throw refused;
      }
      throw failure;
    }
  }

  private void stage(final Base base, final MerkleTrieException missing) {
    checkBudget();
    if (missing.getHash() == null
        || missing.getLocation() == null
        || missing.getMaybeAddress() == null) {
      throw new Refused(WorldStateRepairResult.UNAVAILABLE);
    }
    final Bytes location = missing.getLocation();
    final Hash expected = Hash.wrap(missing.getHash());
    // Account-trie failures also carry the address being updated. Classify by proof, not by
    // exception decoration: prefer the account trie, then try that account's storage trie.
    final Optional<Hash> account;
    if (proves(base.storage, base.header.getStateRoot(), Optional.empty(), location, expected)) {
      account = Optional.empty();
    } else {
      account = missing.getMaybeAddress().map(address -> address.addressHash());
      if (account.isEmpty()
          || !proves(base.storage, base.header.getStateRoot(), account, location, expected)) {
        throw new Refused(WorldStateRepairResult.UNAVAILABLE);
      }
    }
    final Bytes key = key(account, location);
    if (patches.containsKey(key)) {
      throw new Refused(WorldStateRepairResult.UNAVAILABLE);
    }
    if (!budget.consumeRepairObject()) {
      checkBudget();
    }
    final Optional<Bytes> local =
        base.storage
            .getTrieNode(key, Bytes32.wrap(expected.getBytes()))
            .or(
                () ->
                    archive
                        .worldStateKeyValueStorage
                        .getCacheManager()
                        .getTrieNode(Bytes32.wrap(expected.getBytes()))
                        .filter(bytes -> Hash.hash(bytes).equals(expected)));
    final Optional<Bytes> response =
        local.isPresent()
            ? local
            : fetcher.fetch(
                new TrieNodeRequest(base.header.getStateRoot(), account, location, expected),
                budget);
    checkBudget();
    final Bytes data =
        response
            .filter(bytes -> Hash.hash(bytes).equals(expected))
            .orElseThrow(() -> new Refused(WorldStateRepairResult.UNAVAILABLE));
    // Decode now, not after publication. No malformed peer object can reach live storage.
    TrieNodeDecoder.decode(location, data);
    if (!budget.consumeStagedBytes(data.size())) {
      checkBudget();
    }
    patches.put(key, new Patch(account, location, expected, data.copy()));
  }

  private WorldStateRepairResult publish(final Base base) {
    checkBudget();
    if (!archive.headWorldState.blockHash().equals(base.header.getHash())
        || !archive.headWorldState.rootHash().equals(base.header.getStateRoot())
        || !archive.blockchain.getChainHeadHash().equals(base.header.getHash())
        || !archive.blockchain.getFinalized().equals(base.finalized)) {
      return WorldStateRepairResult.SUPERSEDED;
    }
    if (patches.isEmpty()) {
      return WorldStateRepairResult.ALREADY_AVAILABLE;
    }
    for (final Patch patch : patches.values()) {
      if (!proves(
          archive.worldStateKeyValueStorage,
          base.header.getStateRoot(),
          patch.account,
          patch.location,
          patch.hash)) {
        return WorldStateRepairResult.SUPERSEDED;
      }
    }
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        archive.worldStateKeyValueStorage.updater();
    try {
      patches.forEach(
          (key, patch) ->
              updater.putTrieNode(key, Bytes32.wrap(patch.hash.getBytes()), patch.data));
      checkBudget();
      updater.commitComposedOnly();
    } catch (final RuntimeException failure) {
      updater.rollback();
      throw failure;
    }
    archive.worldStateCacheManager.reset();
    return WorldStateRepairResult.REPAIRED;
  }

  /** Walks just the requested nibble path under the trusted base root, never the whole trie. */
  private boolean proves(
      final BonsaiWorldStateKeyValueStorage storage,
      final Hash stateRoot,
      final Optional<Hash> account,
      final Bytes wanted,
      final Hash expected) {
    if (wanted.size() > 64) {
      return false;
    }
    Hash root = stateRoot;
    if (account.isPresent()) {
      final StoredMerklePatriciaTrie<Bytes, Bytes> accounts =
          new StoredMerklePatriciaTrie<>(
              (location, hash) -> read(storage, Optional.empty(), location, Hash.wrap(hash)),
              Bytes32.wrap(stateRoot.getBytes()),
              Function.identity(),
              Function.identity());
      final Optional<Bytes> value = accounts.get(account.get().getBytes());
      if (value.isEmpty()) {
        return false;
      }
      root = PatriciaTrieAccountValue.readFrom(RLP.input(value.get())).getStorageRoot();
    }
    Bytes location = Bytes.EMPTY;
    Hash hash = root;
    for (int depth = 0; depth <= 64; depth++) {
      checkBudget();
      if (location.equals(wanted)) {
        return hash.equals(expected);
      }
      final Optional<Bytes> data = read(storage, account, location, hash);
      if (data.isEmpty()) {
        return false;
      }
      Node<Bytes> next = null;
      for (final Node<Bytes> node : TrieNodeDecoder.decodeNodes(location, data.get())) {
        final Optional<Bytes> childLocation = node.getLocation();
        if (childLocation.isPresent()
            && childLocation.get().size() > location.size()
            && childLocation.get().size() <= wanted.size()
            && wanted.slice(0, childLocation.get().size()).equals(childLocation.get())
            && (next == null
                || childLocation.get().size() > next.getLocation().orElseThrow().size())) {
          next = node;
        }
      }
      if (next == null) {
        return false;
      }
      location = next.getLocation().orElseThrow();
      hash = Hash.wrap(next.getHash());
    }
    return false;
  }

  private Optional<Bytes> read(
      final BonsaiWorldStateKeyValueStorage storage,
      final Optional<Hash> account,
      final Bytes location,
      final Hash hash) {
    final Bytes key = key(account, location);
    final Patch patch = patches.get(key);
    return patch != null && patch.hash.equals(hash)
        ? Optional.of(patch.data)
        : storage.getTrieNode(key, Bytes32.wrap(hash.getBytes()));
  }

  private static Bytes key(final Optional<Hash> account, final Bytes location) {
    return account.map(hash -> TrieNodeKey.of(hash, location)).orElse(location);
  }

  private void checkBudget() {
    if (budget.isCancelled() || Thread.currentThread().isInterrupted()) {
      throw new Refused(WorldStateRepairResult.CANCELLED);
    }
    if (budget.isExhausted()) {
      throw new Refused(WorldStateRepairResult.BUDGET_EXHAUSTED);
    }
  }

  private record Patch(Optional<Hash> account, Bytes location, Hash hash, Bytes data) {}

  /** The only trial storage: all mutations are charged before admission to its private maps. */
  private final class BudgetedLayer extends BonsaiWorldStateLayerStorage {
    private BudgetedLayer(final BonsaiWorldStateKeyValueStorage parent) {
      super(
          new BudgetedStorage(parent.getComposedWorldStateStorage()),
          parent.getTrieLogStorage(),
          parent);
    }
  }

  private final class BudgetedStorage extends LayeredKeyValueStorage {
    private BudgetedStorage(final SegmentedKeyValueStorage parent) {
      super(parent);
    }

    @Override
    public SegmentedKeyValueStorageTransaction startTransaction() {
      checkBudget();
      final SegmentedKeyValueStorageTransaction delegate = super.startTransaction();
      return new SegmentedKeyValueStorageTransaction() {
        @Override
        public void put(final SegmentIdentifier segment, final byte[] key, final byte[] value) {
          charge((long) key.length + value.length + 256L);
          delegate.put(segment, key, value);
        }

        @Override
        public void remove(final SegmentIdentifier segment, final byte[] key) {
          charge(key.length + 256L);
          delegate.remove(segment, key);
        }

        @Override
        public void commit() {
          checkBudget();
          delegate.commit();
        }

        @Override
        public void rollback() {
          delegate.rollback();
        }

        @Override
        public void close() {
          delegate.close();
        }
      };
    }

    @Override
    public boolean tryDelete(final SegmentIdentifier segment, final byte[] key) {
      charge(key.length + 256L);
      return super.tryDelete(segment, key);
    }

    @Override
    public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segment) {
      throw new Refused(WorldStateRepairResult.UNSUPPORTED);
    }

    @Override
    public Stream<byte[]> streamKeys(final SegmentIdentifier segment) {
      throw new Refused(WorldStateRepairResult.UNSUPPORTED);
    }

    private void charge(final long bytes) {
      checkBudget();
      if (!budget.consumeStagedBytes(bytes)) {
        checkBudget();
      }
    }
  }

  private record Base(
      BlockHeader header, Optional<Hash> finalized, BonsaiSnapshotWorldStateKeyValueStorage storage)
      implements AutoCloseable {
    @Override
    public void close() {
      try {
        storage.close();
      } catch (final Exception failure) {
        throw new Refused(WorldStateRepairResult.FAILED);
      }
    }
  }

  private static final class Refused extends RuntimeException {
    private final WorldStateRepairResult result;

    private Refused(final WorldStateRepairResult result) {
      this.result = result;
    }
  }
}
