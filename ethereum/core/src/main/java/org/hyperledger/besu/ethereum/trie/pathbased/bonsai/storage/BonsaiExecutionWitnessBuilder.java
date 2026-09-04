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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

/**
 * Builds the EIP-8025 execution witness (state trie nodes, contract codes, and ancestor headers)
 * for a single block from a Bonsai world state and trie log.
 */
public class BonsaiExecutionWitnessBuilder {

  public record Witness(List<String> state, List<String> codes, List<String> headers) {}

  private final PathBasedWorldStateProvider worldStateProvider;
  private final Blockchain blockchain;

  public BonsaiExecutionWitnessBuilder(
      final WorldStateArchive worldStateArchive, final Blockchain blockchain) {
    if (!(worldStateArchive instanceof PathBasedWorldStateProvider pathBasedWorldStateProvider)) {
      throw new IllegalStateException("execution witness requires a PathBasedWorldStateProvider");
    }
    this.worldStateProvider = pathBasedWorldStateProvider;
    this.blockchain = blockchain;
  }

  /**
   * Builds the EIP-8025 execution witness (state trie nodes, codes, headers) for a block. Uses the
   * TrieLog + BAL for {@code state}, the BAL's touched accounts for {@code codes}, and the oldest
   * accessed ancestor in {@code accessedAncestors} for {@code headers}.
   *
   * <p>Codes are derived from the EIP-7928 block access list rather than from instrumented
   * code-read tracking. Touched accounts are a superset of the accounts whose code the EVM actually
   * read, so the resulting witness is expected to over-approximate {@code codes}.
   */
  public Witness buildWitness(
      final BlockHeader blockHeader,
      final BlockAccessList blockAccessList,
      final Map<Long, Hash> accessedAncestors) {

    final TrieLog trieLog =
        worldStateProvider
            .getTrieLogManager()
            .getTrieLogLayer(blockHeader.getHash())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "trie log missing for block " + blockHeader.getHash()));

    final BlockHeader parentHeader = headerByHash(blockHeader.getParentHash());

    try (final MutableWorldState worldState =
        worldStateProvider
            .getWorldState(withBlockHeaderAndNoUpdateNodeHead(parentHeader))
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "parent world state unavailable for " + parentHeader.getHash()))) {

      if (!(worldState instanceof BonsaiWorldState ws)) {
        throw new IllegalStateException("parent world state is not a BonsaiWorldState");
      }
      final List<String> state = buildTrieNodes(blockHeader, trieLog, ws, blockAccessList);
      final List<String> codes = buildCodes(ws, blockAccessList);
      final long oldestAncestor =
          accessedAncestors.keySet().stream()
              .min(Long::compare)
              .orElse(blockHeader.getNumber() - 1);
      final List<String> headers = buildHeaders(oldestAncestor, blockHeader);
      return new Witness(state, codes, headers);
    } catch (final IllegalStateException e) {
      throw e;
    } catch (final Exception e) {
      throw new IllegalStateException(
          "failed to build execution witness for " + blockHeader.getHash(), e);
    }
  }

  /**
   * Collects the trie nodes required to re-execute the block. A throw-away {@link
   * BonsaiWorldStateWitnessStorage} intercepts every trie-node read issued during account/slot
   * access and the subsequent {@code rollForward} + {@code persist}. Returns nodes as sorted hex
   * strings.
   */
  private List<String> buildTrieNodes(
      final BlockHeader blockHeader,
      final TrieLog trieLog,
      final BonsaiWorldState worldView,
      final BlockAccessList blockAccessList) {

    final BonsaiWorldStateWitnessStorage witnessStorage =
        new BonsaiWorldStateWitnessStorage(
            new NoOpMetricsSystem(), worldView.getWorldStateStorage());
    final PathBasedCodeCache codeCache = new PathBasedCodeCache();
    try (final BonsaiWorldState witnessWorldState =
        new BonsaiWorldState(
            witnessStorage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                witnessStorage, EvmConfiguration.DEFAULT, codeCache),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            worldStateProvider.getWorldStateSharedSpec(),
            codeCache)) {

      final BonsaiWorldStateUpdateAccumulator updater =
          (BonsaiWorldStateUpdateAccumulator) witnessWorldState.updater();

      blockAccessList
          .accountChanges()
          .forEach(
              ac -> {
                updater.getAccount(ac.address());
                ac.storageReads()
                    .forEach(
                        sr -> updater.getStorageValueByStorageSlotKey(ac.address(), sr.slot()));
                ac.storageChanges()
                    .forEach(
                        sc -> updater.getStorageValueByStorageSlotKey(ac.address(), sc.slot()));
              });

      updater.rollForward(trieLog);
      updater.commit();
      witnessWorldState.persist(blockHeader);

      return witnessStorage.getTrieNodes().stream().map(Bytes::toHexString).sorted().toList();
    }
  }

  /**
   * Returns the pre-state contract bytecodes for every account the block access list reports as
   * touched, deduplicated and sorted. Empty code is never included.
   *
   * <p>The block access list is the substitute for instrumented code-read tracking: any account
   * whose code the EVM read must have been touched, so this over-approximates the EIP-8025 {@code
   * get_witness_codes} rule — it also picks up accounts the EVM only read or wrote balance/nonce
   * for. It is likewise unfiltered by in-block code writes, which keeps an EIP-7702 authority's
   * pre-state designator in the witness at the cost of carrying some code the verifier could
   * rebuild from the block body.
   */
  @VisibleForTesting
  List<String> buildCodes(final BonsaiWorldState worldView, final BlockAccessList blockAccessList) {
    final Set<String> resultSet = new HashSet<>();
    for (final var accountChanges : blockAccessList.accountChanges()) {
      final Address address = accountChanges.address();
      final var account = worldView.get(address);
      if (account != null && !account.getCodeHash().equals(Hash.EMPTY)) {
        worldView
            .getCode(address, account.getCodeHash())
            .ifPresent(bytes -> resultSet.add(bytes.toHexString()));
      }
    }
    return resultSet.stream().sorted().toList();
  }

  /**
   * Returns RLP-encoded headers for every block from {@code oldestAncestor} up to (but not
   * including) {@code blockNumber} — that is, ending at the parent of the block the witness is
   * being built for, which is not necessarily the chain head. Ordered ascending by block number as
   * required by EIP-8025.
   */
  @VisibleForTesting
  List<String> buildHeaders(final long oldestAncestor, final BlockHeader blockHeader) {
    // The number bounds the walk, the parent hash resolves it: getBlockHeader(long) is
    // canonical-by-height, the wrong ancestry for a block on a fork.
    final Deque<String> result = new ArrayDeque<>();
    Hash hash = blockHeader.getParentHash();
    final long lowerBound = Math.max(0L, oldestAncestor);
    for (long number = blockHeader.getNumber() - 1; number >= lowerBound; number--) {
      final BlockHeader ancestor = headerByHash(hash);
      result.addFirst(RLP.encode(ancestor::writeTo).toHexString()); // addFirst: EIP-8025 ascending
      hash = ancestor.getParentHash();
    }
    return List.copyOf(result);
  }

  private BlockHeader headerByHash(final Hash hash) {
    return blockchain
        .getBlockHeader(hash)
        .orElseThrow(() -> new IllegalStateException("header not found: " + hash));
  }
}
