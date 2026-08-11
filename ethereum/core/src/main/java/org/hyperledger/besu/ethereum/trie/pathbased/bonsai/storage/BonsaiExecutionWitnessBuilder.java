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
import org.hyperledger.besu.ethereum.WitnessData;
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
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;

/**
 * Builds the EIP-8025 execution witness (state trie nodes, contract codes, and ancestor headers)
 * for a single block from a Bonsai world state and trie log. Used by both {@code
 * debug_executionWitness} and reference-test tooling so that both paths emit identical output.
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
   * TrieLog + BAL for {@code state}, the {@link WitnessData}'s accumulated code-read sets for
   * {@code codes}, and the oldest accessed ancestor from {@link WitnessData#accessedAncestors()}
   * for {@code headers}.
   */
  public Witness buildWitness(
      final BlockHeader blockHeader,
      final Optional<BlockAccessList> maybeBlockAccessList,
      final Optional<WitnessData> maybeWitnessData) {
    final TrieLog trieLog =
        worldStateProvider
            .getTrieLogManager()
            .getTrieLogLayer(blockHeader.getHash())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "trie log missing for block " + blockHeader.getHash()));

    final BlockHeader parentHeader =
        blockchain
            .getBlockHeader(blockHeader.getParentHash())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Parent header not found: " + blockHeader.getParentHash()));

    final MutableWorldState worldState =
        worldStateProvider
            .getWorldState(withBlockHeaderAndNoUpdateNodeHead(parentHeader))
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "parent world state unavailable for " + parentHeader.getHash()));

    if (!(worldState instanceof BonsaiWorldState ws)) {
      throw new IllegalStateException("parent world state is not a BonsaiWorldState");
    }

    try (worldState) {
      final List<String> state = buildTrieNodes(blockHeader, trieLog, ws, maybeBlockAccessList);
      // Addresses whose code was written during the block (CREATE, or a 7702 delegation designator
      // set this block). A stateless verifier already reconstructs this code from the block itself,
      // so an execution read that observed the in-block code must not pull the account's pre-state
      // code into the witness — mirroring EELS get_code, which skips reads served from code_writes.
      final Set<Address> inBlockCodeChanged = collectInBlockCodeChanges(maybeBlockAccessList);
      final List<String> codes =
          buildCodes(
              ws,
              maybeWitnessData.map(WitnessData::codeReads).orElse(Set.of()),
              maybeWitnessData.map(WitnessData::authorizationCodeReads).orElse(Set.of()),
              inBlockCodeChanged);
      final Map<Long, Hash> accessedAncestors =
          maybeWitnessData.map(WitnessData::accessedAncestors).orElse(Map.of());
      final long oldestAncestor =
          accessedAncestors.keySet().stream()
              .min(Long::compare)
              .orElse(blockHeader.getNumber() - 1);
      final List<String> headers =
          buildHeaders(blockchain, oldestAncestor, blockHeader.getNumber());
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
      final Optional<BlockAccessList> maybeBal) {

    final BonsaiWorldStateWitnessStorage witnessStorage =
        new BonsaiWorldStateWitnessStorage(
            new NoOpMetricsSystem(), worldView.getWorldStateStorage());
    final PathBasedCodeCache codeCache = new PathBasedCodeCache();
    final BonsaiWorldState witnessWorldState =
        new BonsaiWorldState(
            witnessStorage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                witnessStorage, EvmConfiguration.DEFAULT, codeCache),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            WorldStateConfig.newBuilder().build(),
            codeCache);

    final BonsaiWorldStateUpdateAccumulator updater =
        (BonsaiWorldStateUpdateAccumulator) witnessWorldState.updater();

    // Prefer BAL when present (Amsterdam+) and fall back to TrieLog alone for pre-Amsterdam blocks.
    if (maybeBal.isPresent()) {
      maybeBal
          .get()
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
    } else {
      trieLog
          .getAccountChanges()
          .forEach(
              (address, __) -> {
                updater.getAccount(address);
                trieLog
                    .getStorageChanges(address)
                    .keySet()
                    .forEach(slot -> updater.getStorageValueByStorageSlotKey(address, slot));
              });
    }

    updater.rollForward(trieLog);
    updater.commit();
    witnessWorldState.persist(blockHeader);

    return witnessStorage.getTrieNodes().stream().map(Bytes::toHexString).sorted().toList();
  }

  /**
   * Returns the RLP-encoded pre-state contract bytecodes required by a stateless verifier,
   * deduplicated and sorted, implementing the EIP-8025 {@code get_witness_codes} rule.
   *
   * <p>{@code preStateAddresses} (EIP-7702 authority reads) are always included — the same
   * transaction that reads the authority's pre-state code also writes new code to it, so the
   * verifier needs the old version even though the address appears in {@code inBlockCodeChanged}.
   * {@code executionAddresses} are filtered: if the code was written in-block the verifier already
   * has it from {@code code_writes}. Empty code is never included. Lookups run in parallel.
   */
  private List<String> buildCodes(
      final BonsaiWorldState worldView,
      final Set<Address> executionAddresses,
      final Set<Address> preStateAddresses,
      final Set<Address> inBlockCodeChanged) {
    final var resultSet = ConcurrentHashMap.<String>newKeySet();
    java.util.stream.Stream.concat(
            preStateAddresses.stream(),
            executionAddresses.stream().filter(a -> !inBlockCodeChanged.contains(a)))
        .distinct()
        .parallel()
        .forEach(
            address -> {
              final var account = worldView.get(address);
              if (account != null && !account.getCodeHash().equals(Hash.EMPTY)) {
                worldView
                    .getCode(address, account.getCodeHash())
                    .ifPresent(bytes -> resultSet.add(bytes.toHexString()));
              }
            });
    return resultSet.stream().sorted().toList();
  }

  /**
   * Collects the addresses whose bytecode was written during the block from the block access list.
   * Returns an empty set when no BAL is present (pre-Amsterdam), which disables the in-block
   * filter.
   */
  private Set<Address> collectInBlockCodeChanges(final Optional<BlockAccessList> maybeBal) {
    if (maybeBal.isEmpty()) {
      return Set.of();
    }
    final Set<Address> changed = ConcurrentHashMap.newKeySet();
    for (final var accountChanges : maybeBal.get().accountChanges()) {
      if (!accountChanges.codeChanges().isEmpty()) {
        changed.add(accountChanges.address());
      }
    }
    return changed;
  }

  /**
   * Returns RLP-encoded headers for every block from {@code oldestAncestor} up to (but not
   * including) the chain head, ordered ascending by block number as required by EIP-8025.
   */
  private List<String> buildHeaders(
      final Blockchain blockchain, final long oldestAncestor, final long blockNumber) {
    final List<String> result = new ArrayList<>();
    for (long number = oldestAncestor; number < blockNumber; number++) {
      result.add(
          RLP.encode(blockchain.getBlockHeader(number).orElseThrow()::writeTo).toHexString());
    }
    return result;
  }
}
