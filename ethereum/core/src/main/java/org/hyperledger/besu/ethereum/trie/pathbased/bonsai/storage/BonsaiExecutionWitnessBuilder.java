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
import org.hyperledger.besu.ethereum.WitnessCodeReads;
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
   * TrieLog + BAL for {@code state}, the {@link WitnessCodeReads}'s accumulated code-read sets for
   * {@code codes}, and the oldest accessed ancestor from {@link
   * WitnessCodeReads#accessedAncestors()} for {@code headers}.
   */
  public Witness buildWitness(
      final BlockHeader blockHeader,
      final BlockAccessList blockAccessList,
      final WitnessCodeReads witnessCodeReads) {

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
      // Addresses whose code was written during the block (CREATE, or a 7702 delegation designator
      // set this block). A stateless verifier already reconstructs this code from the block itself,
      // so an execution read that observed the in-block code must not pull the account's pre-state
      // code into the witness — mirroring EELS get_code, which skips reads served from code_writes.
      final Set<Address> inBlockCodeChanged = buildCodeDeployments(blockAccessList);
      final List<String> codes =
          buildCodes(
              ws,
              witnessCodeReads.codeReads(),
              witnessCodeReads.authorizationCodeReads(),
              inBlockCodeChanged);
      final Map<Long, Hash> accessedAncestors = witnessCodeReads.accessedAncestors();
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
      final BlockAccessList blockAccessList) {

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

    blockAccessList
        .accountChanges()
        .forEach(
            ac -> {
              updater.getAccount(ac.address());
              ac.storageReads()
                  .forEach(sr -> updater.getStorageValueByStorageSlotKey(ac.address(), sr.slot()));
              ac.storageChanges()
                  .forEach(sc -> updater.getStorageValueByStorageSlotKey(ac.address(), sc.slot()));
            });

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
   * Returns the set of addresses where code was newly deployed during the block (CREATE outputs or
   * EIP-7702 designation changes). A stateless verifier reconstructs these from the block body
   * itself, so they are excluded from the {@code codes} witness to avoid redundancy.
   */
  private Set<Address> buildCodeDeployments(final BlockAccessList bal) {
    if (bal.isEmpty()) {
      return Set.of();
    }
    final Set<Address> changed = ConcurrentHashMap.newKeySet();
    for (final var accountChanges : bal.accountChanges()) {
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
      final long n = number;
      result.add(
          RLP.encode(
                  blockchain
                          .getBlockHeader(n)
                          .orElseThrow(
                              () ->
                                  new IllegalStateException(
                                      "ancestor header missing for block " + n))
                      ::writeTo)
              .toHexString());
    }
    return result;
  }
}
