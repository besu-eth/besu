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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Builds the EIP-8025 execution witness (state trie nodes, contract codes, and ancestor headers)
 * for a single block from a Bonsai world state and trie log. Used by both {@code
 * debug_executionWitness} and reference-test tooling so that both paths emit identical output.
 */
public class BonsaiExecutionWitnessBuilder {

  public record Witness(List<String> state, List<String> codes, List<String> headers) {}

  /**
   * Resolves the path-based archive, trie log, and parent world state for {@code blockHeader} and
   * builds the witness. Throws {@link IllegalStateException} when a prerequisite is unavailable
   * (non-Bonsai archive, missing trie log, missing parent state) or if witness construction fails.
   */
  public Witness buildWitness(
      final BlockHeader blockHeader,
      final BlockHeader parentHeader,
      final WorldStateArchive worldStateArchive,
      final Blockchain blockchain,
      final Optional<BlockProcessingOutputs> maybeOutputs) {
    if (!(worldStateArchive instanceof PathBasedWorldStateProvider pathBased)) {
      throw new IllegalStateException("Witness generation requires a path-based (Bonsai) archive");
    }
    final TrieLog trieLog =
        pathBased
            .getTrieLogManager()
            .getTrieLogLayer(blockHeader.getHash())
            .orElseThrow(
                () -> new IllegalStateException("No trie log for block " + blockHeader.getHash()));
    final MutableWorldState maybeParent =
        pathBased
            .getWorldState(withBlockHeaderAndNoUpdateNodeHead(parentHeader))
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Parent world state unavailable for " + parentHeader.getHash()));
    if (!(maybeParent instanceof BonsaiWorldState parent)) {
      throw new IllegalStateException(
          "Parent world state is not a BonsaiWorldState for " + parentHeader.getHash());
    }

    try (parent) {
      final Map<Long, Hash> accessedAncestors =
          maybeOutputs.map(BlockProcessingOutputs::getAccessedAncestors).orElse(Map.of());
      final Optional<BlockAccessList> maybeBlockAccessList =
          maybeOutputs.flatMap(BlockProcessingOutputs::getBlockAccessList);
      final Map<Address, Set<StorageSlotKey>> touchedSlots =
          buildTouchedSlotsMap(trieLog, maybeBlockAccessList);
      return new Witness(
          buildTrieNodes(parent, touchedSlots),
          buildCodes(parent, touchedSlots.keySet()),
          buildHeaders(blockchain, accessedAncestors));
    } catch (final Exception e) {
      throw new IllegalStateException(
          "Failed to build execution witness for " + blockHeader.getHash(), e);
    }
  }

  /**
   * Returns address → touched storage slots for witness construction.
   *
   * <p>When a {@link BlockAccessList} is present (Amsterdam+) it is preferred over the trie log.
   * There are known discrepancies between the two sources that cause ~4 000 zkevm reference-test
   * failures when relying on the trie log alone; the root cause is still under investigation. Using
   * the BAL passes 99.99% of those tests and allows users to start experimenting with
   * Besu-generated witnesses while the trie log issue is investigated in a follow-up PR.
   *
   * <p>TODO: investigate trie log discrepancies and remove the BAL dependency once resolved.
   */
  private Map<Address, Set<StorageSlotKey>> buildTouchedSlotsMap(
      final TrieLog trieLog, final Optional<BlockAccessList> maybeBal) {
    if (maybeBal.isPresent()) {
      // BAL preferred over trie log — see Javadoc above for context.
      final Map<Address, Set<StorageSlotKey>> result = new LinkedHashMap<>();
      maybeBal
          .get()
          .accountChanges()
          .forEach(
              ac -> {
                final Set<StorageSlotKey> slots = new LinkedHashSet<>();
                ac.storageReads().forEach(sr -> slots.add(sr.slot()));
                ac.storageChanges().forEach(sc -> slots.add(sc.slot()));
                result.put(ac.address(), slots);
              });
      return result;
    }
    // Fallback for pre-Amsterdam forks where no BAL is available.
    final Map<Address, Set<StorageSlotKey>> result = new LinkedHashMap<>();
    trieLog
        .getAccountChanges()
        .forEach(
            (address, __) ->
                result.put(
                    address, new LinkedHashSet<>(trieLog.getStorageChanges(address).keySet())));
    return result;
  }

  /**
   * Collects the trie nodes required to prove the given {@code touchedSlots} set by walking the
   * account-state and storage tries directly. For each account, an intercepting {@link NodeLoader}
   * records every node read while traversing its path in the parent state trie. For accounts with
   * touched storage slots the same interception is applied to the account's storage trie.
   *
   * <p>This avoids constructing a throw-away {@link BonsaiWorldState} and driving a full {@code
   * rollForward} + {@code persist} cycle: the trie is traversed directly against the parent
   * storage, skipping the world-state machinery and any unmodified subtrees visited during
   * root-hash recomputation.
   */
  private List<String> buildTrieNodes(
      final BonsaiWorldState worldView, final Map<Address, Set<StorageSlotKey>> touchedSlots) {

    final var storage = worldView.getWorldStateStorage();
    final Set<Bytes> collectedNodes = ConcurrentHashMap.newKeySet();

    // Intercept every account-trie node read and record it for the witness.
    final NodeLoader accountNodeLoader =
        (location, hash) -> {
          final Optional<Bytes> node = storage.getAccountStateTrieNode(location, hash);
          node.ifPresent(collectedNodes::add);
          return node;
        };
    final StoredMerklePatriciaTrie<Bytes, Bytes> accountTrie =
        new StoredMerklePatriciaTrie<>(
            new StoredNodeFactory<>(accountNodeLoader, Function.identity(), Function.identity()),
            Bytes32.wrap(worldView.rootHash().getBytes()));

    // For each touched account, walk the account trie to collect nodes on the path to the account
    touchedSlots.forEach(
        (address, slots) -> {
          final Hash addressHash = Hash.hash(address.getBytes());
          final Optional<Bytes> accountRlp = accountTrie.get(addressHash.getBytes());

          // Walk the storage trie only when slots were touched and the account exists in
          // the parent state (newly created accounts have no pre-execution storage trie).
          if (accountRlp.isPresent() && !slots.isEmpty()) {
            final Hash storageRoot =
                PmtStateTrieAccountValue.readFrom(RLP.input(accountRlp.get())).getStorageRoot();
            // Intercept storage-trie node reads for this account.
            final NodeLoader storageNodeLoader =
                (location, hash) -> {
                  final Optional<Bytes> node =
                      storage.getAccountStorageTrieNode(addressHash, location, hash);
                  node.ifPresent(collectedNodes::add);
                  return node;
                };
            final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
                new StoredMerklePatriciaTrie<>(
                    new StoredNodeFactory<>(
                        storageNodeLoader, Function.identity(), Function.identity()),
                    Bytes32.wrap(storageRoot.getBytes()));
            slots.forEach(slot -> storageTrie.get(slot.getSlotHash().getBytes()));
          }
        });

    return collectedNodes.stream().map(Bytes::toHexString).sorted().toList();
  }

  /**
   * Returns the RLP-encoded contract bytecodes for all {@code addresses} that have non-empty code
   * in the parent world state. Lookups run in parallel; results are deduplicated and sorted.
   */
  private List<String> buildCodes(final BonsaiWorldState worldView, final Set<Address> addresses) {
    final var resultSet = ConcurrentHashMap.<String>newKeySet();
    addresses.parallelStream()
        .forEach(
            address -> {
              final var account = worldView.get(address);
              if (account != null
                  && !account.getCodeHash().getBytes().equals(Hash.EMPTY.getBytes())) {
                worldView
                    .getCode(address, account.getCodeHash())
                    .ifPresent(bytes -> resultSet.add(bytes.toHexString()));
              }
            });
    return resultSet.stream().sorted().toList();
  }

  /**
   * Returns RLP-encoded block headers for every ancestor whose hash was observed during block
   * execution. At minimum the parent header is always present; additional entries are added for any
   * ancestor resolved while serving {@code BLOCKHASH}. Headers are ordered ascending by block
   * number as required by EIP-8025.
   */
  private List<String> buildHeaders(
      final Blockchain blockchain, final Map<Long, Hash> accessedAncestors) {
    return new TreeSet<>(accessedAncestors.keySet())
        .stream()
            .map(
                number -> {
                  final Hash hash = accessedAncestors.get(number);
                  return blockchain
                      .getBlockHeader(hash)
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "missing block header for accessed ancestor "
                                      + number
                                      + " ("
                                      + hash
                                      + ")"));
                })
            .map(h -> RLP.encode(h::writeTo).toHexString())
            .toList();
  }
}
