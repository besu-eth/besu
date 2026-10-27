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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.patricia.TrieNodeDecoder;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Exercises dependency proof and publication against real Bonsai storage. */
class BonsaiWorldStateRepairDependencyTest {
  private static final Address CONTRACT = Address.fromHexString("0x1234");
  private static final Bytes EMPTY_LOCATION = Bytes.EMPTY;

  private ExecutionContextTestFixture fixture;
  private BonsaiWorldStateProvider archive;
  private BonsaiWorldState state;
  private BlockHeader target;
  private Bytes storageRootNode;
  private Hash storageRootHash;
  private WorldStateRepairRequest request;

  @BeforeEach
  void setUp() {
    fixture =
        ExecutionContextTestFixture.builder(
                GenesisConfig.fromConfig(
                    """
        {"config":{"chainId":42,"londonBlock":0},"gasLimit":"0x1c9c380",
         "difficulty":"0x1","alloc":{
         "0000000000000000000000000000000000001234":{"balance":"0x1","storage":{"0x0":"0x1"}},
         "0000000000000000000000000000000000001235":{"balance":"0x1"}}}
        """))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    archive = (BonsaiWorldStateProvider) fixture.getStateArchive();
    state = (BonsaiWorldState) archive.getWorldState();
    state.disableCacheMerkleTrieLoader();
    final var genesis = fixture.getGenesis().getHeader();
    final var updater = state.updater();
    updater.getAccount(CONTRACT).setStorageValue(UInt256.ZERO, UInt256.valueOf(2));
    updater.commit();
    target =
        new BlockHeaderTestFixture()
            .number(1)
            .timestamp(1)
            .parentHash(genesis.getHash())
            .stateRoot(state.frontierRootHash())
            .buildHeader();
    state.persist(target);
    fixture.getBlockchain().appendBlock(new Block(target, BlockBody.empty()), List.of());

    storageRootHash = ((BonsaiAccount) state.get(CONTRACT)).getStorageRoot();
    final var storage = archive.getWorldStateKeyValueStorage();
    final Bytes rootKey = TrieNodeKey.of(CONTRACT.addressHash(), EMPTY_LOCATION);
    storageRootNode =
        storage.getTrieNode(rootKey, Bytes32.wrap(storageRootHash.getBytes())).orElseThrow();
    final var deletion = storage.updater();
    deletion.removeTrieNode(rootKey);
    deletion.commitComposedOnly();
    storage.clearCrossBlockCache();
    archive.getWorldStateCacheManager().reset();
    request =
        new WorldStateRepairRequest(
            target.getHash(),
            target.getStateRoot(),
            Optional.of(
                new WorldStateRepairRequest.MissingTrieNode(
                    storageRootHash, EMPTY_LOCATION, Optional.of(CONTRACT))));
  }

  @Test
  void repairsMissingSameHeadStorageRootAndThenReportsAvailable() {
    archive.setMissingTrieNodeFetcher(
        (nodeRequest, budget) -> {
          assertThat(nodeRequest.accountHash()).contains(CONTRACT.addressHash());
          assertThat(nodeRequest.expectedNodeHash()).isEqualTo(storageRootHash);
          budget.consumeReceivedBytes(storageRootNode.size());
          return Optional.of(storageRootNode);
        });

    assertThat(archive.repairWorldState(request, WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.REPAIRED);
    assertThat(
            archive
                .getWorldStateKeyValueStorage()
                .getTrieNode(
                    TrieNodeKey.of(CONTRACT.addressHash(), EMPTY_LOCATION),
                    Bytes32.wrap(storageRootHash.getBytes())))
        .contains(storageRootNode);
    assertThat(archive.repairWorldState(request, WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.ALREADY_AVAILABLE);
  }

  @Test
  void rejectsUnprovedDependencyAndBadNodeWithoutPublishing() {
    final var storage = archive.getWorldStateKeyValueStorage();
    final var unproved =
        new WorldStateRepairRequest(
            target.getHash(),
            target.getStateRoot(),
            Optional.of(
                new WorldStateRepairRequest.MissingTrieNode(
                    Hash.ZERO, EMPTY_LOCATION, Optional.of(CONTRACT))));
    archive.setMissingTrieNodeFetcher(
        (nodeRequest, budget) -> {
          budget.consumeReceivedBytes(storageRootNode.size());
          return Optional.of(storageRootNode);
        });

    assertThat(archive.repairWorldState(unproved, WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.UNAVAILABLE);
    assertThat(
            storage.getTrieNode(
                TrieNodeKey.of(CONTRACT.addressHash(), EMPTY_LOCATION),
                Bytes32.wrap(storageRootHash.getBytes())))
        .isEmpty();

    archive.setMissingTrieNodeFetcher(
        (nodeRequest, budget) -> {
          budget.consumeReceivedBytes(1);
          return Optional.of(Bytes.of(1));
        });
    assertThat(archive.repairWorldState(request, WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.UNAVAILABLE);
    assertThat(
            storage.getTrieNode(
                TrieNodeKey.of(CONTRACT.addressHash(), EMPTY_LOCATION),
                Bytes32.wrap(storageRootHash.getBytes())))
        .isEmpty();
  }

  @Test
  void cancellationAndMovedHeadDoNotPublishDependency() {
    final var storage = archive.getWorldStateKeyValueStorage();
    final WorldStateRepairBudget cancelled = WorldStateRepairBudget.defaults();
    archive.setMissingTrieNodeFetcher(
        (nodeRequest, budget) -> {
          budget.consumeReceivedBytes(storageRootNode.size());
          budget.cancel();
          return Optional.of(storageRootNode);
        });
    assertThat(archive.repairWorldState(request, cancelled))
        .isEqualTo(WorldStateRepairResult.CANCELLED);
    assertThat(
            storage.getTrieNode(
                TrieNodeKey.of(CONTRACT.addressHash(), EMPTY_LOCATION),
                Bytes32.wrap(storageRootHash.getBytes())))
        .isEmpty();

    final BlockHeader replacement =
        new BlockHeaderTestFixture()
            .number(target.getNumber() + 1)
            .parentHash(target.getHash())
            .stateRoot(target.getStateRoot())
            .buildHeader();
    archive.setMissingTrieNodeFetcher(
        (nodeRequest, budget) -> {
          budget.consumeReceivedBytes(storageRootNode.size());
          archive.withHeadWorldStateLock(
              () -> {
                state.resetWorldStateTo(replacement);
                return null;
              });
          return Optional.of(storageRootNode);
        });
    assertThat(archive.repairWorldState(request, WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.SUPERSEDED);
    assertThat(
            storage.getTrieNode(
                TrieNodeKey.of(CONTRACT.addressHash(), EMPTY_LOCATION),
                Bytes32.wrap(storageRootHash.getBytes())))
        .isEmpty();
  }

  @Test
  void derivesDependencyHintFromRealMissingStorageFailure() {
    final var updater = state.updater();
    updater.getAccount(CONTRACT).setStorageValue(UInt256.ZERO, UInt256.valueOf(3));
    updater.commit();
    final MerkleTrieException failure =
        assertThrows(MerkleTrieException.class, state::frontierRootHash);
    state.updater().reset();
    final WorldStateRepairRequest derived =
        WorldStateRepairRequest.fromFailure(target.getHash(), target.getStateRoot(), failure);
    assertThat(derived.missingNode()).isPresent();
    assertThat(derived.missingNode().orElseThrow().expectedHash()).isEqualTo(storageRootHash);
    assertThat(derived.missingNode().orElseThrow().location()).isEqualTo(EMPTY_LOCATION);
    assertThat(derived.missingNode().orElseThrow().address()).contains(CONTRACT);
    archive.setMissingTrieNodeFetcher(
        (nodeRequest, budget) -> {
          budget.consumeReceivedBytes(storageRootNode.size());
          return Optional.of(storageRootNode);
        });
    assertThat(archive.repairWorldState(derived, WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.REPAIRED);
    final var retry = state.updater();
    retry.getAccount(CONTRACT).setStorageValue(UInt256.ZERO, UInt256.valueOf(3));
    retry.commit();
    assertThat(state.frontierRootHash()).isNotEqualTo(target.getStateRoot());
    state.updater().reset();
  }

  @Test
  void repairsMissingAccountTrieDescendantProvedByCurrentRoot() {
    final var storage = archive.getWorldStateKeyValueStorage();
    final Bytes accountRoot =
        storage
            .getTrieNode(Bytes.EMPTY, Bytes32.wrap(target.getStateRoot().getBytes()))
            .orElseThrow();
    final Node<Bytes> missingNode =
        TrieNodeDecoder.decodeNodes(Bytes.EMPTY, accountRoot).stream()
            .filter(node -> node.getLocation().filter(location -> !location.isEmpty()).isPresent())
            .filter(Node::isReferencedByHash)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected a hashed account trie descendant"));
    final Bytes location = missingNode.getLocation().orElseThrow();
    final Hash expectedHash = Hash.wrap(missingNode.getHash());
    final Bytes data =
        storage.getTrieNode(location, Bytes32.wrap(expectedHash.getBytes())).orElseThrow();
    final var deletion = storage.updater();
    deletion.removeTrieNode(location);
    deletion.commitComposedOnly();
    storage.clearCrossBlockCache();
    archive.getWorldStateCacheManager().reset();
    final var dependencyRequest =
        new WorldStateRepairRequest(
            target.getHash(),
            target.getStateRoot(),
            Optional.of(
                new WorldStateRepairRequest.MissingTrieNode(
                    expectedHash, location, Optional.empty())));
    archive.setMissingTrieNodeFetcher(
        (nodeRequest, budget) -> {
          assertThat(nodeRequest.accountHash()).isEmpty();
          assertThat(nodeRequest.location()).isEqualTo(location);
          assertThat(nodeRequest.expectedNodeHash()).isEqualTo(expectedHash);
          budget.consumeReceivedBytes(data.size());
          return Optional.of(data);
        });

    assertThat(archive.repairWorldState(dependencyRequest, WorldStateRepairBudget.defaults()))
        .isEqualTo(WorldStateRepairResult.REPAIRED);
    assertThat(storage.getTrieNode(location, Bytes32.wrap(expectedHash.getBytes()))).contains(data);
  }
}
