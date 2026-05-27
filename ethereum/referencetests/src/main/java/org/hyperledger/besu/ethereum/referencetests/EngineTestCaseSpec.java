/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.referencetests;

import org.hyperledger.besu.config.BlobScheduleOptions;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.ConsensusContextFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.BesuService;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Represents an engine test fixture (blockchain_test_engine format) containing engine API payloads
 * to be executed against the client.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EngineTestCaseSpec {

  // Shared no-op ServiceManager — avoids anonymous class allocation per test
  private static final ServiceManager SHARED_SERVICE_MANAGER =
      new ServiceManager() {
        @Override
        public <T extends BesuService> void addService(
            final Class<T> serviceType, final T service) {}

        @Override
        public <T extends BesuService> Optional<T> getService(final Class<T> serviceType) {
          return Optional.empty();
        }
      };

  // Shared no-op trie loader — stateless, safe to reuse
  private static final NoopBonsaiCachedMerkleTrieLoader SHARED_TRIE_LOADER =
      new NoopBonsaiCachedMerkleTrieLoader();

  // Shared PostMergeContext singleton for engine tests — always post-merge, never syncing
  private static final org.hyperledger.besu.consensus.merge.PostMergeContext SHARED_MERGE_CONTEXT;

  static {
    SHARED_MERGE_CONTEXT =
        new org.hyperledger.besu.consensus.merge.PostMergeContext() {
          @Override
          public boolean isSyncing() {
            return false;
          }
        };
    SHARED_MERGE_CONTEXT.setIsPostMerge(Difficulty.ZERO);
  }

  private final String network;
  private final BlockchainReferenceTestCaseSpec.ReferenceTestBlockHeader genesisBlockHeader;
  private final Map<String, ReferenceTestWorldState.AccountMock> pre;
  private final Map<String, ReferenceTestWorldState.AccountMock> postState;
  private final Hash lastBlockHash;
  private final EngineNewPayload[] engineNewPayloads;
  private final JsonNode config;

  @JsonCreator
  public EngineTestCaseSpec(
      @JsonProperty("network") final String network,
      @JsonProperty("genesisBlockHeader")
          final BlockchainReferenceTestCaseSpec.ReferenceTestBlockHeader genesisBlockHeader,
      @JsonProperty("pre") final Map<String, ReferenceTestWorldState.AccountMock> pre,
      @JsonProperty("postState") final Map<String, ReferenceTestWorldState.AccountMock> postState,
      @JsonProperty("lastblockhash") final String lastBlockHash,
      @JsonProperty("engineNewPayloads") final EngineNewPayload[] engineNewPayloads,
      @JsonProperty("config") final JsonNode config) {
    this.network = network;
    this.genesisBlockHeader = genesisBlockHeader;
    this.pre = pre;
    this.postState = postState;
    this.lastBlockHash = Hash.fromHexString(lastBlockHash);
    this.engineNewPayloads = engineNewPayloads;
    this.config = config;
  }

  /**
   * The raw JSON of the fixture's {@code config.blobSchedule}, or {@code null} if absent. Used as a
   * cache key so a protocol schedule is built once per distinct blob schedule.
   *
   * @return the blob schedule node as a string, or "" when absent
   */
  public String getBlobScheduleKey() {
    final JsonNode blobSchedule = config == null ? null : config.get("blobSchedule");
    return blobSchedule == null ? "" : blobSchedule.toString();
  }

  /**
   * Builds {@link BlobScheduleOptions} from the fixture's {@code config.blobSchedule}. Fork keys are
   * lower-cased because Besu reads them as {@code cancun}/{@code prague}/{@code bpo1}..{@code bpo5}
   * whereas fixtures use {@code Cancun}/{@code BPO1} etc.
   *
   * @return the blob schedule options, or empty when the fixture carries no blob schedule
   */
  public Optional<BlobScheduleOptions> getBlobScheduleOptions() {
    final JsonNode blobSchedule = config == null ? null : config.get("blobSchedule");
    if (blobSchedule == null || !blobSchedule.isObject()) {
      return Optional.empty();
    }
    final ObjectNode root = ((ObjectNode) blobSchedule).objectNode();
    final Iterator<Map.Entry<String, JsonNode>> forks = blobSchedule.fields();
    while (forks.hasNext()) {
      final Map.Entry<String, JsonNode> fork = forks.next();
      final JsonNode value = fork.getValue();
      if (!value.isObject()) {
        continue;
      }
      // Besu's BlobSchedule reads numeric, all-lowercase keys (target/max/basefeeupdatefraction),
      // but EELS fixtures encode the values as hex strings (e.g. "0x0e") under a camelCase
      // baseFeeUpdateFraction; convert to lowercase numeric nodes.
      final ObjectNode entry = root.objectNode();
      entry.put("target", asInt(value.get("target")));
      entry.put("max", asInt(value.get("max")));
      entry.put("basefeeupdatefraction", asInt(value.get("baseFeeUpdateFraction")));
      root.set(fork.getKey().toLowerCase(Locale.ROOT), entry);
    }
    return Optional.of(new BlobScheduleOptions(root));
  }

  private static int asInt(final JsonNode node) {
    if (node == null || node.isNull()) {
      return 0;
    }
    return node.isNumber() ? node.asInt() : Long.decode(node.asText().trim()).intValue();
  }

  public String getNetwork() {
    return network;
  }

  public BlockchainReferenceTestCaseSpec.ReferenceTestBlockHeader getGenesisBlockHeader() {
    return genesisBlockHeader;
  }

  public Map<String, ReferenceTestWorldState.AccountMock> getPre() {
    return pre;
  }

  public Map<String, ReferenceTestWorldState.AccountMock> getPostState() {
    return postState;
  }

  public Hash getLastBlockHash() {
    return lastBlockHash;
  }

  public EngineNewPayload[] getEngineNewPayloads() {
    return engineNewPayloads;
  }

  public MutableBlockchain buildBlockchain() {
    final Block genesisBlock = new Block(genesisBlockHeader, BlockBody.empty());
    return InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesisBlock);
  }

  public ProtocolContext buildProtocolContext(final MutableBlockchain blockchain) {
    return new ProtocolContext.Builder()
        .withBlockchain(blockchain)
        .withWorldStateArchive(buildWorldStateArchive(blockchain))
        .withConsensusContext(new ConsensusContextFixture())
        .build();
  }

  /**
   * Build a ProtocolContext with MergeContext for engine tests. The MergeContext is required by
   * AbstractEngineNewPayload to determine terminal total difficulty and merge status.
   */
  public ProtocolContext buildProtocolContextForEngine(final MutableBlockchain blockchain) {
    return new ProtocolContext.Builder()
        .withBlockchain(blockchain)
        .withWorldStateArchive(buildWorldStateArchive(blockchain))
        .withConsensusContext(SHARED_MERGE_CONTEXT)
        .build();
  }

  private WorldStateArchive buildWorldStateArchive(
      final org.hyperledger.besu.ethereum.chain.Blockchain blockchain) {
    final InMemoryKeyValueStorageProvider inMemoryKeyValueStorageProvider =
        new InMemoryKeyValueStorageProvider();
    final WorldStateArchive worldStateArchive =
        new BonsaiWorldStateProvider(
            (BonsaiWorldStateKeyValueStorage)
                inMemoryKeyValueStorageProvider.createWorldStateStorage(
                    DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            blockchain,
            ImmutablePathBasedExtraStorageConfiguration.builder()
                .maxLayersToLoad(engineNewPayloads != null ? (long) engineNewPayloads.length : 0L)
                .build(),
            SHARED_TRIE_LOADER,
            SHARED_SERVICE_MANAGER,
            EvmConfiguration.DEFAULT,
            () -> (__, ___) -> {},
            new CodeCache());

    final MutableWorldState worldState = worldStateArchive.getWorldState();
    final WorldUpdater updater = worldState.updater();

    if (pre != null) {
      for (final Map.Entry<String, ReferenceTestWorldState.AccountMock> entry : pre.entrySet()) {
        ReferenceTestWorldState.insertAccount(
            updater, Address.fromHexString(entry.getKey()), entry.getValue());
      }
    }

    updater.commit();
    worldState.persist(null);

    worldStateArchive.resetArchiveStateTo(genesisBlockHeader);
    return worldStateArchive;
  }

  /** Represents a single engine_newPayload call from the fixture. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class EngineNewPayload {
    private final JsonNode[] params;
    private final String newPayloadVersion;
    private final String forkchoiceUpdatedVersion;
    private final String validationError;
    private final String errorCode;

    @JsonCreator
    public EngineNewPayload(
        @JsonProperty("params") final JsonNode[] params,
        @JsonProperty("newPayloadVersion") final String newPayloadVersion,
        @JsonProperty("forkchoiceUpdatedVersion") final String forkchoiceUpdatedVersion,
        @JsonProperty("validationError") final String validationError,
        @JsonProperty("errorCode") final String errorCode) {
      this.params = params;
      this.newPayloadVersion = newPayloadVersion;
      this.forkchoiceUpdatedVersion = forkchoiceUpdatedVersion;
      this.validationError = validationError;
      this.errorCode = errorCode;
    }

    public JsonNode[] getParams() {
      return params;
    }

    public int getNewPayloadVersion() {
      return Integer.parseInt(newPayloadVersion != null ? newPayloadVersion : "1");
    }

    public int getForkchoiceUpdatedVersion() {
      return Integer.parseInt(forkchoiceUpdatedVersion != null ? forkchoiceUpdatedVersion : "1");
    }

    public String getValidationError() {
      return validationError;
    }

    public String getErrorCode() {
      return errorCode;
    }

    /** Returns true if this payload is expected to be valid. */
    public boolean expectsValid() {
      return validationError == null && errorCode == null;
    }

    /** Returns the versioned hashes from params[1] (V3+), or null. */
    public List<String> getVersionedHashes() {
      if (params != null && params.length > 1 && params[1].isArray()) {
        var result = new java.util.ArrayList<String>();
        params[1].forEach(node -> result.add(node.asText()));
        return result;
      }
      return null;
    }

    /** Returns the parent beacon block root from params[2] (V3+), or null. */
    public String getBeaconRoot() {
      if (params != null && params.length > 2 && params[2].isTextual()) {
        return params[2].asText();
      }
      return null;
    }

    /** Returns the execution requests from params[3] (V4+), or null. */
    public List<String> getExecutionRequests() {
      if (params != null && params.length > 3 && params[3].isArray()) {
        var result = new java.util.ArrayList<String>();
        params[3].forEach(node -> result.add(node.asText()));
        return result;
      }
      return null;
    }
  }
}
