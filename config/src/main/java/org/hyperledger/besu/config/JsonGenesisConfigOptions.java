/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.config;

import static java.util.Collections.emptyMap;
import static java.util.Objects.isNull;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.tuweni.units.bigints.UInt256;

/** The Json genesis config options. */
public class JsonGenesisConfigOptions implements GenesisConfigOptions {

  private static final String ETHASH_CONFIG_KEY = "ethash";
  // Preferred alias for ethash's fixeddifficulty block in genesis files; "ethash" is retained for
  // backwards compatibility with existing genesis files.
  private static final String FIXED_DIFFICULTY_CONFIG_KEY = "fixeddifficulty";
  private static final String IBFT_LEGACY_CONFIG_KEY = "ibft";
  private static final String IBFT2_CONFIG_KEY = "ibft2";
  private static final String QBFT_CONFIG_KEY = "qbft";
  private static final String CLIQUE_CONFIG_KEY = "clique";
  private static final String EC_CURVE_CONFIG_KEY = "eccurve";
  private static final String TRANSITIONS_CONFIG_KEY = "transitions";
  private static final String DISCOVERY_CONFIG_KEY = "discovery";
  private static final String CHECKPOINT_CONFIG_KEY = "checkpoint";
  private static final String BLOB_SCHEDULE_CONFIG_KEY = "blobschedule";
  private static final String ZERO_BASE_FEE_KEY = "zerobasefee";
  private static final String FIXED_BASE_FEE_KEY = "fixedbasefee";
  private static final String WITHDRAWAL_REQUEST_CONTRACT_ADDRESS_KEY =
      "withdrawalrequestcontractaddress";
  private static final String DEPOSIT_CONTRACT_ADDRESS_KEY = "depositcontractaddress";
  private static final String CONSOLIDATION_REQUEST_CONTRACT_ADDRESS_KEY =
      "consolidationrequestcontractaddress";

  private final ObjectNode configRoot;
  private final Map<String, String> configOverrides = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  private final TransitionsConfigOptions transitions;

  /**
   * From json object json genesis config options.
   *
   * @param configRoot the config root
   * @return the json genesis config options
   */
  public static JsonGenesisConfigOptions fromJsonObject(final ObjectNode configRoot) {
    return fromJsonObjectWithOverrides(configRoot, emptyMap());
  }

  /**
   * From json object with overrides json genesis config options.
   *
   * @param configRoot the config root
   * @param configOverrides the config overrides
   * @return the json genesis config options
   */
  static JsonGenesisConfigOptions fromJsonObjectWithOverrides(
      final ObjectNode configRoot, final Map<String, String> configOverrides) {
    final TransitionsConfigOptions transitionsConfigOptions;
    transitionsConfigOptions = loadTransitionsFrom(configRoot);
    return new JsonGenesisConfigOptions(configRoot, configOverrides, transitionsConfigOptions);
  }

  private static TransitionsConfigOptions loadTransitionsFrom(final ObjectNode parentNode) {
    final Optional<ObjectNode> transitionsNode =
        JsonUtil.getObjectNode(parentNode, TRANSITIONS_CONFIG_KEY);
    if (transitionsNode.isEmpty()) {
      return new TransitionsConfigOptions(JsonUtil.createEmptyObjectNode());
    }

    return new TransitionsConfigOptions(transitionsNode.get());
  }

  /**
   * Instantiates a new Json genesis config options.
   *
   * @param maybeConfig the optional config
   * @param configOverrides the config overrides map
   * @param transitionsConfig the transitions configuration
   */
  JsonGenesisConfigOptions(
      final ObjectNode maybeConfig,
      final Map<String, String> configOverrides,
      final TransitionsConfigOptions transitionsConfig) {
    this.configRoot = isNull(maybeConfig) ? JsonUtil.createEmptyObjectNode() : maybeConfig;
    if (configOverrides != null) {
      this.configOverrides.putAll(configOverrides);
    }
    this.transitions = transitionsConfig;
  }

  @Override
  public String getConsensusEngine() {
    if (isEthHash()) {
      return ETHASH_CONFIG_KEY;
    } else if (isIbft2()) {
      return IBFT2_CONFIG_KEY;
    } else if (isIbftLegacy()) {
      return IBFT_LEGACY_CONFIG_KEY;
    } else if (isQbft()) {
      return QBFT_CONFIG_KEY;
    } else if (isClique()) {
      return CLIQUE_CONFIG_KEY;
    } else {
      return "unknown";
    }
  }

  @Override
  public boolean isEthHash() {
    return configRoot.has(ETHASH_CONFIG_KEY) || configRoot.has(FIXED_DIFFICULTY_CONFIG_KEY);
  }

  @Override
  public boolean isIbftLegacy() {
    return configRoot.has(IBFT_LEGACY_CONFIG_KEY);
  }

  @Override
  public boolean isClique() {
    return configRoot.has(CLIQUE_CONFIG_KEY);
  }

  @Override
  public boolean isIbft2() {
    return configRoot.has(IBFT2_CONFIG_KEY);
  }

  @Override
  public boolean isQbft() {
    return configRoot.has(QBFT_CONFIG_KEY);
  }

  @Override
  public boolean isPoa() {
    return isQbft() || isClique() || isIbft2() || isIbftLegacy();
  }

  @Override
  public boolean hasPos() {
    return getTerminalTotalDifficulty().isPresent();
  }

  @Override
  public IbftLegacyConfigOptions getIbftLegacyConfigOptions() {
    return JsonUtil.getObjectNode(configRoot, IBFT_LEGACY_CONFIG_KEY)
        .map(IbftLegacyConfigOptions::new)
        .orElse(IbftLegacyConfigOptions.DEFAULT);
  }

  @Override
  public BftConfigOptions getBftConfigOptions() {
    final String fieldKey = isIbft2() ? IBFT2_CONFIG_KEY : QBFT_CONFIG_KEY;
    return JsonUtil.getObjectNode(configRoot, fieldKey)
        .map(JsonBftConfigOptions::new)
        .orElse(JsonBftConfigOptions.DEFAULT);
  }

  @Override
  public QbftConfigOptions getQbftConfigOptions() {
    return JsonUtil.getObjectNode(configRoot, QBFT_CONFIG_KEY)
        .map(JsonQbftConfigOptions::new)
        .orElse(JsonQbftConfigOptions.DEFAULT);
  }

  @Override
  public DiscoveryOptions getDiscoveryOptions() {
    return JsonUtil.getObjectNode(configRoot, DISCOVERY_CONFIG_KEY)
        .map(DiscoveryOptions::new)
        .orElse(DiscoveryOptions.DEFAULT);
  }

  @Override
  public CheckpointConfigOptions getCheckpointOptions() {
    return JsonUtil.getObjectNode(configRoot, CHECKPOINT_CONFIG_KEY)
        .map(CheckpointConfigOptions::new)
        .orElse(CheckpointConfigOptions.DEFAULT);
  }

  @Override
  public JsonCliqueConfigOptions getCliqueConfigOptions() {
    return JsonUtil.getObjectNode(configRoot, CLIQUE_CONFIG_KEY)
        .map(JsonCliqueConfigOptions::new)
        .orElse(JsonCliqueConfigOptions.DEFAULT);
  }

  @Override
  public FixedDifficultyConfigOptions getFixedDifficultyConfigOptions() {
    // Prefer the "fixeddifficulty" key; fall back to "ethash" for backwards compatibility.
    return JsonUtil.getObjectNode(configRoot, FIXED_DIFFICULTY_CONFIG_KEY)
        .or(() -> JsonUtil.getObjectNode(configRoot, ETHASH_CONFIG_KEY))
        .map(FixedDifficultyConfigOptions::new)
        .orElse(FixedDifficultyConfigOptions.DEFAULT);
  }

  @Override
  public Optional<BlobScheduleOptions> getBlobScheduleOptions() {
    return JsonUtil.getObjectNode(configRoot, BLOB_SCHEDULE_CONFIG_KEY)
        .map(BlobScheduleOptions::new);
  }

  @Override
  public TransitionsConfigOptions getTransitions() {
    return transitions;
  }

  @Override
  public OptionalLong getHomesteadBlockNumber() {
    return getOptionalLong("homesteadblock");
  }

  @Override
  public OptionalLong getDaoForkBlock() {
    final OptionalLong block = getOptionalLong("daoforkblock");
    if (block.isPresent() && block.getAsLong() <= 0) {
      return OptionalLong.empty();
    }
    return block;
  }

  @Override
  public OptionalLong getTangerineWhistleBlockNumber() {
    return getOptionalLong("eip150block");
  }

  @Override
  public OptionalLong getSpuriousDragonBlockNumber() {
    return getOptionalLong("eip158block");
  }

  @Override
  public OptionalLong getByzantiumBlockNumber() {
    return getOptionalLong("byzantiumblock");
  }

  @Override
  public OptionalLong getConstantinopleBlockNumber() {
    return getOptionalLong("constantinopleblock");
  }

  @Override
  public OptionalLong getPetersburgBlockNumber() {
    final OptionalLong petersburgBlock = getOptionalLong("petersburgblock");
    final OptionalLong constantinopleFixBlock = getOptionalLong("constantinoplefixblock");
    if (constantinopleFixBlock.isPresent()) {
      if (petersburgBlock.isPresent()) {
        throw new RuntimeException(
            "Genesis files cannot specify both petersburgBlock and constantinopleFixBlock.");
      }
      return constantinopleFixBlock;
    }
    return petersburgBlock;
  }

  @Override
  public OptionalLong getIstanbulBlockNumber() {
    return getOptionalLong("istanbulblock");
  }

  @Override
  public OptionalLong getMuirGlacierBlockNumber() {
    return getOptionalLong("muirglacierblock");
  }

  @Override
  public OptionalLong getBerlinBlockNumber() {
    return getOptionalLong("berlinblock");
  }

  @Override
  public OptionalLong getLondonBlockNumber() {
    return getOptionalLong("londonblock");
  }

  @Override
  public OptionalLong getArrowGlacierBlockNumber() {
    return getOptionalLong("arrowglacierblock");
  }

  @Override
  public OptionalLong getGrayGlacierBlockNumber() {
    return getOptionalLong("grayglacierblock");
  }

  @Override
  public OptionalLong getMergeNetSplitBlockNumber() {
    return getOptionalLong("mergenetsplitblock");
  }

  @Override
  public OptionalLong getShanghaiTime() {
    return getOptionalLong("shanghaitime");
  }

  @Override
  public OptionalLong getCancunTime() {
    return getOptionalLong("cancuntime");
  }

  @Override
  public OptionalLong getPragueTime() {
    return getOptionalLong("praguetime");
  }

  @Override
  public OptionalLong getOsakaTime() {
    return getOptionalLong("osakatime");
  }

  @Override
  public OptionalLong getBpo1Time() {
    return getOptionalLong("bpo1time");
  }

  @Override
  public OptionalLong getBpo2Time() {
    return getOptionalLong("bpo2time");
  }

  @Override
  public OptionalLong getBpo3Time() {
    return getOptionalLong("bpo3time");
  }

  @Override
  public OptionalLong getBpo4Time() {
    return getOptionalLong("bpo4time");
  }

  @Override
  public OptionalLong getBpo5Time() {
    return getOptionalLong("bpo5time");
  }

  @Override
  public OptionalLong getAmsterdamTime() {
    return getOptionalLong("amsterdamtime");
  }

  @Override
  public OptionalLong getFutureEipsTime() {
    return getOptionalLong("futureeipstime");
  }

  @Override
  public OptionalLong getExperimentalEipsTime() {
    return getOptionalLong("experimentaleipstime");
  }

  @Override
  public Optional<Wei> getBaseFeePerGas() {
    return Optional.ofNullable(configOverrides.get("baseFeePerGas")).map(Wei::fromHexString);
  }

  @Override
  public Optional<UInt256> getTerminalTotalDifficulty() {
    return getOptionalBigInteger("terminaltotaldifficulty").map(UInt256::valueOf);
  }

  @Override
  public OptionalLong getTerminalBlockNumber() {
    return getOptionalLong("terminalblocknumber");
  }

  @Override
  public Optional<Hash> getTerminalBlockHash() {
    return getOptionalHash("terminalblockhash");
  }

  @Override
  public Optional<BigInteger> getChainId() {
    return getOptionalBigInteger("chainid");
  }

  @Override
  public OptionalInt getContractSizeLimit() {
    return getOptionalInt("contractsizelimit");
  }

  @Override
  public OptionalInt getEvmStackSize() {
    return getOptionalInt("evmstacksize");
  }

  @Override
  public PowAlgorithm getPowAlgorithm() {
    return isEthHash() ? PowAlgorithm.ETHASH : PowAlgorithm.UNSUPPORTED;
  }

  @Override
  public Optional<String> getEcCurve() {
    return JsonUtil.getString(configRoot, EC_CURVE_CONFIG_KEY);
  }

  @Override
  public boolean isZeroBaseFee() {
    return getOptionalBoolean(ZERO_BASE_FEE_KEY).orElse(false);
  }

  @Override
  public boolean isFixedBaseFee() {
    return getOptionalBoolean(FIXED_BASE_FEE_KEY).orElse(false);
  }

  @Override
  public Optional<Address> getWithdrawalRequestContractAddress() {
    Optional<String> inputAddress =
        JsonUtil.getString(configRoot, WITHDRAWAL_REQUEST_CONTRACT_ADDRESS_KEY);
    return inputAddress.map(Address::fromHexString);
  }

  @Override
  public Optional<Address> getDepositContractAddress() {
    Optional<String> inputAddress = JsonUtil.getString(configRoot, DEPOSIT_CONTRACT_ADDRESS_KEY);
    return inputAddress.map(Address::fromHexString);
  }

  @Override
  public Optional<Address> getConsolidationRequestContractAddress() {
    Optional<String> inputAddress =
        JsonUtil.getString(configRoot, CONSOLIDATION_REQUEST_CONTRACT_ADDRESS_KEY);
    return inputAddress.map(Address::fromHexString);
  }

  @Override
  public Map<String, Object> asMap() {
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    getChainId().ifPresent(chainId -> builder.put("chainId", chainId));

    // mainnet fork blocks
    getHomesteadBlockNumber().ifPresent(l -> builder.put("homesteadBlock", l));
    getDaoForkBlock().ifPresent(l -> builder.put("daoForkBlock", l));
    getTangerineWhistleBlockNumber().ifPresent(l -> builder.put("eip150Block", l));
    getSpuriousDragonBlockNumber().ifPresent(l -> builder.put("eip158Block", l));
    getByzantiumBlockNumber().ifPresent(l -> builder.put("byzantiumBlock", l));
    getConstantinopleBlockNumber().ifPresent(l -> builder.put("constantinopleBlock", l));
    getPetersburgBlockNumber().ifPresent(l -> builder.put("petersburgBlock", l));
    getIstanbulBlockNumber().ifPresent(l -> builder.put("istanbulBlock", l));
    getMuirGlacierBlockNumber().ifPresent(l -> builder.put("muirGlacierBlock", l));
    getBerlinBlockNumber().ifPresent(l -> builder.put("berlinBlock", l));
    getLondonBlockNumber().ifPresent(l -> builder.put("londonBlock", l));
    getArrowGlacierBlockNumber().ifPresent(l -> builder.put("arrowGlacierBlock", l));
    getGrayGlacierBlockNumber().ifPresent(l -> builder.put("grayGlacierBlock", l));
    getMergeNetSplitBlockNumber().ifPresent(l -> builder.put("mergeNetSplitBlock", l));
    getShanghaiTime().ifPresent(l -> builder.put("shanghaiTime", l));
    getCancunTime().ifPresent(l -> builder.put("cancunTime", l));
    getPragueTime().ifPresent(l -> builder.put("pragueTime", l));
    getOsakaTime().ifPresent(l -> builder.put("osakaTime", l));
    getBpo1Time().ifPresent(l -> builder.put("bpo1Time", l));
    getBpo2Time().ifPresent(l -> builder.put("bpo2Time", l));
    getBpo3Time().ifPresent(l -> builder.put("bpo3Time", l));
    getBpo4Time().ifPresent(l -> builder.put("bpo4Time", l));
    getBpo5Time().ifPresent(l -> builder.put("bpo5Time", l));
    getAmsterdamTime().ifPresent(l -> builder.put("amsterdamTime", l));
    getTerminalBlockNumber().ifPresent(l -> builder.put("terminalBlockNumber", l));
    getTerminalBlockHash()
        .ifPresent(h -> builder.put("terminalBlockHash", h.getBytes().toHexString()));
    getFutureEipsTime().ifPresent(l -> builder.put("futureEipsTime", l));
    getExperimentalEipsTime().ifPresent(l -> builder.put("experimentalEipsTime", l));

    getContractSizeLimit().ifPresent(l -> builder.put("contractSizeLimit", l));
    getEvmStackSize().ifPresent(l -> builder.put("evmstacksize", l));

    getWithdrawalRequestContractAddress()
        .ifPresent(l -> builder.put("withdrawalRequestContractAddress", l));
    getDepositContractAddress().ifPresent(l -> builder.put("depositContractAddress", l));
    getConsolidationRequestContractAddress()
        .ifPresent(l -> builder.put("consolidationRequestContractAddress", l));

    if (isClique()) {
      builder.put("clique", getCliqueConfigOptions().asMap());
    }
    if (isEthHash()) {
      builder.put("ethash", getFixedDifficultyConfigOptions().asMap());
    }
    if (isIbftLegacy()) {
      builder.put("ibft", getIbftLegacyConfigOptions().asMap());
    }
    if (isIbft2()) {
      builder.put("ibft2", getBftConfigOptions().asMap());
    }
    if (isQbft()) {
      builder.put("qbft", getQbftConfigOptions().asMap());
    }

    if (isZeroBaseFee()) {
      builder.put("zeroBaseFee", true);
    }

    if (isFixedBaseFee()) {
      builder.put("fixedBaseFee", true);
    }

    if (getBlobScheduleOptions().isPresent()) {
      builder.put("blobSchedule", getBlobScheduleOptions().get().asMap());
    }

    return builder.build();
  }

  private OptionalLong getOptionalLong(final String key) {
    if (configOverrides.containsKey(key)) {
      final String value = configOverrides.get(key);
      return value == null || value.isEmpty()
          ? OptionalLong.empty()
          : OptionalLong.of(Long.valueOf(configOverrides.get(key), 10));
    } else {
      return JsonUtil.getLong(configRoot, key);
    }
  }

  private OptionalInt getOptionalInt(final String key) {
    if (configOverrides.containsKey(key)) {
      final String value = configOverrides.get(key);
      return value == null || value.isEmpty()
          ? OptionalInt.empty()
          : OptionalInt.of(Integer.valueOf(configOverrides.get(key), 10));
    } else {
      return JsonUtil.getInt(configRoot, key);
    }
  }

  private Optional<BigInteger> getOptionalBigInteger(final String key) {
    if (configOverrides.containsKey(key)) {
      final String value = configOverrides.get(key);
      return value == null || value.isEmpty()
          ? Optional.empty()
          : Optional.of(new BigInteger(value));
    } else {
      return JsonUtil.getValueAsString(configRoot, key).map(s -> new BigInteger(s, 10));
    }
  }

  private Optional<Boolean> getOptionalBoolean(final String key) {
    if (configOverrides.containsKey(key)) {
      final String value = configOverrides.get(key);
      return value == null || value.isEmpty()
          ? Optional.empty()
          : Optional.of(Boolean.valueOf(configOverrides.get(key)));
    } else {
      return JsonUtil.getBoolean(configRoot, key);
    }
  }

  private Optional<Hash> getOptionalHash(final String key) {
    if (configOverrides.containsKey(key)) {
      final String overrideHash = configOverrides.get(key);
      return Optional.of(Hash.fromHexString(overrideHash));
    } else {
      return JsonUtil.getValueAsString(configRoot, key).map(Hash::fromHexString);
    }
  }

  @Override
  public List<Long> getForkBlockNumbers() {
    Stream<OptionalLong> forkBlockNumbers =
        Stream.of(
            getHomesteadBlockNumber(),
            getDaoForkBlock(),
            getTangerineWhistleBlockNumber(),
            getSpuriousDragonBlockNumber(),
            getByzantiumBlockNumber(),
            getConstantinopleBlockNumber(),
            getPetersburgBlockNumber(),
            getIstanbulBlockNumber(),
            getMuirGlacierBlockNumber(),
            getBerlinBlockNumber(),
            getLondonBlockNumber(),
            getArrowGlacierBlockNumber(),
            getGrayGlacierBlockNumber(),
            getMergeNetSplitBlockNumber());
    // when adding forks add an entry to ${REPO_ROOT}/config/src/test/resources/all_forks.json

    return forkBlockNumbers
        .filter(OptionalLong::isPresent)
        .map(OptionalLong::getAsLong)
        .distinct()
        .sorted()
        .toList();
  }

  @Override
  public List<Long> getForkBlockTimestamps() {
    Stream<OptionalLong> forkBlockTimestamps =
        Stream.of(
            getShanghaiTime(),
            getCancunTime(),
            getPragueTime(),
            getOsakaTime(),
            getBpo1Time(),
            getBpo2Time(),
            getBpo3Time(),
            getBpo4Time(),
            getBpo5Time(),
            getAmsterdamTime(),
            getFutureEipsTime(),
            getExperimentalEipsTime());
    // when adding forks add an entry to ${REPO_ROOT}/config/src/test/resources/all_forks.json

    return forkBlockTimestamps
        .filter(OptionalLong::isPresent)
        .map(OptionalLong::getAsLong)
        .distinct()
        .sorted()
        .toList();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final JsonGenesisConfigOptions that = (JsonGenesisConfigOptions) o;
    return Objects.equals(configRoot, that.configRoot)
        && Objects.equals(configOverrides, that.configOverrides);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configRoot, configOverrides);
  }
}
