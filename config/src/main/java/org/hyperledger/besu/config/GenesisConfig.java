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

import org.hyperledger.besu.datatypes.Wei;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jspecify.annotations.Nullable;

/** The Genesis config file. */
public class GenesisConfig {

  /** The constant DEFAULT. */
  public static final GenesisConfig DEFAULT =
      new GenesisConfig(new GenesisReader.FromObjectNode(JsonUtil.createEmptyObjectNode()));

  /** The constant BASEFEE_AT_GENESIS_DEFAULT_VALUE. */
  public static final Wei BASEFEE_AT_GENESIS_DEFAULT_VALUE = Wei.of(1_000_000_000L);

  private final GenesisReader loader;
  private final ObjectNode genesisRoot;
  private @Nullable Map<String, String> overrides;

  private GenesisConfig(final GenesisReader loader) {
    this.loader = loader;
    this.genesisRoot = loader.getRoot();
  }

  /**
   * Mainnet genesis config file.
   *
   * @return the genesis config file
   */
  public static GenesisConfig mainnet() {
    return fromSource(GenesisConfig.class.getResource("/mainnet.json"));
  }

  /**
   * Genesis file from URL.
   *
   * @param jsonSource the URL
   * @return the genesis config file
   */
  public static GenesisConfig fromSource(final URL jsonSource) {
    return fromConfig(JsonUtil.objectNodeFromURL(jsonSource, false));
  }

  /**
   * Genesis file from resource.
   *
   * @param resourceName the resource name
   * @return the genesis config file
   */
  public static GenesisConfig fromResource(final String resourceName) {
    return fromConfig(GenesisConfig.class.getResource(resourceName));
  }

  /**
   * From config genesis config file.
   *
   * @param jsonSource the json string
   * @return the genesis config file
   */
  public static GenesisConfig fromConfig(final URL jsonSource) {
    return new GenesisConfig(new GenesisReader.FromURL(jsonSource));
  }

  /**
   * From config genesis config file.
   *
   * @param json the json string
   * @return the genesis config file
   */
  public static GenesisConfig fromConfig(final String json) {
    return fromConfig(JsonUtil.objectNodeFromString(json, false));
  }

  /**
   * From config genesis config file.
   *
   * @param config the config
   * @return the genesis config file
   */
  public static GenesisConfig fromConfig(final ObjectNode config) {
    return new GenesisConfig(new GenesisReader.FromObjectNode(config));
  }

  /**
   * Gets config options, including any overrides.
   *
   * @return the config options
   */
  public GenesisConfigOptions getConfigOptions() {
    final ObjectNode config = loader.getConfig();
    // are there any overrides to apply?
    if (this.overrides == null) {
      return JsonGenesisConfigOptions.fromJsonObject(config);
    }
    // otherwise apply overrides
    Map<String, String> overridesRef = this.overrides;

    // if baseFeePerGas has been explicitly configured, pass it as an override:
    final var optBaseFee = getBaseFeePerGas();
    if (optBaseFee.isPresent()) {
      // streams and maps cannot handle null values.
      overridesRef = new HashMap<>(this.overrides);
      overridesRef.put("baseFeePerGas", optBaseFee.get().toShortHexString());
    }

    return JsonGenesisConfigOptions.fromJsonObjectWithOverrides(config, overridesRef);
  }

  /**
   * Sets overrides for genesis options.
   *
   * @param overrides the overrides
   * @return the config options
   */
  public GenesisConfig withOverrides(final Map<String, String> overrides) {

    this.overrides = overrides;
    return this;
  }

  /**
   * Stream allocations stream.
   *
   * @return the stream
   */
  public Stream<GenesisAccount> streamAllocations() {
    return loader.streamAllocations();
  }

  /**
   * Gets parent hash.
   *
   * @return the parent hash
   */
  public String getParentHash() {
    return JsonUtil.getString(genesisRoot, "parenthash", "");
  }

  /**
   * Gets difficulty.
   *
   * @return the difficulty
   */
  public String getDifficulty() {
    return getRequiredString("difficulty");
  }

  /**
   * Gets extra data.
   *
   * @return the extra data
   */
  public String getExtraData() {
    return JsonUtil.getString(genesisRoot, "extradata", "");
  }

  /**
   * Gets gas limit.
   *
   * @return the gas limit
   */
  public long getGasLimit() {
    return parseLong("gasLimit", getFirstRequiredString("gaslimit", "gastarget"));
  }

  /**
   * Gets base fee per gas.
   *
   * @return the base fee per gas
   */
  public Optional<Wei> getBaseFeePerGas() {
    return JsonUtil.getString(genesisRoot, "basefeepergas")
        .map(baseFeeStr -> Wei.of(parseLong("baseFeePerGas", baseFeeStr)));
  }

  /**
   * Gets genesis base fee per gas.
   *
   * @return the genesis base fee per gas
   */
  public Optional<Wei> getGenesisBaseFeePerGas() {
    if (getBaseFeePerGas().isPresent()) {
      // always use specified basefee if present
      return getBaseFeePerGas();
    } else if (getConfigOptions().getLondonBlockNumber().orElse(-1L) == 0) {
      // if not specified, and we specify london at block zero use a default fee
      // this is needed for testing.
      return Optional.of(BASEFEE_AT_GENESIS_DEFAULT_VALUE);
    } else {
      // no explicit base fee and no london block zero means no basefee at genesis
      return Optional.empty();
    }
  }

  /**
   * Gets mix hash.
   *
   * @return the mix hash
   */
  public String getMixHash() {
    return JsonUtil.getString(genesisRoot, "mixhash", "");
  }

  /**
   * Gets nonce.
   *
   * @return the nonce
   */
  public String getNonce() {
    return JsonUtil.getValueAsString(genesisRoot, "nonce", "0x0");
  }

  /**
   * Gets excess blob gas.
   *
   * @return the excess blob gas
   */
  public String getExcessBlobGas() {
    return JsonUtil.getValueAsString(genesisRoot, "excessblobgas", "0x0");
  }

  /**
   * Gets blob gas used.
   *
   * @return the blob gas used
   */
  public String getBlobGasUsed() {
    return JsonUtil.getValueAsString(genesisRoot, "blobgasused", "0x0");
  }

  /**
   * Gets parent beacon block root.
   *
   * @return the parent beacon block root
   */
  public String getParentBeaconBlockRoot() {
    return JsonUtil.getValueAsString(
        genesisRoot,
        "parentbeaconblockroot",
        "0x0000000000000000000000000000000000000000000000000000000000000000");
  }

  /**
   * Gets slot number.
   *
   * @return the slot number
   */
  public String getSlotNumber() {
    return JsonUtil.getValueAsString(genesisRoot, "slotnumber", "0x0");
  }

  /**
   * Gets coinbase.
   *
   * @return the coinbase
   */
  public Optional<String> getCoinbase() {
    return JsonUtil.getString(genesisRoot, "coinbase");
  }

  /**
   * Gets timestamp.
   *
   * @return the timestamp
   */
  public long getTimestamp() {
    if (overrides != null && overrides.containsKey("timestamp")) {
      return Long.parseLong(overrides.get("timestamp"));
    }
    return parseLong("timestamp", JsonUtil.getValueAsString(genesisRoot, "timestamp", "0x0"));
  }

  private String getRequiredString(final String key) {
    return getFirstRequiredString(key);
  }

  private String getFirstRequiredString(final String... keys) {
    List<String> keysList = Arrays.asList(keys);
    return keysList.stream()
        .filter(genesisRoot::has)
        .findFirst()
        .map(key -> genesisRoot.get(key).asText())
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Invalid genesis block configuration, missing value for one of '%s'",
                        keysList)));
  }

  private long parseLong(final String name, final String value) {
    try {
      return Long.decode(value);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid genesis block configuration, "
              + name
              + " must be a number but was '"
              + value
              + "'");
    }
  }

  /**
   * Get Fork Block numbers
   *
   * @return list of fork block numbers
   */
  public List<Long> getForkBlockNumbers() {
    return getConfigOptions().getForkBlockNumbers();
  }

  /**
   * Get fork time stamps
   *
   * @return list of fork time stamps
   */
  public List<Long> getForkTimestamps() {
    return getConfigOptions().getForkBlockTimestamps();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final GenesisConfig that = (GenesisConfig) o;
    return Objects.equals(genesisRoot, that.genesisRoot);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(genesisRoot);
  }

  @Override
  public String toString() {
    return "GenesisConfigFile{"
        + "genesisRoot="
        + genesisRoot
        + ", allocations="
        + loader.streamAllocations().map(GenesisAccount::toString).collect(Collectors.joining(","))
        + '}';
  }
}
