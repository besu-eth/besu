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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.config.GenesisConfig.fromConfig;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

class GenesisConfigTest {

  private static final BigInteger MAINNET_CHAIN_ID = BigInteger.ONE;
  private static final BigInteger DEVELOPMENT_CHAIN_ID = BigInteger.valueOf(1337);
  private static final GenesisConfig EMPTY_CONFIG = fromConfig("{}");

  @Test
  void shouldLoadMainnetConfigFile() {
    final GenesisConfig config = GenesisConfig.mainnet();
    // Sanity check some basic properties to confirm this is the mainnet file.
    assertThat(config.getConfigOptions().isEthHash()).isTrue();
    assertThat(config.getConfigOptions().getChainId()).hasValue(MAINNET_CHAIN_ID);
    assertThat(
            config
                .streamAllocations()
                .map(GenesisAccount::address)
                .map(a -> a.getBytes().toUnprefixedHexString()))
        .contains(
            "000d836201318ec6899a67540690382780743280",
            "001762430ea9c3a26e5749afdb70da5f78ddbb8c",
            "001d14804b399c6ef80e64576f657660804fec0b");
  }

  @Test
  void shouldLoadDevelopmentConfigFile() {
    final GenesisConfig config = GenesisConfig.fromResource("/dev.json");
    // Sanity check some basic properties to confirm this is the dev file.
    assertThat(config.getConfigOptions().isEthHash()).isTrue();
    assertThat(config.getConfigOptions().getChainId()).hasValue(DEVELOPMENT_CHAIN_ID);
    assertThat(
            config
                .streamAllocations()
                .map(GenesisAccount::address)
                .map(a -> a.getBytes().toUnprefixedHexString()))
        .contains(
            "fe3b557e8fb62b89f4916b721be55ceb828dbd73",
            "627306090abab3a6e1400e9345bc60c78a8bef57",
            "f17f52151ebef6c7334fad080c5704d77216b732");
  }

  @Test
  void shouldGetParentHash() {
    assertThat(configWithProperty("parentHash", "844633").getParentHash()).isEqualTo("844633");
  }

  @Test
  void shouldDefaultParentHashToEmptyString() {
    assertThat(EMPTY_CONFIG.getParentHash()).isEmpty();
  }

  @Test
  void shouldGetDifficulty() {
    assertThat(configWithProperty("difficulty", "1234").getDifficulty()).isEqualTo("1234");
  }

  @Test
  void shouldRequireDifficulty() {
    assertInvalidConfiguration(EMPTY_CONFIG::getDifficulty);
  }

  @Test
  void shouldGetExtraData() {
    assertThat(configWithProperty("extraData", "yay").getExtraData()).isEqualTo("yay");
  }

  @Test
  void shouldDefaultExtraDataToEmptyString() {
    assertThat(EMPTY_CONFIG.getExtraData()).isEmpty();
  }

  @Test
  void shouldGetGasLimit() {
    assertThat(configWithProperty("gasLimit", "1000").getGasLimit()).isEqualTo(1000);
  }

  @Test
  void shouldRequireGasLimit() {
    assertInvalidConfiguration(EMPTY_CONFIG::getGasLimit);
  }

  @Test
  void shouldGetMixHash() {
    assertThat(configWithProperty("mixHash", "asdf").getMixHash()).isEqualTo("asdf");
  }

  @Test
  void shouldDefaultMixHashToEmptyString() {
    assertThat(EMPTY_CONFIG.getMixHash()).isEmpty();
  }

  @Test
  void shouldGetNonce() {
    assertThat(configWithProperty("nonce", "0x10").getNonce()).isEqualTo("0x10");
  }

  @Test
  void shouldDefaultNonceToZero() {
    assertThat(EMPTY_CONFIG.getNonce()).isEqualTo("0x0");
  }

  @Test
  void shouldGetCoinbase() {
    assertThat(configWithProperty("coinbase", "abcd").getCoinbase()).contains("abcd");
  }

  @Test
  void shouldReturnEmptyWhenCoinbaseNotSpecified() {
    assertThat(EMPTY_CONFIG.getCoinbase()).isEmpty();
  }

  @Test
  void shouldGetTimestamp() {
    assertThat(configWithProperty("timestamp", "0x10").getTimestamp()).isEqualTo(16L);
  }

  @Test
  void shouldGetBaseFeeAtGenesis() {
    GenesisConfig withBaseFeeAtGenesis =
        GenesisConfig.fromConfig("{\"config\":{\"londonBlock\":0},\"baseFeePerGas\":\"0xa\"}");
    assertThat(withBaseFeeAtGenesis.getBaseFeePerGas()).isPresent();
    assertThat(withBaseFeeAtGenesis.getBaseFeePerGas().get().toLong()).isEqualTo(10L);
  }

  @Test
  void shouldGetDefaultBaseFeeAtGenesis() {
    GenesisConfig withBaseFeeAtGenesis =
        GenesisConfig.fromConfig("{\"config\":{\"londonBlock\":0}}");
    // no specified baseFeePerGas:
    assertThat(withBaseFeeAtGenesis.getBaseFeePerGas()).isNotPresent();
    // supply a default genesis baseFeePerGas when london-at-genesis:
    assertThat(withBaseFeeAtGenesis.getGenesisBaseFeePerGas())
        .contains(GenesisConfig.BASEFEE_AT_GENESIS_DEFAULT_VALUE);
  }

  @Test
  void shouldGetBaseFeeExplicitlyAtGenesis() {
    GenesisConfig withBaseFeeNotAtGenesis =
        GenesisConfig.fromConfig("{\"config\":{\"londonBlock\":10},\"baseFeePerGas\":\"0xa\"}");
    // specified baseFeePerGas:
    Wei expectedBaseFee = Wei.of(0xa);
    assertThat(withBaseFeeNotAtGenesis.getBaseFeePerGas()).contains(expectedBaseFee);
    // but no baseFeePerGas since london block is not at genesis:
    assertThat(withBaseFeeNotAtGenesis.getGenesisBaseFeePerGas()).contains(expectedBaseFee);
  }

  @Test
  void shouldOverrideConfigOptionsBaseFeeWhenSpecified() {
    GenesisConfigOptions withOverrides =
        EMPTY_CONFIG
            .withOverrides(Map.of("baseFeePerGas", Wei.of(8).toString()))
            .getConfigOptions();
    assertThat(withOverrides.getBaseFeePerGas()).contains(Wei.of(8L));
  }

  @Test
  void shouldGetTerminalTotalDifficultyAtGenesis() {
    GenesisConfig withTerminalTotalDifficultyAtGenesis =
        fromConfig("{\"config\":{\"terminalTotalDifficulty\":1000}}");
    assertThat(withTerminalTotalDifficultyAtGenesis.getConfigOptions().getTerminalTotalDifficulty())
        .contains(UInt256.valueOf(1000L));
  }

  @Test
  void shouldGetEmptyTerminalTotalDifficultyAtGenesis() {
    assertThat(EMPTY_CONFIG.getConfigOptions().getTerminalTotalDifficulty()).isNotPresent();
  }

  @Test
  void assertSepoliaTerminalTotalDifficulty() {
    GenesisConfigOptions sepoliaOptions =
        GenesisConfig.fromResource("/sepolia.json").getConfigOptions();

    assertThat(sepoliaOptions.getTerminalTotalDifficulty()).isPresent();
    assertThat(sepoliaOptions.getTerminalTotalDifficulty())
        .contains(UInt256.valueOf(new BigInteger("17000000000000000")));
  }

  @Test
  void assertMainnetTerminalTotalDifficulty() {
    GenesisConfigOptions mainnetOptions =
        GenesisConfig.fromResource("/mainnet.json").getConfigOptions();

    assertThat(mainnetOptions.getTerminalTotalDifficulty()).isPresent();
    // tentative as of 2022-08-11:
    assertThat(mainnetOptions.getTerminalTotalDifficulty())
        .contains(UInt256.valueOf(new BigInteger("58750000000000000000000")));
  }

  @Test
  void assertTerminalTotalDifficultyOverride() {
    GenesisConfigOptions sepoliaOverrideOptions =
        GenesisConfig.fromResource("/sepolia.json")
            .withOverrides(Map.of("terminalTotalDifficulty", String.valueOf(Long.MAX_VALUE)))
            .getConfigOptions();

    assertThat(sepoliaOverrideOptions.getTerminalTotalDifficulty()).isPresent();
    assertThat(sepoliaOverrideOptions.getTerminalTotalDifficulty())
        .contains(UInt256.valueOf(Long.MAX_VALUE));
  }

  @Test
  void shouldFindMergeNetSplitForkAndAlias() {
    GenesisConfig mergeNetSplitGenesis =
        GenesisConfig.fromConfig(
            "{\"config\":{\"mergeNetsplitBlock\":11},\"baseFeePerGas\":\"0xa\"}");
    assertThat(mergeNetSplitGenesis.getForkBlockNumbers()).hasSize(1);
    assertThat(mergeNetSplitGenesis.getConfigOptions().getMergeNetSplitBlockNumber()).isPresent();
    assertThat(mergeNetSplitGenesis.getConfigOptions().getMergeNetSplitBlockNumber().getAsLong())
        .isEqualTo(11L);

    // assert empty if not present:
    GenesisConfig londonGenesis =
        GenesisConfig.fromConfig("{\"config\":{\"londonBlock\":11},\"baseFeePerGas\":\"0xa\"}");
    assertThat(londonGenesis.getForkBlockNumbers()).hasSize(1);
    assertThat(londonGenesis.getConfigOptions().getMergeNetSplitBlockNumber()).isEmpty();
  }

  @Test
  void assertMainnetDepositContractAddress() {
    GenesisConfigOptions mainnetOptions =
        GenesisConfig.fromResource("/mainnet.json").getConfigOptions();

    assertThat(mainnetOptions.getDepositContractAddress()).isPresent();
    assertThat(mainnetOptions.getDepositContractAddress().get())
        .isEqualTo(Address.fromHexString("0x00000000219ab540356cbb839cbe05303d7705fa"));
  }

  @Test
  void assertSepoliaDepositContractAddress() {
    GenesisConfigOptions sepoliaOptions =
        GenesisConfig.fromResource("/sepolia.json").getConfigOptions();

    assertThat(sepoliaOptions.getDepositContractAddress()).isPresent();
    assertThat(sepoliaOptions.getDepositContractAddress().get())
        .isEqualTo(Address.fromHexString("0x7f02c3e3c98b133055b8b348b2ac625669ed295d"));
  }

  @Test
  void assertEphemeryDepositContractAddress() {
    GenesisConfigOptions ephemeryOptions =
        GenesisConfig.fromResource("/ephemery.json").getConfigOptions();

    assertThat(ephemeryOptions.getDepositContractAddress()).isPresent();
    assertThat(ephemeryOptions.getDepositContractAddress().get())
        .isEqualTo(Address.fromHexString("0x00000000219ab540356cbb839cbe05303d7705fa"));
  }

  @Test
  void shouldDefaultTimestampToZero() {
    assertThat(EMPTY_CONFIG.getTimestamp()).isZero();
  }

  @Test
  void shouldGetSlotNumber() {
    assertThat(configWithProperty("slotnumber", "0x10").getSlotNumber()).isEqualTo("0x10");
  }

  @Test
  void shouldDefaultSlotNumberToZero() {
    assertThat(EMPTY_CONFIG.getSlotNumber()).isEqualTo("0x0");
  }

  @Test
  void shouldGetAllocations() {
    final GenesisConfig config =
        fromConfig(
            "{"
                + "  \"alloc\": {"
                + "    \"fe3b557e8fb62b89f4916b721be55ceb828dbd73\": {"
                + "      \"balance\": \"0xad78ebc5ac6200000\""
                + "    },"
                + "    \"627306090abaB3A6e1400e9345bC60c78a8BEf57\": {"
                + "      \"balance\": \"1000\""
                + "    },"
                + "    \"f17f52151EbEF6C7334FAD080c5704D77216b732\": {"
                + "      \"balance\": \"90000000000000000000000\","
                + "        \"storage\": {"
                + "          \"0xc4c3a3f99b26e5e534b71d6f33ca6ea5c174decfb16dd7237c60eff9774ef4a4\": \"0x937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0\",\n"
                + "          \"0xc4c3a3f99b26e5e534b71d6f33ca6ea5c174decfb16dd7237c60eff9774ef4a3\": \"0x6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012\""
                + "        }"
                + "    }"
                + "  }"
                + "}");

    final Map<Address, GenesisAccount> allocations =
        config
            .streamAllocations()
            .collect(Collectors.toMap(GenesisAccount::address, Function.identity()));
    assertThat(allocations.keySet())
        .map(addr -> addr.getBytes().toUnprefixedHexString())
        .containsOnly(
            "fe3b557e8fb62b89f4916b721be55ceb828dbd73",
            "627306090abab3a6e1400e9345bc60c78a8bef57",
            "f17f52151ebef6c7334fad080c5704d77216b732");
    final GenesisAccount alloc1 =
        allocations.get(Address.fromHexString("fe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    final GenesisAccount alloc2 =
        allocations.get(Address.fromHexString("627306090abab3a6e1400e9345bc60c78a8bef57"));
    final GenesisAccount alloc3 =
        allocations.get(Address.fromHexString("f17f52151ebef6c7334fad080c5704d77216b732"));

    assertThat(alloc1.balance())
        .isEqualTo(GenesisReader.ParserUtils.parseBalance("0xad78ebc5ac6200000"));
    assertThat(alloc2.balance()).isEqualTo(GenesisReader.ParserUtils.parseBalance("1000"));
    assertThat(alloc3.balance())
        .isEqualTo(GenesisReader.ParserUtils.parseBalance("90000000000000000000000"));
    assertThat(alloc3.storage()).hasSize(2);
    assertThat(alloc3.storage())
        .containsEntry(
            UInt256.fromHexString(
                "0xc4c3a3f99b26e5e534b71d6f33ca6ea5c174decfb16dd7237c60eff9774ef4a4"),
            UInt256.fromHexString(
                "0x937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0"));
    assertThat(alloc3.storage())
        .containsEntry(
            UInt256.fromHexString(
                "0xc4c3a3f99b26e5e534b71d6f33ca6ea5c174decfb16dd7237c60eff9774ef4a3"),
            UInt256.fromHexString(
                "0x6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012"));
  }

  @Test
  void shouldGetEmptyAllocationsWhenAllocNotPresent() {
    final GenesisConfig config = fromConfig("{}");
    assertThat(config.streamAllocations()).isEmpty();
  }

  @Test
  void shouldGetLargeChainId() {
    final GenesisConfig config =
        fromConfig(
            "{\"config\": { \"chainId\": 31415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679821480865132823066470938446095 }}");
    assertThat(config.getConfigOptions().getChainId())
        .contains(
            new BigInteger(
                "31415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679821480865132823066470938446095"));
  }

  @Test
  void mustNotAcceptComments() {
    assertThatThrownBy(
            () ->
                fromConfig(
                    "{\"config\": { \"chainId\": 2017 }\n/* C comment }*/\n//C++ comment }\n}"))
        .hasCauseInstanceOf(JsonParseException.class)
        .hasMessageContaining("Unexpected character ('/'");
  }

  @Test
  void testOverridePresent() {
    final GenesisConfig config = GenesisConfig.fromResource("/dev.json");
    final int bigBlock = 999_999_999;
    final String bigBlockString = Integer.toString(bigBlock);
    final Map<String, String> override = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    override.put("istanbulBlock", bigBlockString);
    override.put("chainId", bigBlockString);
    override.put("contractSizeLimit", bigBlockString);

    assertThat(config.getForkBlockNumbers()).isNotEmpty();
    assertThat(config.withOverrides(override).getConfigOptions().getIstanbulBlockNumber())
        .hasValue(bigBlock);
    assertThat(config.withOverrides(override).getConfigOptions().getChainId())
        .hasValue(BigInteger.valueOf(bigBlock));
    assertThat(config.withOverrides(override).getConfigOptions().getContractSizeLimit())
        .hasValue(bigBlock);
  }

  @Test
  void testOverrideNull() {
    final GenesisConfig config = GenesisConfig.fromResource("/dev.json");
    final Map<String, String> override = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    override.put("istanbulBlock", null);
    override.put("chainId", null);
    override.put("contractSizeLimit", null);

    assertThat(config.getForkBlockNumbers()).isNotEmpty();
    assertThat(config.withOverrides(override).getConfigOptions().getIstanbulBlockNumber())
        .isNotPresent();
    assertThat(config.withOverrides(override).getConfigOptions().getChainId()).isNotPresent();
    assertThat(config.withOverrides(override).getConfigOptions().getContractSizeLimit())
        .isNotPresent();
  }

  @Test
  void testOverrideCaseInsensitivity() {
    final GenesisConfig config = GenesisConfig.fromResource("/dev.json");
    final int bigBlock = 999_999_999;
    final String bigBlockString = Integer.toString(bigBlock);
    final Map<String, String> override = new HashMap<>();
    // as specified
    override.put("istanbulBlock", bigBlockString);
    // ALL CAPS
    override.put("CHAINID", bigBlockString);
    // all lower case
    override.put("contractsizelimit", bigBlockString);

    assertThat(config.withOverrides(override).getConfigOptions().getIstanbulBlockNumber())
        .hasValue(bigBlock);
    assertThat(config.withOverrides(override).getConfigOptions().getChainId())
        .hasValue(BigInteger.valueOf(bigBlock));
    assertThat(config.withOverrides(override).getConfigOptions().getContractSizeLimit())
        .hasValue(bigBlock);
  }

  @Test
  void testOverrideEmptyString() {
    final GenesisConfig config = GenesisConfig.fromResource("/dev.json");
    final Map<String, String> override = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    override.put("istanbulBlock", "");
    override.put("chainId", "");
    override.put("contractSizeLimit", "");

    assertThat(config.withOverrides(override).getConfigOptions().getIstanbulBlockNumber())
        .isNotPresent();
    assertThat(config.withOverrides(override).getConfigOptions().getChainId()).isNotPresent();
    assertThat(config.withOverrides(override).getConfigOptions().getContractSizeLimit())
        .isNotPresent();
  }

  @Test
  void testNoOverride() {
    final GenesisConfig config = GenesisConfig.fromResource("/dev.json");

    assertThat(config.getConfigOptions().getLondonBlockNumber()).hasValue(0);
    assertThat(config.getConfigOptions().getIstanbulBlockNumber()).isNotPresent();
    assertThat(config.getConfigOptions().getChainId()).hasValue(BigInteger.valueOf(1337));
    assertThat(config.getConfigOptions().getContractSizeLimit()).hasValue(2147483647);
    assertThat(config.getConfigOptions().getEvmStackSize()).isNotPresent();
  }

  @Test
  void testConstantinopleFixShouldNotBeSupportedAlongPetersburg() {
    // petersburg node
    final GenesisConfig config = GenesisConfig.fromResource("/all_forks.json");

    assertThat(config.getConfigOptions().getPetersburgBlockNumber()).hasValue(7);

    // constantinopleFix node
    final Map<String, String> override = new HashMap<>();
    override.put("constantinopleFixBlock", "1000");

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(
            () -> config.withOverrides(override).getConfigOptions().getPetersburgBlockNumber())
        .withMessage(
            "Genesis files cannot specify both petersburgBlock and constantinopleFixBlock.");
  }

  @Test
  void shouldLoadForksInSortedOrder() throws IOException {
    final ObjectNode configNode =
        new ObjectMapper()
            .createObjectNode()
            .set(
                "config",
                JsonUtil.objectNodeFromString(
                    Resources.toString(
                        Resources.getResource(
                            // If you inspect this config, you should see that fork block 2 is
                            // declared before 1
                            "valid_config_with_custom_forks.json"),
                        StandardCharsets.UTF_8)));

    final GenesisConfig config = fromConfig(configNode);

    assertThat(config.getForkBlockNumbers()).containsExactly(1L, 2L, 3L, 1035301L, 2222222L);
    assertThat(config.getConfigOptions().getChainId()).hasValue(BigInteger.valueOf(4));
  }

  @Test
  void shouldLoadForksIgnoreClassicForkBlock() throws IOException {
    final ObjectNode configNode =
        new ObjectMapper()
            .createObjectNode()
            .set(
                "config",
                JsonUtil.objectNodeFromString(
                    Resources.toString(
                        Resources.getResource(
                            // If you inspect this config, you should see that classicForkBlock is
                            // declared (which we want to ignore)
                            "valid_config_with_etc_forks.json"),
                        StandardCharsets.UTF_8)));
    final GenesisConfig config = fromConfig(configNode);

    assertThat(config.getForkBlockNumbers()).containsExactly(1L, 2L, 3L, 1035301L);
    assertThat(config.getConfigOptions().getChainId()).hasValue(BigInteger.valueOf(61));
  }

  @Test
  void shouldLoadForksIgnoreUnexpectedValues() throws IOException {
    final ObjectNode configNoUnexpectedForks =
        new ObjectMapper()
            .createObjectNode()
            .set(
                "config",
                JsonUtil.objectNodeFromString(
                    Resources.toString(
                        Resources.getResource("valid_config.json"), StandardCharsets.UTF_8)));

    final ObjectNode configClassicFork =
        new ObjectMapper()
            .createObjectNode()
            .set(
                "config",
                JsonUtil.objectNodeFromString(
                    Resources.toString(
                        Resources.getResource(
                            // If you inspect this config, you should see that classicForkBlock is
                            // declared (which we want to ignore)
                            "valid_config_with_etc_forks.json"),
                        StandardCharsets.UTF_8)));

    final ObjectNode configMultipleUnexpectedForks =
        new ObjectMapper()
            .createObjectNode()
            .set(
                "config",
                JsonUtil.objectNodeFromString(
                    Resources.toString(
                        Resources.getResource(
                            // If you inspect this config, you should see that
                            // 'unexpectedFork1Block',
                            // 'unexpectedFork2Block' and 'unexpectedFork3Block' are
                            // declared (which we want to ignore)
                            "valid_config_with_unexpected_forks.json"),
                        StandardCharsets.UTF_8)));

    final GenesisConfig configFileNoUnexpectedForks = fromConfig(configNoUnexpectedForks);
    final GenesisConfig configFileClassicFork = fromConfig(configClassicFork);
    final GenesisConfig configFileMultipleUnexpectedForks =
        fromConfig(configMultipleUnexpectedForks);

    assertThat(configFileNoUnexpectedForks.getForkBlockNumbers())
        .containsExactly(1L, 2L, 3L, 1035301L);
    assertThat(configFileNoUnexpectedForks.getForkBlockNumbers())
        .isEqualTo(configFileClassicFork.getForkBlockNumbers());
    assertThat(configFileNoUnexpectedForks.getForkBlockNumbers())
        .isEqualTo(configFileMultipleUnexpectedForks.getForkBlockNumbers());
    assertThat(configFileNoUnexpectedForks.getConfigOptions().getChainId())
        .hasValue(BigInteger.valueOf(61));
  }

  /**
   * The intent of this test is to catch encoding errors when a new hard fork is being added and the
   * config is being inserted in all the places the prior fork was. The intent is that
   * all_forks.json will also be updated.
   *
   * <p>This catches a common error in JsonGenesisConfigOptions where internally the names are all
   * lower ase but 'canonically' they are mixed case, as well as being mixed case almost everywhere
   * else in the code. Case differences are common in custom genesis files, so historically we have
   * been case agnostic.
   */
  @Test
  void roundTripForkIdBlocks() throws IOException {
    final String configText =
        Resources.toString(Resources.getResource("all_forks.json"), StandardCharsets.UTF_8);
    final ObjectNode genesisNode = JsonUtil.objectNodeFromString(configText);

    final GenesisConfig genesisConfig = fromConfig(genesisNode);

    final ObjectNode output = JsonUtil.objectNodeFromMap(genesisConfig.getConfigOptions().asMap());

    assertThat(JsonUtil.getJson(output, true))
        .isEqualTo(JsonUtil.getJson(genesisNode.get("config"), true));
  }

  private GenesisConfig configWithProperty(final String key, final String value) {
    return fromConfig("{\"" + key + "\":\"" + value + "\"}");
  }

  private void assertInvalidConfiguration(final ThrowingCallable getter) {
    assertThatThrownBy(getter)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid genesis block configuration");
  }
}
