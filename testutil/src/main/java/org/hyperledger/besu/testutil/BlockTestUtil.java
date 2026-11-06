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
package org.hyperledger.besu.testutil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.io.Resources;

/** The Block test util. */
public final class BlockTestUtil {

  private BlockTestUtil() {
    throw new RuntimeException("Utility Class");
  }

  private static final Supplier<ChainResources> testChainSupplier =
      Suppliers.memoize(BlockTestUtil::supplyTestChainResources);
  private static final Supplier<ChainResources> hiveTestChainSupplier =
      Suppliers.memoize(BlockTestUtil::supplyHiveTestChainResources);
  private static final Supplier<ChainResources> testChainLondonSupplier =
      Suppliers.memoize(BlockTestUtil::supplyTestChainLondonResources);
  private static final Supplier<ChainResources> mainnetChainSupplier =
      Suppliers.memoize(BlockTestUtil::supplyMainnetChainResources);
  private static final Supplier<ChainResources> badPowChainSupplier =
      Suppliers.memoize(BlockTestUtil::supplyBadPowChainResources);
  private static final Supplier<ChainResources> forkOutdatedSupplier =
      Suppliers.memoize(BlockTestUtil::supplyOutdatedForkResources);
  private static final Supplier<ChainResources> forkUpgradedSupplier =
      Suppliers.memoize(BlockTestUtil::supplyUpgradedForkResources);
  private static final Supplier<ChainResources> testRpcCompactChainSupplier =
      Suppliers.memoize(BlockTestUtil::supplyTestRpcCompactResources);
  private static final Supplier<ChainResources> snapTestChainSupplier =
      Suppliers.memoize(BlockTestUtil::supplySnapTestChainResources);

  /**
   * Gets test blockchain url.
   *
   * @return the test blockchain url
   */
  public static URL getTestBlockchainUrl() {
    return getTestChainResources().getBlocksURL();
  }

  /**
   * Gets test london blockchain url.
   *
   * @return the test london blockchain url
   */
  public static URL getTestLondonBlockchainUrl() {
    return getTestChainLondonResources().getBlocksURL();
  }

  /**
   * Gets test genesis url.
   *
   * @return the test genesis url
   */
  public static URL getTestGenesisUrl() {
    return getTestChainResources().getGenesisURL();
  }

  /**
   * Gets test london genesis url.
   *
   * @return the test london genesis url
   */
  public static URL getTestLondonGenesisUrl() {
    return getTestChainLondonResources().getGenesisURL();
  }

  /**
   * Gets test chain resources.
   *
   * @return the test chain resources
   */
  public static ChainResources getTestChainResources() {
    return testChainSupplier.get();
  }

  /**
   * Gets test chain resources for hive tests.
   *
   * @return the test chain resources
   */
  public static ChainResources getHiveTestChainResources() {
    return hiveTestChainSupplier.get();
  }

  /**
   * Gets test chain london resources.
   *
   * @return the test chain london resources
   */
  public static ChainResources getTestChainLondonResources() {
    return testChainLondonSupplier.get();
  }

  /**
   * Gets mainnet resources.
   *
   * @return the mainnet resources
   */
  public static ChainResources getMainnetResources() {
    return mainnetChainSupplier.get();
  }

  private static ChainResources getBadPowResources() {
    return badPowChainSupplier.get();
  }

  /**
   * Gets outdated fork resources.
   *
   * @return the outdated fork resources
   */
  public static ChainResources getOutdatedForkResources() {
    return forkOutdatedSupplier.get();
  }

  /**
   * Gets upgraded fork resources.
   *
   * @return the upgraded fork resources
   */
  public static ChainResources getUpgradedForkResources() {
    return forkUpgradedSupplier.get();
  }

  /**
   * Gets Eth Ref Test resources.
   *
   * @return the Eth Ref Test resources.
   */
  public static ChainResources getEthRefTestResources() {
    return testRpcCompactChainSupplier.get();
  }

  /**
   * Gets test chain resources for Snap tests.
   *
   * @return the test chain resources
   */
  public static ChainResources getSnapTestChainResources() {
    return snapTestChainSupplier.get();
  }

  private static ChainResources supplyTestChainResources() {
    final URL genesisURL = ensureFileUrl(requiredResource("testGenesis.json"));
    final URL blocksURL = ensureFileUrl(requiredResource("testBlockchain.blocks"));
    return new ChainResources(genesisURL, blocksURL);
  }

  private static ChainResources supplySnapTestChainResources() {
    final URL genesisURL = ensureFileUrl(requiredResource("snap/snapGenesis.json"));
    final URL blocksURL = ensureFileUrl(requiredResource("snap/testBlockchain.blocks"));
    return new ChainResources(genesisURL, blocksURL);
  }

  private static ChainResources supplyHiveTestChainResources() {
    final URL genesisURL = ensureFileUrl(requiredResource("hive/testGenesis.json"));
    final URL blocksURL = ensureFileUrl(requiredResource("hive/testBlockchain.blocks"));
    return new ChainResources(genesisURL, blocksURL);
  }

  private static ChainResources supplyTestChainLondonResources() {
    final URL genesisURL =
        ensureFileUrl(requiredResource("fork-london-data/testLondonGenesis.json"));
    final URL blocksURL =
        ensureFileUrl(requiredResource("fork-london-data/testLondonBlockchain.blocks"));
    return new ChainResources(genesisURL, blocksURL);
  }

  private static ChainResources supplyMainnetChainResources() {
    final URL genesisURL = ensureFileUrl(requiredResource("mainnet-data/mainnet.json"));
    final URL blocksURL = ensureFileUrl(requiredResource("mainnet-data/1000.blocks"));
    return new ChainResources(genesisURL, blocksURL);
  }

  private static ChainResources supplyBadPowChainResources() {
    final URL genesisURL = ensureFileUrl(requiredResource("mainnet-data/mainnet.json"));
    final URL blocksURL = ensureFileUrl(requiredResource("mainnet-data/badpow.blocks"));
    return new ChainResources(genesisURL, blocksURL);
  }

  private static ChainResources supplyOutdatedForkResources() {
    final URL genesisURL = ensureFileUrl(requiredResource("fork-chain-data/genesis-outdated.json"));
    final URL blocksURL = ensureFileUrl(requiredResource("fork-chain-data/fork-outdated.blocks"));
    return new ChainResources(genesisURL, blocksURL);
  }

  private static ChainResources supplyUpgradedForkResources() {
    final URL genesisURL = ensureFileUrl(requiredResource("fork-chain-data/genesis-upgraded.json"));
    final URL blocksURL = ensureFileUrl(requiredResource("fork-chain-data/fork-upgraded.blocks"));
    return new ChainResources(genesisURL, blocksURL);
  }

  private static ChainResources supplyTestRpcCompactResources() {
    final URL genesisURL = ensureFileUrl(requiredResource("test-eth-ref-rpc-compact/genesis.json"));
    final URL blocksURL = ensureFileUrl(requiredResource("test-eth-ref-rpc-compact/chain.blocks"));
    return new ChainResources(genesisURL, blocksURL);
  }

  private static URL requiredResource(final String name) {
    return Objects.requireNonNull(
        BlockTestUtil.class.getClassLoader().getResource(name), () -> "Missing resource " + name);
  }

  /** Take a resource URL and if needed copy it to a temp file and return that URL. */
  private static URL ensureFileUrl(final URL resource) {
    try {
      try {
        Paths.get(resource.toURI());
      } catch (final FileSystemNotFoundException e) {
        final Path target = Files.createTempFile("besu", null);
        target.toFile().deleteOnExit();
        Files.copy(resource.openStream(), target, StandardCopyOption.REPLACE_EXISTING);
        return target.toUri().toURL();
      }
    } catch (final IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return resource;
  }

  /**
   * Writes the first 1000 blocks of the public chain to the given file.
   *
   * @param target FIle to write blocks to
   */
  public static void write1000Blocks(final Path target) {
    try {
      Files.write(
          target,
          Resources.toByteArray(getMainnetResources().getBlocksURL()),
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING);
    } catch (final IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Writes the first 1000 blocks of the public chain to the given file.
   *
   * @param target FIle to write blocks to
   */
  public static void writeBadPowBlocks(final Path target) {
    try {
      Files.write(
          target,
          Resources.toByteArray(getBadPowResources().getBlocksURL()),
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING);
    } catch (final IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** The Chain resources. */
  public static class ChainResources {
    private final URL genesisURL;
    private final URL blocksURL;

    /**
     * Instantiates a new Chain resources.
     *
     * @param genesisURL the genesis url
     * @param blocksURL the blocks url
     */
    public ChainResources(final URL genesisURL, final URL blocksURL) {
      this.genesisURL = genesisURL;
      this.blocksURL = blocksURL;
    }

    /**
     * Gets genesis url.
     *
     * @return the genesis url
     */
    public URL getGenesisURL() {
      return genesisURL;
    }

    /**
     * Gets blocks url.
     *
     * @return the blocks url
     */
    public URL getBlocksURL() {
      return blocksURL;
    }
  }
}
