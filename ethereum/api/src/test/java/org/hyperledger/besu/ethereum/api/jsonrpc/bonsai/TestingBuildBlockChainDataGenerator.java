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
package org.hyperledger.besu.ethereum.api.jsonrpc.bonsai;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.blockcreation.AbstractBlockCreator;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.util.RawBlockIterator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Regenerates the {@code testing_buildBlockV1} chain-data fixtures for {@link
 * TestingBuildBlockJsonRpcHttpBySpecTest}.
 *
 * <p>The by-spec test loads a fixed chain (its {@code chain-data/genesis.json} plus {@code
 * chain-data/blocks.bin}). Whenever the protocol rules that apply to those blocks change — e.g.
 * EIP-8282 adding builder deposit/exit system calls to Amsterdam — the pre-built blocks can no
 * longer be re-imported and {@code blocks.bin} must be rebuilt against the current rules (and the
 * genesis, if it gained the required predeploys).
 *
 * <p>Regeneration is driven by Gradle, not by editing this class. After editing {@code
 * genesis.json}, run {@code ./gradlew :ethereum:api:regenerateTestingBuildBlockChainData}. That
 * task runs this class's {@link #main(String[])} (which rebuilds {@code blocks.bin} and swaps the
 * old genesis hash for the new one across the spec files) and then refreshes the spec responses
 * with {@code -Dbesu.test.update.specs=true}. It prints the new integrity checksum; paste that into
 * the {@code knownHash} of the {@code checkTestingBuildBlockChainData} guard in {@code
 * ethereum/api/build.gradle}. See the fixture {@code README.md} for the full procedure.
 *
 * <p>This class builds {@value #BLOCK_COUNT} empty blocks on top of the current genesis using the
 * same mainnet protocol schedule the test imports with, then writes them (RLP-encoded, one block
 * per top-level list, in the format {@link RawBlockIterator} reads) into the {@code
 * src/test/resources} copy of {@code blocks.bin}.
 */
public final class TestingBuildBlockChainDataGenerator {

  private static final int BLOCK_COUNT = 10;
  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/api/jsonrpc/testing_buildBlockV1/chain-data/genesis.json";

  public static void main(final String[] args) throws Exception {
    new TestingBuildBlockChainDataGenerator().regenerate();
  }

  private void regenerate() throws Exception {
    final Path genesisPath = resolveSourceFile("genesis.json");
    final Path blocksPath = genesisPath.resolveSibling("blocks.bin");
    // chain-data/ -> testing_buildBlockV1/
    final Path specDir = genesisPath.getParent().getParent();

    final GenesisConfig genesisConfig = GenesisConfig.fromResource(GENESIS_RESOURCE);

    final ProtocolSchedule protocolSchedule =
        MainnetProtocolSchedule.fromConfig(
            genesisConfig.getConfigOptions(),
            EvmConfiguration.DEFAULT,
            MiningConfiguration.newDefault(),
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    final BlockHeaderFunctions blockHeaderFunctions =
        ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);

    // The parent hash of the first existing block is the current (old) genesis hash. Capture it
    // before overwriting blocks.bin so we can swap it for the new one in the spec files.
    final Optional<Hash> oldGenesisHash = readFirstParentHash(blocksPath, blockHeaderFunctions);

    final ExecutionContextTestFixture fixture =
        ExecutionContextTestFixture.builder(genesisConfig)
            .protocolSchedule(protocolSchedule)
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    final MutableBlockchain blockchain = fixture.getBlockchain();
    final Hash newGenesisHash = blockchain.getGenesisBlock().getHash();

    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                MutableInitValues.builder().extraData(Bytes.EMPTY).coinbase(Address.ZERO).build())
            .build();
    final EthScheduler ethScheduler = new DeterministicEthScheduler();
    final AbstractBlockCreator blockCreator =
        new EmptyBlockCreator(
            miningConfiguration,
            fixture.getProtocolContext(),
            fixture.getProtocolSchedule(),
            ethScheduler);

    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    BlockHeader parentHeader = blockchain.getChainHeadHeader();
    for (int i = 0; i < BLOCK_COUNT; i++) {
      final Block block =
          blockCreator
              .createBlock(
                  Optional.of(List.of()),
                  Optional.of(List.of()),
                  Optional.of(List.of()),
                  Optional.of(Bytes32.ZERO),
                  Optional.of(Bytes32.ZERO),
                  Optional.empty(),
                  parentHeader.getTimestamp() + 1L,
                  true,
                  parentHeader)
              .getBlock();

      final ProtocolSpec spec = protocolSchedule.getByBlockHeader(block.getHeader());
      final boolean imported =
          spec.getBlockImporter()
              .importBlock(
                  fixture.getProtocolContext(),
                  block,
                  HeaderValidationMode.FULL,
                  HeaderValidationMode.FULL)
              .isImported();
      if (!imported) {
        throw new IllegalStateException(
            "Generated block " + block.getHeader().getNumber() + " failed to import");
      }
      block.writeTo(out);
      parentHeader = block.getHeader();
    }

    Files.write(blocksPath, out.encoded().toArray());
    System.out.println("Wrote " + BLOCK_COUNT + " blocks to " + blocksPath);

    if (oldGenesisHash.isPresent() && !oldGenesisHash.get().equals(newGenesisHash)) {
      final int changed =
          swapGenesisHashInSpecs(
              specDir, oldGenesisHash.get().toHexString(), newGenesisHash.toHexString());
      System.out.println(
          "Genesis hash changed "
              + oldGenesisHash.get().toHexString()
              + " -> "
              + newGenesisHash.toHexString()
              + " ("
              + changed
              + " spec files updated)");
    } else {
      System.out.println("Genesis hash unchanged: " + newGenesisHash.toHexString());
    }

    final String checksum = computeChecksum(blocksPath, genesisPath);
    System.out.println(
        "Integrity checksum: "
            + checksum
            + " -- update knownHash for the checkTestingBuildBlockChainData task in"
            + " ethereum/api/build.gradle to this value.");
  }

  private static Optional<Hash> readFirstParentHash(
      final Path blocksPath, final BlockHeaderFunctions blockHeaderFunctions) throws Exception {
    if (!Files.exists(blocksPath) || Files.size(blocksPath) == 0) {
      return Optional.empty();
    }
    try (final RawBlockIterator iterator = new RawBlockIterator(blocksPath, blockHeaderFunctions)) {
      return iterator.hasNext()
          ? Optional.of(iterator.next().getHeader().getParentHash())
          : Optional.empty();
    }
  }

  private static int swapGenesisHashInSpecs(
      final Path specDir, final String oldHash, final String newHash) throws Exception {
    int changed = 0;
    try (final Stream<Path> specs = Files.list(specDir)) {
      final List<Path> files =
          specs.filter(p -> p.getFileName().toString().matches("\\d.*\\.json")).toList();
      for (final Path spec : files) {
        final String original = Files.readString(spec, StandardCharsets.UTF_8);
        final String updated = original.replace(oldHash, newHash);
        if (!updated.equals(original)) {
          Files.writeString(spec, updated, StandardCharsets.UTF_8);
          changed++;
        }
      }
    }
    return changed;
  }

  /**
   * SHA-256 (Base64) of the guarded fixtures, computed exactly as the {@code
   * checkTestingBuildBlockChainData} Gradle guard does: files sorted by canonical path, contents
   * concatenated.
   */
  private static String computeChecksum(final Path... files) throws Exception {
    final Hasher hasher = Hashing.sha256().newHasher();
    final List<Path> sorted = new ArrayList<>(List.of(files));
    sorted.sort(Comparator.comparing(p -> p.toFile().toString()));
    for (final Path file : sorted) {
      hasher.putBytes(Files.readAllBytes(file));
    }
    return Base64.getEncoder().encodeToString(hasher.hash().asBytes());
  }

  /**
   * Resolve the {@code src/test/resources} copy of a chain-data file from the classpath resource.
   */
  private static Path resolveSourceFile(final String fileName) throws Exception {
    final URL genesisUrl = TestingBuildBlockChainDataGenerator.class.getResource(GENESIS_RESOURCE);
    final String buildGenesis = Paths.get(genesisUrl.toURI()).toString();
    final int buildIdx = buildGenesis.indexOf("/build/resources/test/");
    if (buildIdx < 0) {
      throw new IllegalStateException("Cannot map resource to src path: " + buildGenesis);
    }
    final String moduleRoot = buildGenesis.substring(0, buildIdx);
    final String resourceDir =
        buildGenesis
            .substring(buildIdx + "/build/resources/test/".length())
            .replace("/genesis.json", "");
    return Paths.get(moduleRoot, "src", "test", "resources", resourceDir, fileName);
  }

  private static final class EmptyBlockCreator extends AbstractBlockCreator {
    EmptyBlockCreator(
        final MiningConfiguration miningConfiguration,
        final org.hyperledger.besu.ethereum.ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final EthScheduler ethScheduler) {
      super(
          miningConfiguration,
          (__, ___) -> Address.ZERO,
          __ -> Bytes.EMPTY,
          mock(TransactionPool.class, RETURNS_DEEP_STUBS),
          protocolContext,
          protocolSchedule,
          ethScheduler);
    }

    @Override
    protected BlockHeader createFinalBlockHeader(final SealableBlockHeader sealableBlockHeader) {
      return BlockHeaderBuilder.create()
          .difficulty(Difficulty.ZERO)
          .populateFrom(sealableBlockHeader)
          .mixHash(Hash.ZERO)
          .nonce(0L)
          .blockHeaderFunctions(blockHeaderFunctions)
          .buildBlockHeader();
    }
  }
}
