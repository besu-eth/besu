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
package org.hyperledger.besu.ethereum.vm;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.WitnessCodeReads;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.WitnessCodeTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.referencetests.BlockchainReferenceTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.FixtureExecutionWitness;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiExecutionWitnessBuilder;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.precompile.KZGPointEvalPrecompiledContract;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.testutil.JsonTestParameters;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Harness for the zkEVM execution-spec fixtures: executes each candidate block and asserts that the
 * EIP-8025 execution witness Besu derives matches the fixture's {@code executionWitness}.
 *
 * <p>Deliberately independent of {@link BlockchainReferenceTestTools}. It omits block re-building,
 * the journaled-updater guard and rejection-message matching — none of which affect witness
 * generation — so this suite stays focused on the witness itself. Local/manual suite; not run in CI.
 */
public class ZkevmWitnessReferenceTestTools {

  private static final Logger LOG = LoggerFactory.getLogger(ZkevmWitnessReferenceTestTools.class);

  /**
   * The zkevm fixtures declare only "Amsterdam" and "BPO2ToAmsterdamAtTime15k". The latter has no
   * entry in {@link ReferenceTestProtocolSchedules}, so it is not run.
   */
  private static final List<String> NETWORKS_TO_RUN = List.of("Amsterdam");

  private static final JsonTestParameters<?, ?> params =
      JsonTestParameters.create(BlockchainReferenceTestCaseSpec.class)
          .generator(
              (testName, fullPath, spec, collector) -> {
                final String eip = spec.getNetwork();
                collector.add(
                    testName + "[" + eip + "]", fullPath, spec, NETWORKS_TO_RUN.contains(eip));
              });

  static {
    // Stateless-verifier format fixtures: they exercise optional-proof encoding rather than witness
    // derivation from execution.
    params.ignore("eip8025_optional_proofs");
  }

  private ZkevmWitnessReferenceTestTools() {
    // utility class
  }

  public static Collection<Object[]> generateTestParametersForConfig(final String[] filePath) {
    return params.generate(filePath);
  }

  public static void executeTest(final String name, final BlockchainReferenceTestCaseSpec spec) {
    final MutableBlockchain blockchain = spec.buildBlockchain();
    final ProtocolContext protocolContext =
        spec.buildProtocolContext(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG, blockchain);
    final ProtocolSchedule schedule = zkevmSchedule(spec);

    for (final BlockchainReferenceTestCaseSpec.CandidateBlock candidateBlock :
        spec.getCandidateBlocks()) {
      if (!candidateBlock.isExecutable()) {
        return;
      }

      try {
        final Block block = candidateBlock.getBlock();
        final ProtocolSpec protocolSpec = schedule.getByBlockHeader(block.getHeader());

        final HeaderValidationMode validationMode =
            "NoProof".equalsIgnoreCase(spec.getSealEngine())
                ? HeaderValidationMode.LIGHT
                : HeaderValidationMode.FULL;

        // The witness code tracker has to be supplied up front: WitnessCodeReads is only produced
        // when block processing is given one.
        final WitnessCodeTracker witnessCodeTracker = new WitnessCodeTracker();
        final BlockProcessingResult processingResult =
            protocolSpec
                .getBlockValidator()
                .validateAndProcessBlock(
                    protocolContext,
                    block,
                    validationMode,
                    validationMode,
                    candidateBlock.getBlockAccessList(),
                    false,
                    false,
                    Optional.of(witnessCodeTracker));

        final boolean imported = processingResult.isSuccessful();
        assertThat(imported)
            .as(
                "Block import status for block %s (%s)",
                block.getHash(), processingResult.errorMessage.orElse("no error"))
            .isEqualTo(candidateBlock.isValid());

        if (imported) {
          // Assert before appending: while the chain head is still the parent, the parent world
          // state the witness builder needs is a direct lookup rather than a historical one.
          assertWitness(protocolContext, block, blockchain, processingResult, candidateBlock);

          processingResult
              .getYield()
              .ifPresent(
                  outputs -> {
                    blockchain.appendBlock(
                        block, outputs.getReceipts(), outputs.getBlockAccessList());
                    protocolContext
                        .getWorldStateArchive()
                        .getWorldState(
                            WorldStateQueryParams.newBuilder()
                                .withBlockHeader(block.getHeader())
                                .withShouldWorldStateUpdateHead(true)
                                .build());
                  });
        }
      } catch (final RLPException e) {
        assertThat(candidateBlock.isValid()).isFalse();
      }
    }

    assertThat(blockchain.getChainHeadHash()).isEqualTo(spec.getLastBlockHash());
  }

  /**
   * Builds the single protocol schedule this suite needs. The zkevm fixtures target post-BPO
   * Amsterdam blob parameters, so BPO1-5 are activated alongside Amsterdam.
   *
   * <p>Built directly rather than through {@link ReferenceTestProtocolSchedules}, whose factory
   * derives ~35 schedules from one genesis stub: the BPO timestamps would leak into every entry via
   * {@code clone()} and fail fork-order validation on the pre-merge ones. Building just the fork we
   * need also keeps the shared schedule map, and every other reference-test flavour that consumes
   * it, untouched.
   */
  private static ProtocolSchedule zkevmSchedule(final BlockchainReferenceTestCaseSpec spec) {
    final StubGenesisConfigOptions genesisOptions = new StubGenesisConfigOptions();
    genesisOptions.baseFeePerGas(0x0a);
    genesisOptions.bpo1Time(0).bpo2Time(0).bpo3Time(0).bpo4Time(0).bpo5Time(0).amsterdamTime(0);
    spec.getBlobScheduleOptions().ifPresent(genesisOptions::blobScheduleOptions);
    KZGPointEvalPrecompiledContract.init();
    return new ProtocolScheduleBuilder(
            genesisOptions,
            Optional.of(BigInteger.ONE),
            ProtocolSpecAdapters.create(0, Function.identity()),
            false,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem())
        .createProtocolSchedule();
  }

  private static void assertWitness(
      final ProtocolContext ctx,
      final Block block,
      final Blockchain blockchain,
      final BlockProcessingResult processingResult,
      final BlockchainReferenceTestCaseSpec.CandidateBlock candidateBlock) {

    // Skip genesis block since it doesn't have a parent to build the witness against
    if (block.getHeader().getNumber() == blockchain.getGenesisBlock().getHeader().getNumber()) {
      return;
    }

    final Optional<FixtureExecutionWitness> expectedWitnessOpt =
        candidateBlock.getExpectedWitness();
    if (expectedWitnessOpt.isEmpty()) {
      LOG.debug("Block {} has no expected witness in the fixture, skipping", block.getHash());
      return;
    }

    final BlockAccessList blockAccessList =
        processingResult.getYield().flatMap(BlockProcessingOutputs::getBlockAccessList).orElse(null);
    assertThat(blockAccessList)
        .as("block access list for block %s, needed to build its expected witness", block.getHash())
        .isNotNull();

    final WitnessCodeReads witnessCodeReads =
        processingResult.getYield().flatMap(BlockProcessingOutputs::getWitnessCodeReads).orElse(null);
    assertThat(witnessCodeReads)
        .as("witness code reads for block %s, needed to build its expected witness", block.getHash())
        .isNotNull();

    final FixtureExecutionWitness expected = expectedWitnessOpt.get();
    final BonsaiExecutionWitnessBuilder.Witness got =
        new BonsaiExecutionWitnessBuilder(ctx.getWorldStateArchive(), ctx.getBlockchain())
            .buildWitness(block.getHeader(), blockAccessList, witnessCodeReads);

    logWitnessDiff("state", got.state(), expected.state(), block.getHash());
    logWitnessDiff("codes", got.codes(), expected.codes(), block.getHash());
    logWitnessDiff("headers", got.headers(), expected.headers(), block.getHash());

    assertThat(got.state()).as("state for block %s", block.getHash()).isEqualTo(expected.state());
    assertThat(got.codes()).as("codes for block %s", block.getHash()).isEqualTo(expected.codes());
    assertThat(got.headers())
        .as("headers for block %s", block.getHash())
        .isEqualTo(expected.headers());
  }

  private static void logWitnessDiff(
      final String field,
      final List<String> got,
      final List<String> expected,
      final Hash blockHash) {
    final List<String> missing = new ArrayList<>(expected);
    missing.removeAll(got);
    final List<String> extra = new ArrayList<>(got);
    extra.removeAll(expected);
    if (missing.isEmpty() && extra.isEmpty()) {
      LOG.info("Block {} {} match", blockHash, field);
    } else {
      if (!missing.isEmpty()) {
        LOG.warn("Block {} {} missing ({}):", blockHash, field, missing.size());
        missing.forEach(e -> LOG.warn("  - {}", e));
      }
      if (!extra.isEmpty()) {
        LOG.warn("Block {} {} extra ({}):", blockHash, field, extra.size());
        extra.forEach(e -> LOG.warn("  + {}", e));
      }
    }
  }
}
