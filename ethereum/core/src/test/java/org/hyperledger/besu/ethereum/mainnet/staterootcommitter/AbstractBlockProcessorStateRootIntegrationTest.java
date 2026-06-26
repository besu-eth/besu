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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.WorldStateHealerHelper.throwingWorldStateHealerSupplier;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_3;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_4;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.CONTRACT_ADDRESS;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.GENESIS_CONFIG;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor.TransactionReceiptFactory;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.parallelization.MainnetParallelBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelTransactionPreprocessing;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;

/**
 * Verifies that {@link org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor} produces the
 * same state root and Bonsai flat-database contents for sequential, optimistic-parallel, and BAL-
 * parallel import paths.
 */
@SuppressWarnings("rawtypes")
class AbstractBlockProcessorStateRootIntegrationTest {

  private static final Duration BAL_TIMEOUT = Duration.ofSeconds(5);
  private static final Wei BASE_FEE = Wei.of(5);
  private static final BalConfiguration BAL_STATE_ROOT_CONFIG =
      ImmutableBalConfiguration.builder()
          .balStateRootTimeout(BAL_TIMEOUT)
          .isBalStateRootEnabled(true)
          .build();
  private static final BalConfiguration OPTIMISTIC_PARALLEL_CONFIG =
      ImmutableBalConfiguration.builder()
          .isPerfectParallelizationEnabled(false)
          .isBalStateRootEnabled(true)
          .build();
  private static final BalConfiguration BAL_PARALLEL_CONFIG =
      ImmutableBalConfiguration.builder()
          .isPerfectParallelizationEnabled(true)
          .isBalStateRootEnabled(true)
          .build();

  @Test
  void independentTransfers_sequentialOptimisticAndBalAgree() {
    final Transaction tx1 =
        transfer(0, 1_000_000_000_000_000_000L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        transfer(0, 2_000_000_000_000_000_000L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);

    final ThreeModeOutcome outcome = compareThreeModes(BASE_FEE, tx1, tx2);

    assertAccountsEqual(
        outcome.sequential().worldState(),
        outcome.optimistic().worldState(),
        Address.fromHexStringStrict(ACCOUNT_2));
    assertAccountsEqual(
        outcome.sequential().worldState(),
        outcome.balParallel().worldState(),
        Address.fromHexStringStrict(ACCOUNT_3));
    assertThat(((BonsaiAccount) outcome.sequential().worldState().get(
            Address.fromHexStringStrict(ACCOUNT_2)))
        .getBalance())
        .isEqualTo(Wei.of(1_000_000_000_000_000_000L));
  }

  @Test
  void conflictedTransfersSameSender_sequentialOptimisticAndBalAgree() {
    final Transaction tx1 =
        transfer(0, 1_000_000_000_000_000_000L, ACCOUNT_4, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        transfer(1, 2_000_000_000_000_000_000L, ACCOUNT_3, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx3 =
        transfer(2, 500_000_000_000_000_000L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);

    compareThreeModes(BASE_FEE, tx1, tx2, tx3);
  }

  @Test
  void contractStorageWrites_sequentialOptimisticAndBalAgree() {
    final Address contract = Address.fromHexStringStrict(CONTRACT_ADDRESS);
    final Transaction txSetSlot1 =
        contractCall(0, contract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100));
    final Transaction txSetSlot2 =
        contractCall(1, contract, "setSlot2", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(200));
    final Transaction txSetSlot3 =
        contractCall(2, contract, "setSlot3", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(300));

    final ThreeModeOutcome outcome = compareThreeModes(BASE_FEE, txSetSlot1, txSetSlot2, txSetSlot3);

    assertStorageEqual(
        outcome.sequential().worldState(),
        outcome.optimistic().worldState(),
        contract,
        0,
        100);
    assertStorageEqual(
        outcome.balParallel().worldState(),
        outcome.sequential().worldState(),
        contract,
        2,
        300);
  }

  @Test
  void multiBlockImport_sequentialOptimisticAndBalAgree() {
    final Transaction block1Tx =
        transfer(0, 1_000_000_000_000_000_000L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    final Address contract = Address.fromHexStringStrict(CONTRACT_ADDRESS);
    final Transaction block2Tx =
        contractCall(0, contract, "setSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.of(42));

    final ThreeModeOutcome outcome =
        compareThreeModesMultiBlock(BASE_FEE, List.of(block1Tx), List.of(block2Tx));

    assertStorageEqual(
        outcome.sequential().worldState(),
        outcome.balParallel().worldState(),
        contract,
        0,
        42);
  }

  private ThreeModeOutcome compareThreeModes(final Wei baseFee, final Transaction... txs) {
    final BlockProcessorHarness discovery = BlockProcessorHarness.create();
    final Hash stateRoot = discovery.discoverStateRoot(baseFee, txs);

    final BlockProcessorHarness sequentialHarness = BlockProcessorHarness.create();
    final ImportOutcome sequential = sequentialHarness.importSequential(stateRoot, baseFee, txs);

    final BlockProcessorHarness optimisticHarness = BlockProcessorHarness.create();
    final ImportOutcome optimistic =
        optimisticHarness.importOptimisticParallel(stateRoot, baseFee, txs);

    final BlockAccessList referenceBal = sequential.bal().orElseThrow();
    final BlockProcessorHarness balHarness = BlockProcessorHarness.create();
    final ImportOutcome balParallel =
        balHarness.importBalParallel(referenceBal, stateRoot, baseFee, txs);

    assertModesAgree(sequential, optimistic, balParallel, referenceBal);
    return new ThreeModeOutcome(sequential, optimistic, balParallel);
  }

  private ThreeModeOutcome compareThreeModesMultiBlock(
      final Wei baseFee, final List<Transaction> block1Txs, final List<Transaction> block2Txs) {
    final BlockProcessorHarness discovery = BlockProcessorHarness.create();
    final Hash root1 =
        discovery.discoverStateRoot(baseFee, block1Txs.toArray(Transaction[]::new));
    final ImportOutcome discoveryBlock1 =
        discovery.importSequentialAndAppend(root1, baseFee, block1Txs.toArray(Transaction[]::new));
    final BlockHeader block1Header = discoveryBlock1.block().getHeader();
    final Hash root2 =
        discovery.discoverStateRootAtParent(
            block1Header, baseFee, block2Txs.toArray(Transaction[]::new));

    final BlockProcessorHarness sequentialHarness = BlockProcessorHarness.create();
    sequentialHarness.importSequentialAndAppend(root1, baseFee, block1Txs.toArray(Transaction[]::new));
    final ImportOutcome sequential =
        sequentialHarness.importSequentialAndAppend(
            root2, baseFee, block1Header, block2Txs.toArray(Transaction[]::new));

    final BlockProcessorHarness optimisticHarness = BlockProcessorHarness.create();
    optimisticHarness.importOptimisticParallelAndAppend(
        root1, baseFee, block1Txs.toArray(Transaction[]::new));
    final ImportOutcome optimistic =
        optimisticHarness.importOptimisticParallelAndAppend(
            root2, baseFee, block1Header, block2Txs.toArray(Transaction[]::new));

    final BlockAccessList referenceBal = sequential.bal().orElseThrow();
    final BlockProcessorHarness balHarness = BlockProcessorHarness.create();
    balHarness.importBalParallelAndAppend(
        discoveryBlock1.bal().orElseThrow(), root1, baseFee, block1Txs.toArray(Transaction[]::new));
    final ImportOutcome balParallel =
        balHarness.importBalParallelAndAppend(
            referenceBal, root2, baseFee, block1Header, block2Txs.toArray(Transaction[]::new));

    assertModesAgree(sequential, optimistic, balParallel, referenceBal);
    return new ThreeModeOutcome(sequential, optimistic, balParallel);
  }

  private void assertModesAgree(
      final ImportOutcome sequential,
      final ImportOutcome optimistic,
      final ImportOutcome balParallel,
      final BlockAccessList referenceBal) {
    assertThat(optimistic.stateRoot()).isEqualTo(sequential.stateRoot());
    assertThat(balParallel.stateRoot()).isEqualTo(sequential.stateRoot());
    assertThat(sequential.block().getHeader().getStateRoot()).isEqualTo(sequential.stateRoot());

    assertFlatDbEqual(sequential.flatDb(), optimistic.flatDb());
    assertFlatDbEqual(sequential.flatDb(), balParallel.flatDb());

    assertThat(sequential.trieLogPresent()).isTrue();
    assertThat(optimistic.trieLogPresent()).isTrue();
    assertThat(balParallel.trieLogPresent()).isTrue();

    assertThat(BodyValidation.balHash(optimistic.bal().orElseThrow()))
        .isEqualTo(BodyValidation.balHash(referenceBal));
    assertThat(BodyValidation.balHash(balParallel.bal().orElseThrow()))
        .isEqualTo(BodyValidation.balHash(referenceBal));
  }

  private static void assertAccountsEqual(
      final MutableWorldState expected, final MutableWorldState actual, final Address address) {
    final BonsaiAccount expectedAccount = (BonsaiAccount) expected.get(address);
    final BonsaiAccount actualAccount = (BonsaiAccount) actual.get(address);
    if (expectedAccount == null) {
      assertThat(actualAccount).isNull();
      return;
    }
    assertThat(actualAccount).isNotNull();
    assertThat(actualAccount.getBalance()).isEqualTo(expectedAccount.getBalance());
    assertThat(actualAccount.getNonce()).isEqualTo(expectedAccount.getNonce());
    assertThat(actualAccount.getCode()).isEqualTo(expectedAccount.getCode());
  }

  private static void assertStorageEqual(
      final MutableWorldState expected,
      final MutableWorldState actual,
      final Address contract,
      final int slot,
      final int value) {
    final UInt256 slotKey = UInt256.valueOf(slot);
    assertThat(((BonsaiAccount) actual.get(contract)).getStorageValue(slotKey))
        .isEqualTo(UInt256.valueOf(value));
    assertThat(((BonsaiAccount) actual.get(contract)).getStorageValue(slotKey))
        .isEqualTo(((BonsaiAccount) expected.get(contract)).getStorageValue(slotKey));
  }

  private static Transaction transfer(
      final long nonce, final long value, final String to, final KeyPair signer) {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerGas(Wei.of(5))
        .gasLimit(300_000L)
        .to(Address.fromHexStringStrict(to))
        .value(Wei.of(value))
        .payload(Bytes.EMPTY)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(signer);
  }

  private static Transaction contractCall(
      final int nonce,
      final Address contract,
      final String method,
      final KeyPair signer,
      final Optional<Integer> value) {
    final List<Type> parameters =
        value.isPresent() ? Arrays.<Type>asList(new Uint256(value.get())) : List.of();
    final Bytes payload =
        Bytes.fromHexString(
            FunctionEncoder.encode(
                new org.web3j.abi.datatypes.Function(method, parameters, List.of())));
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerGas(Wei.of(5))
        .gasLimit(3_000_000L)
        .to(contract)
        .value(Wei.ZERO)
        .payload(payload)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(signer);
  }

  private record ThreeModeOutcome(
      ImportOutcome sequential, ImportOutcome optimistic, ImportOutcome balParallel) {}

  private record ImportOutcome(
      Hash stateRoot,
      FlatDbSnapshot flatDb,
      MutableWorldState worldState,
      Block block,
      boolean trieLogPresent,
      Optional<BlockAccessList> bal,
      BlockProcessingResult result) {}

  private record FlatDbSnapshot(
      Map<Bytes, Bytes> account, Map<Bytes, Bytes> code, Map<Bytes, Bytes> storage) {}

  private static final class BlockProcessorHarness {

    private final InMemoryKeyValueStorageProvider provider;
    private final BonsaiWorldStateProvider archive;
    private final ProtocolContext protocolContext;
    private final ProtocolSchedule protocolSchedule;
    private final MutableBlockchain blockchain;
    private final KeyValueStorage trieLogStorage;

    private BlockProcessorHarness(
        final InMemoryKeyValueStorageProvider provider,
        final BonsaiWorldStateProvider archive,
        final ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final MutableBlockchain blockchain,
        final KeyValueStorage trieLogStorage) {
      this.provider = provider;
      this.archive = archive;
      this.protocolContext = protocolContext;
      this.protocolSchedule = protocolSchedule;
      this.blockchain = blockchain;
      this.trieLogStorage = trieLogStorage;
    }

    static BlockProcessorHarness create() {
      final ProtocolSchedule protocolSchedule =
          new ProtocolScheduleBuilder(
                  GenesisConfig.fromResource(GENESIS_CONFIG).getConfigOptions(),
                  Optional.of(BigInteger.valueOf(42)),
                  ProtocolSpecAdapters.create(0, Function.identity()),
                  false,
                  EvmConfiguration.DEFAULT,
                  MiningConfiguration.MINING_DISABLED,
                  new BadBlockManager(),
                  false,
                  BAL_STATE_ROOT_CONFIG,
                  new NoOpMetricsSystem())
              .createProtocolSchedule();
      final GenesisState genesisState =
          GenesisState.fromConfig(
              GenesisConfig.fromResource(GENESIS_CONFIG), protocolSchedule, new CodeCache());
      final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
      final MutableBlockchain blockchain =
          InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesisState.getBlock());
      final BonsaiWorldStateKeyValueStorage kvStorage =
          new BonsaiWorldStateKeyValueStorage(
              provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
      final BonsaiWorldStateProvider archive =
          new BonsaiWorldStateProvider(
              kvStorage,
              blockchain,
              DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getPathBasedExtraStorageConfiguration(),
              new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
              null,
              EvmConfiguration.DEFAULT,
              throwingWorldStateHealerSupplier(),
              new CodeCache());
      genesisState.writeStateTo(archive.getWorldState());
      final ProtocolContext protocolContext =
          new ProtocolContext.Builder()
              .withBlockchain(blockchain)
              .withWorldStateArchive(archive)
              .build();
      return new BlockProcessorHarness(
          provider,
          archive,
          protocolContext,
          protocolSchedule,
          blockchain,
          provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE));
    }

    Hash discoverStateRoot(final Wei baseFee, final Transaction... txs) {
      return discoverStateRootAtParent(blockchain.getChainHeadHeader(), baseFee, txs);
    }

    Hash discoverStateRootAtParent(
        final BlockHeader parentHeader, final Wei baseFee, final Transaction... txs) {
      final MutableWorldState worldState = archive.getWorldState();
      final Block block = buildBlock(parentHeader, Hash.ZERO, baseFee, txs);
      final BlockProcessingResult result =
          createSequentialProcessor().processBlock(protocolContext, blockchain, worldState, block);
      if (result.isSuccessful()) {
        return worldState.rootHash();
      }
      final String message =
          result.errorMessage.orElseThrow(
              () -> new AssertionError("Discovery failed without error message"));
      final String marker = "calculated ";
      final int idx = message.indexOf(marker);
      if (idx < 0) {
        throw new AssertionError("Unexpected discovery error: " + message);
      }
      return Hash.fromHexString(message.substring(idx + marker.length()));
    }

    ImportOutcome importSequential(final Hash stateRoot, final Wei baseFee, final Transaction... txs) {
      return importSequential(stateRoot, baseFee, blockchain.getChainHeadHeader(), txs);
    }

    ImportOutcome importSequential(
        final Hash stateRoot,
        final Wei baseFee,
        final BlockHeader parentHeader,
        final Transaction... txs) {
      final MutableWorldState worldState = archive.getWorldState();
      final Block block = buildBlock(parentHeader, stateRoot, baseFee, txs);
      final BlockProcessingResult result =
          createSequentialProcessor().processBlock(protocolContext, blockchain, worldState, block);
      assertTrue(
          result.isSuccessful(),
          "Sequential import failed: " + result.errorMessage.orElse("(no message)"));
      return toOutcome(worldState, block, result);
    }

    ImportOutcome importSequentialAndAppend(
        final Hash stateRoot, final Wei baseFee, final Transaction... txs) {
      return importSequentialAndAppend(stateRoot, baseFee, blockchain.getChainHeadHeader(), txs);
    }

    ImportOutcome importSequentialAndAppend(
        final Hash stateRoot,
        final Wei baseFee,
        final BlockHeader parentHeader,
        final Transaction... txs) {
      final ImportOutcome outcome = importSequential(stateRoot, baseFee, parentHeader, txs);
      appendBlock(outcome);
      return outcome;
    }

    ImportOutcome importOptimisticParallel(
        final Hash stateRoot, final Wei baseFee, final Transaction... txs) {
      return importOptimisticParallel(stateRoot, baseFee, blockchain.getChainHeadHeader(), txs);
    }

    ImportOutcome importOptimisticParallel(
        final Hash stateRoot,
        final Wei baseFee,
        final BlockHeader parentHeader,
        final Transaction... txs) {
      final MutableWorldState worldState = archive.getWorldState();
      final Block block = buildBlock(parentHeader, stateRoot, baseFee, txs);
      final ProtocolSpec spec = protocolSpec();
      final ParallelTransactionPreprocessing preprocessing =
          new ParallelTransactionPreprocessing(
              spec.getTransactionProcessor(), Runnable::run, OPTIMISTIC_PARALLEL_CONFIG);
      final BlockProcessingResult result =
          createParallelProcessor(OPTIMISTIC_PARALLEL_CONFIG)
              .processBlock(
                  protocolContext, blockchain, worldState, block, preprocessing);
      assertTrue(
          result.isSuccessful(),
          "Optimistic parallel import failed: " + result.errorMessage.orElse("(no message)"));
      return toOutcome(worldState, block, result);
    }

    ImportOutcome importOptimisticParallelAndAppend(
        final Hash stateRoot, final Wei baseFee, final Transaction... txs) {
      return importOptimisticParallelAndAppend(
          stateRoot, baseFee, blockchain.getChainHeadHeader(), txs);
    }

    ImportOutcome importOptimisticParallelAndAppend(
        final Hash stateRoot,
        final Wei baseFee,
        final BlockHeader parentHeader,
        final Transaction... txs) {
      final ImportOutcome outcome = importOptimisticParallel(stateRoot, baseFee, parentHeader, txs);
      appendBlock(outcome);
      return outcome;
    }

    ImportOutcome importBalParallel(
        final BlockAccessList referenceBal,
        final Hash stateRoot,
        final Wei baseFee,
        final Transaction... txs) {
      return importBalParallel(referenceBal, stateRoot, baseFee, blockchain.getChainHeadHeader(), txs);
    }

    ImportOutcome importBalParallel(
        final BlockAccessList referenceBal,
        final Hash stateRoot,
        final Wei baseFee,
        final BlockHeader parentHeader,
        final Transaction... txs) {
      final MutableWorldState worldState = archive.getWorldState();
      final Block block = buildBlock(parentHeader, stateRoot, baseFee, txs);
      final ProtocolSpec spec = protocolSpec();
      final ParallelTransactionPreprocessing preprocessing =
          new ParallelTransactionPreprocessingWithBal(
              spec.getTransactionProcessor(), referenceBal, BAL_PARALLEL_CONFIG);
      final BlockProcessingResult result =
          createParallelProcessor(BAL_PARALLEL_CONFIG)
              .processBlock(
                  protocolContext, blockchain, worldState, block, preprocessing);
      assertTrue(
          result.isSuccessful(),
          "BAL parallel import failed: " + result.errorMessage.orElse("(no message)"));
      return toOutcome(worldState, block, result);
    }

    ImportOutcome importBalParallelAndAppend(
        final BlockAccessList referenceBal,
        final Hash stateRoot,
        final Wei baseFee,
        final Transaction... txs) {
      return importBalParallelAndAppend(
          referenceBal, stateRoot, baseFee, blockchain.getChainHeadHeader(), txs);
    }

    ImportOutcome importBalParallelAndAppend(
        final BlockAccessList referenceBal,
        final Hash stateRoot,
        final Wei baseFee,
        final BlockHeader parentHeader,
        final Transaction... txs) {
      final ImportOutcome outcome =
          importBalParallel(referenceBal, stateRoot, baseFee, parentHeader, txs);
      appendBlock(outcome);
      return outcome;
    }

    private void appendBlock(final ImportOutcome outcome) {
      blockchain.appendBlock(
          outcome.block(),
          outcome.result().getYield().orElseThrow().getReceipts(),
          outcome.bal());
    }

    private ImportOutcome toOutcome(
        final MutableWorldState worldState, final Block block, final BlockProcessingResult result) {
      return new ImportOutcome(
          worldState.rootHash(),
          captureFlatDb(),
          worldState,
          block,
          trieLogStorage
              .get(block.getHeader().getHash().getBytes().toArrayUnsafe())
              .isPresent(),
          result.getYield().flatMap(BlockProcessingOutputs::getBlockAccessList),
          result);
    }

    private FlatDbSnapshot captureFlatDb() {
      return new FlatDbSnapshot(
          snapshot(provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)),
          snapshot(provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.CODE_STORAGE)),
          snapshot(
              provider.getStorageBySegmentIdentifier(
                  KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE)));
    }

    private Block buildBlock(
        final BlockHeader parentHeader,
        final Hash stateRoot,
        final Wei baseFee,
        final Transaction... txs) {
      final BlockHeader blockHeader =
          new BlockHeaderTestFixture()
              .number(parentHeader.getNumber() + 1L)
              .parentHash(parentHeader.getHash())
              .coinbase(MINING_BENEFICIARY)
              .stateRoot(stateRoot)
              .gasLimit(30_000_000L)
              .baseFeePerGas(baseFee)
              .buildHeader();
      return new Block(
          blockHeader,
          new BlockBody(Arrays.asList(txs), Collections.emptyList(), Optional.empty()));
    }

    private ProtocolSpec protocolSpec() {
      return protocolSchedule.getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
    }

    private MainnetBlockProcessor createSequentialProcessor() {
      final ProtocolSpec spec = protocolSpec();
      return new MainnetBlockProcessor(
          spec.getTransactionProcessor(),
          spec.getTransactionReceiptFactory(),
          Wei.ZERO,
          BlockHeader::getCoinbase,
          true,
          protocolSchedule,
          BAL_STATE_ROOT_CONFIG);
    }

    private MainnetParallelBlockProcessor createParallelProcessor(
        final BalConfiguration balConfiguration) {
      final ProtocolSpec spec = protocolSpec();
      return new NoFallbackParallelBlockProcessor(
          spec.getTransactionProcessor(),
          spec.getTransactionReceiptFactory(),
          Wei.ZERO,
          BlockHeader::getCoinbase,
          true,
          protocolSchedule,
          balConfiguration,
          new NoOpMetricsSystem());
    }
  }

  /** Parallel processor without silent sequential fallback (failures surface in tests). */
  private static final class NoFallbackParallelBlockProcessor extends MainnetParallelBlockProcessor {

    NoFallbackParallelBlockProcessor(
        final MainnetTransactionProcessor transactionProcessor,
        final TransactionReceiptFactory transactionReceiptFactory,
        final Wei blockReward,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final boolean skipZeroBlockRewards,
        final ProtocolSchedule protocolSchedule,
        final BalConfiguration balConfiguration,
        final MetricsSystem metricsSystem) {
      super(
          transactionProcessor,
          transactionReceiptFactory,
          blockReward,
          miningBeneficiaryCalculator,
          skipZeroBlockRewards,
          protocolSchedule,
          balConfiguration,
          metricsSystem);
    }

    @Override
    public BlockProcessingResult processBlock(
        final ProtocolContext protocolContext,
        final org.hyperledger.besu.ethereum.chain.Blockchain blockchain,
        final MutableWorldState worldState,
        final Block block,
        final Optional<BlockAccessList> blockAccessList) {
      return super.processBlock(
          protocolContext,
          blockchain,
          worldState,
          block,
          blockAccessList,
          new ParallelTransactionPreprocessing(
              transactionProcessor, Runnable::run, balConfiguration));
    }
  }

  private static final class ParallelTransactionPreprocessingWithBal
      extends ParallelTransactionPreprocessing {

    private final BlockAccessList preComputedBal;

    ParallelTransactionPreprocessingWithBal(
        final MainnetTransactionProcessor transactionProcessor,
        final BlockAccessList preComputedBal,
        final BalConfiguration balConfiguration) {
      super(transactionProcessor, Runnable::run, balConfiguration);
      this.preComputedBal = preComputedBal;
    }

    @Override
    public Optional<org.hyperledger.besu.ethereum.mainnet.parallelization.PreprocessingContext>
        run(
            final ProtocolContext protocolContext,
            final BlockHeader blockHeader,
            final List<Transaction> transactions,
            final Address miningBeneficiary,
            final BlockHashLookup blockHashLookup,
            final Wei blobGasPrice,
            final Optional<BlockAccessList.BlockAccessListBuilder> blockAccessListBuilder,
            final Optional<BlockAccessList> maybeBlockBal,
            final Optional<BlockHeader> maybeParentHeader) {
      return super.run(
          protocolContext,
          blockHeader,
          transactions,
          miningBeneficiary,
          blockHashLookup,
          blobGasPrice,
          blockAccessListBuilder,
          Optional.of(preComputedBal),
          maybeParentHeader);
    }
  }

  private static Map<Bytes, Bytes> snapshot(final KeyValueStorage storage) {
    return storage.getAllKeysThat(k -> true).stream()
        .map(Bytes::wrap)
        .collect(
            Collectors.toMap(
                key -> key, key -> Bytes.wrap(storage.get(key.toArrayUnsafe()).orElseThrow())));
  }

  private static void assertFlatDbEqual(final FlatDbSnapshot expected, final FlatDbSnapshot actual) {
    assertStorageKeysEqual(expected.account(), actual.account(), "account flat DB");
    assertStorageKeysEqual(expected.code(), actual.code(), "code flat DB");
    assertStorageKeysEqual(expected.storage(), actual.storage(), "storage flat DB");
  }

  private static void assertStorageKeysEqual(
      final Map<Bytes, Bytes> expected, final Map<Bytes, Bytes> actual, final String label) {
    final Set<Bytes> actualKeys = actual.keySet();
    assertThat(actualKeys).as(label + " keys").isEqualTo(expected.keySet());
    for (final Map.Entry<Bytes, Bytes> entry : expected.entrySet()) {
      assertThat(actual.get(entry.getKey())).as(label + " value for " + entry.getKey())
          .isEqualTo(entry.getValue());
    }
  }
}
