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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.parallelization.MainnetParallelBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.parallelization.OptimisticConcurrentTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockTransactionProcessor;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/**
 * The DAO recovery block must be processed sequentially even with parallel transaction processing
 * enabled: DaoBlockProcessor moves the DAO balances before delegating, and a parallel processor's
 * fallback would reset the world state and lose those transfers if its parallel attempt failed.
 * Whether a transaction actually runs in parallel depends on timing, so the tests check which
 * processor the DAO block gets rather than how its transactions ran.
 */
class DaoRecoveryBlockParallelProcessingTest {

  // Two of the addresses in daoAddresses.json, drained into the refund contract at the DAO block.
  private static final Address DAO_ACCOUNT_1 =
      Address.fromHexString("0xd4fe7bc31cedb7bfb8a345f31e668033056b2728");
  private static final Address DAO_ACCOUNT_2 =
      Address.fromHexString("0xb3fb0e5aba0e20e5c49d252dfd30e102b171a425");
  private static final Address DAO_REFUND_CONTRACT =
      Address.fromHexString("0xbf4ed7b27f1d666546e30d74d50d173d20bca754");

  private static final KeyPair SENDER_KEYS =
      SignatureAlgorithmFactory.getInstance()
          .createKeyPair(
              SECPPrivateKey.create(
                  Bytes32.fromHexString(
                      "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"),
                  SignatureAlgorithm.ALGORITHM));
  private static final Address SENDER = Util.publicKeyToAddress(SENDER_KEYS.getPublicKey());
  private static final Address RECIPIENT =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address COINBASE =
      Address.fromHexString("0x00000000000000000000000000000000000000cc");

  @Test
  void daoRecoveryBlockIsProcessedSequentiallyWithParallelProcessingEnabled() {
    final GenesisConfig genesis = genesis(1);
    final Hash expectedStateRoot = discoverStateRoot(genesis);

    final ExecutionContextTestFixture ctx = fixture(genesis, true);
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final BlockProcessingResult result = processBlockOne(ctx, worldState, expectedStateRoot);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(worldState.rootHash()).isEqualTo(expectedStateRoot);
    // The DAO balances moved to the refund contract.
    assertThat(worldState.get(DAO_ACCOUNT_1).getBalance()).isEqualTo(Wei.ZERO);
    assertThat(worldState.get(DAO_ACCOUNT_2).getBalance()).isEqualTo(Wei.ZERO);
    assertThat(worldState.get(DAO_REFUND_CONTRACT).getBalance()).isEqualTo(Wei.fromEth(3));
    // The DAO block's processor is sequential, so no parallel fallback can reset the transfers.
    assertThat(blockProcessor(ctx)).isInstanceOf(MainnetProtocolSpecs.DaoBlockProcessor.class);
    assertThat(((MainnetProtocolSpecs.DaoBlockProcessor) blockProcessor(ctx)).wrapped())
        .isNotInstanceOf(MainnetParallelBlockProcessor.class);
  }

  @Test
  void otherBlocksUseTheParallelProcessorInTheSameSetup() {
    // Same chain without a DAO fork: block 1 gets the parallel processor, which shows the setup
    // enables parallel processing and the assertion above would catch a parallel DAO processor.
    assertThat(blockProcessor(fixture(genesis(0), true)))
        .isInstanceOf(MainnetParallelBlockProcessor.class);
  }

  @Test
  void parallelFallbackOnTheDaoBlockLosesTheDaoTransfers() {
    // What the sequential DAO processor prevents: DaoBlockProcessor applies the DAO transfers, then
    // delegates to a parallel processor whose parallel attempt fails. Its fallback resets the world
    // state, dropping the transfers, and reruns the block directly on the sequential processor,
    // so nothing applies them again and the block no longer matches its state root.
    final GenesisConfig genesis = genesis(1);
    final Hash expectedStateRoot = discoverStateRoot(genesis);
    final ExecutionContextTestFixture ctx = fixture(genesis, false);
    final ProtocolSpec spec = ctx.getProtocolSchedule().getByBlockHeader(blockOneHeader(ctx));
    final BlockProcessor sequential =
        new MainnetBlockProcessor(
            spec.getTransactionProcessor(),
            spec.getTransactionReceiptFactory(),
            spec.getMiningBeneficiaryCalculator(),
            ctx.getProtocolSchedule(),
            BalConfiguration.DEFAULT);
    final RecordingRerun rerun = new RecordingRerun(sequential);
    final FailingInParallel parallel =
        new FailingInParallel(spec, ctx.getProtocolSchedule(), rerun);
    final BlockProcessor daoOverParallel = new MainnetProtocolSpecs.DaoBlockProcessor(parallel);

    final BlockProcessingResult result =
        processBlockOne(
            ctx, ctx.getStateArchive().getWorldState(), expectedStateRoot, daoOverParallel);

    // The DAO transfers were applied before the parallel attempt...
    assertThat(parallel.refundBalanceAtParallelAttempt).isEqualTo(Wei.fromEth(3));
    // ...and gone when the fallback reran the block: the reset dropped them.
    assertThat(rerun.refundBalanceAtRerun).isEqualTo(Wei.ZERO);
    assertThat(rerun.daoAccountBalanceAtRerun).isEqualTo(Wei.fromEth(1));
    // So the rerun misses them, and the block no longer matches its state root.
    assertThat(result.isFailed()).isTrue();
    assertThat(result.errorMessage)
        .hasValueSatisfying(message -> assertThat(message).contains(expectedStateRoot.toString()));
  }

  private static BlockProcessor blockProcessor(final ExecutionContextTestFixture ctx) {
    return ctx.getProtocolSchedule()
        .getByBlockHeader(new BlockHeaderTestFixture().number(1).buildHeader())
        .getBlockProcessor();
  }

  /** Discovers block 1's post-state root with parallel processing disabled. */
  private static Hash discoverStateRoot(final GenesisConfig genesis) {
    final ExecutionContextTestFixture ctx = fixture(genesis, false);
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final BlockProcessingResult result = processBlockOne(ctx, worldState, Hash.ZERO);
    if (result.isSuccessful()) {
      return worldState.rootHash();
    }
    final String message = result.errorMessage.orElseThrow();
    final String marker = "calculated ";
    return Hash.fromHexString(message.substring(message.indexOf(marker) + marker.length()));
  }

  private static BlockProcessingResult processBlockOne(
      final ExecutionContextTestFixture ctx,
      final MutableWorldState worldState,
      final Hash stateRoot) {
    return processBlockOne(
        ctx,
        worldState,
        stateRoot,
        ctx.getProtocolSchedule().getByBlockHeader(blockOneHeader(ctx)).getBlockProcessor());
  }

  private static BlockHeader blockOneHeader(final ExecutionContextTestFixture ctx) {
    return blockOneHeader(ctx, Hash.ZERO);
  }

  private static BlockHeader blockOneHeader(
      final ExecutionContextTestFixture ctx, final Hash stateRoot) {
    return new BlockHeaderTestFixture()
        .number(1)
        .parentHash(ctx.getBlockchain().getChainHeadHeader().getHash())
        .coinbase(COINBASE)
        .stateRoot(stateRoot)
        .gasLimit(30_000_000L)
        .buildHeader();
  }

  private static BlockProcessingResult processBlockOne(
      final ExecutionContextTestFixture ctx,
      final MutableWorldState worldState,
      final Hash stateRoot,
      final BlockProcessor blockProcessor) {
    final BlockHeader header = blockOneHeader(ctx, stateRoot);
    final Transaction transfer =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(0)
            .gasPrice(Wei.of(1))
            .gasLimit(21_000)
            .to(RECIPIENT)
            .value(Wei.fromEth(1))
            .payload(Bytes.EMPTY)
            .signAndBuild(SENDER_KEYS);
    final Block block =
        new Block(header, new BlockBody(List.of(transfer), List.of(), Optional.empty()));
    return blockProcessor.processBlock(
        ctx.getProtocolContext(), ctx.getBlockchain(), worldState, block);
  }

  private static ExecutionContextTestFixture fixture(
      final GenesisConfig genesis, final boolean parallelTxProcessingEnabled) {
    final ProtocolSchedule protocolSchedule =
        MainnetProtocolSchedule.fromConfig(
            genesis.getConfigOptions(),
            Optional.of(false),
            Optional.of(EvmConfiguration.DEFAULT),
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            parallelTxProcessingEnabled,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    return ExecutionContextTestFixture.builder(genesis)
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .protocolSchedule(protocolSchedule)
        .build();
  }

  /** Homestead from genesis, with the DAO fork at {@code daoForkBlock} (0 means no DAO fork). */
  private static GenesisConfig genesis(final long daoForkBlock) {
    return GenesisConfig.fromConfig(
        """
        {
          "config": {
            "chainId": 1337,
            "homesteadBlock": 0,
            "daoForkBlock": %d,
            "ethash": {}
          },
          "difficulty": "0x1",
          "gasLimit": "0x1c9c380",
          "alloc": {
            "%s": { "balance": "0x56bc75e2d63100000" },
            "%s": { "balance": "0xde0b6b3a7640000" },
            "%s": { "balance": "0x1bc16d674ec80000" }
          }
        }
        """
            .formatted(
                daoForkBlock,
                SENDER.toHexString(),
                DAO_ACCOUNT_1.toHexString(),
                DAO_ACCOUNT_2.toHexString()));
  }

  /**
   * Reads a balance through the world state's updater, which on Bonsai is the accumulator holding
   * the block's uncommitted changes, such as the DAO transfers.
   */
  private static Wei balance(final MutableWorldState worldState, final Address address) {
    return Optional.ofNullable(worldState.updater().get(address))
        .map(Account::getBalance)
        .orElse(Wei.ZERO);
  }

  /** Records the world state the fallback reruns the block on, then reruns it. */
  private static final class RecordingRerun implements BlockProcessor {

    private final BlockProcessor delegate;
    private Wei refundBalanceAtRerun;
    private Wei daoAccountBalanceAtRerun;

    RecordingRerun(final BlockProcessor delegate) {
      this.delegate = delegate;
    }

    @Override
    public BlockProcessingResult processBlock(
        final ProtocolContext protocolContext,
        final Blockchain blockchain,
        final MutableWorldState worldState,
        final Block block) {
      return processBlock(protocolContext, blockchain, worldState, block, Optional.empty());
    }

    @Override
    public BlockProcessingResult processBlock(
        final ProtocolContext protocolContext,
        final Blockchain blockchain,
        final MutableWorldState worldState,
        final Block block,
        final Optional<BlockAccessList> blockAccessList) {
      refundBalanceAtRerun = balance(worldState, DAO_REFUND_CONTRACT);
      daoAccountBalanceAtRerun = balance(worldState, DAO_ACCOUNT_1);
      return delegate.processBlock(protocolContext, blockchain, worldState, block, blockAccessList);
    }
  }

  /** A parallel processor whose parallel attempt always fails, so the block falls back. */
  private static final class FailingInParallel extends MainnetParallelBlockProcessor {

    FailingInParallel(
        final ProtocolSpec spec,
        final ProtocolSchedule protocolSchedule,
        final BlockProcessor sequentialBlockProcessor) {
      super(
          spec.getTransactionProcessor(),
          spec.getTransactionReceiptFactory(),
          spec.getMiningBeneficiaryCalculator(),
          protocolSchedule,
          BalConfiguration.DEFAULT,
          new NoOpMetricsSystem(),
          Runnable::run,
          Optional.of(sequentialBlockProcessor));
    }

    private Wei refundBalanceAtParallelAttempt;

    @Override
    public BlockProcessingResult processBlock(
        final ProtocolContext protocolContext,
        final Blockchain blockchain,
        final MutableWorldState worldState,
        final Block block,
        final Optional<BlockAccessList> blockAccessList) {
      refundBalanceAtParallelAttempt = balance(worldState, DAO_REFUND_CONTRACT);
      return super.processBlock(protocolContext, blockchain, worldState, block, blockAccessList);
    }

    @Override
    protected Optional<ParallelBlockTransactionProcessor> startParallelExecution(
        final ProtocolContext protocolContext,
        final BlockHeader blockHeader,
        final List<Transaction> transactions,
        final Address miningBeneficiary,
        final BlockHashLookup blockHashLookup,
        final Wei blobGasPrice,
        final Optional<BlockAccessList.BlockAccessListBuilder> blockAccessListBuilder,
        final Optional<BlockAccessListAccountLookup> blockAccessListLookup,
        final Optional<BlockHeader> maybeParentHeader) {
      return Optional.of(
          new OverBudgetResults(transactionProcessor, blockHeader.getGasLimit() + 1));
    }
  }

  /**
   * Reports a result for every transaction that uses more gas than the block allows, a failure that
   * does not reset the world state itself, so the reset happens in the fallback.
   */
  private static final class OverBudgetResults extends OptimisticConcurrentTransactionProcessor {

    private final long gasUsed;

    OverBudgetResults(final MainnetTransactionProcessor transactionProcessor, final long gasUsed) {
      super(transactionProcessor);
      this.gasUsed = gasUsed;
    }

    @Override
    public Optional<TransactionProcessingResult> getProcessingResult(
        final MutableWorldState worldState,
        final Address miningBeneficiary,
        final Transaction transaction,
        final int location,
        final Optional<Counter> confirmedParallelizedTransactionCounter,
        final Optional<Counter> conflictingButCachedTransactionCounter) {
      // gasLimit - gasRemaining, the pre-London block gas accounting, comes to gasUsed.
      return Optional.of(
          TransactionProcessingResult.successful(
              List.of(),
              gasUsed,
              transaction.getGasLimit() - gasUsed,
              Bytes.EMPTY,
              Optional.empty(),
              ValidationResult.valid()));
    }
  }
}
