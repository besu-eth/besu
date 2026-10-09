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
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.parallelization.MainnetParallelBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.parallelization.PreprocessingContext;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The DAO refund (EIP-779) on the DAO fork block, with and without parallel processing, and under
 * whichever fork is active on that block.
 */
class DaoRecoveryBlockProcessingTest {

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

  private static final String HOMESTEAD_ONLY = "";

  // The DAO block's post-state root: the transfer, the refund and the coinbase reward. Tangerine
  // Whistle's gas changes do not affect a plain transfer, so it is the same under both forks.
  private static final Hash DAO_BLOCK_STATE_ROOT =
      Hash.fromHexString("0x4dc8fe39a09bfe5a1ebc55534ee2786b89252e9cc4fd3153fa5fa69fd247607d");

  @ParameterizedTest(name = "parallel processing enabled: {0}")
  @ValueSource(booleans = {false, true})
  void daoBlockMovesTheDaoBalancesToTheRefundContract(final boolean parallelTxProcessingEnabled) {
    final Hash expectedStateRoot = DAO_BLOCK_STATE_ROOT;
    final ExecutionContextTestFixture ctx = fixture(HOMESTEAD_ONLY, parallelTxProcessingEnabled);
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();

    final BlockProcessingResult result =
        processDaoBlock(ctx, worldState, expectedStateRoot, blockProcessor(ctx));

    assertThat(result.isSuccessful()).as(result.errorMessage.orElse("")).isTrue();
    assertDaoBalancesMoved(worldState);
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(
      strings = {
        // Tangerine Whistle on the DAO fork block, so the DAO_RECOVERY spec governs no block.
        "\"eip150Block\": 1,"
      })
  void daoRefundAppliesWhenAnotherForkSharesTheDaoForkBlock(final String forks) {
    final Hash expectedStateRoot = DAO_BLOCK_STATE_ROOT;
    final ExecutionContextTestFixture ctx = fixture(forks, false);
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();

    final BlockProcessingResult result =
        processDaoBlock(ctx, worldState, expectedStateRoot, blockProcessor(ctx));

    assertThat(result.isSuccessful()).as(result.errorMessage.orElse("")).isTrue();
    assertDaoBalancesMoved(worldState);
  }

  @Test
  void parallelFallbackOnTheDaoBlockKeepsTheDaoTransfers() {
    // The fallback resets the world state and processes the block again. The refund is part of
    // block processing, so the second attempt applies it again.
    final Hash expectedStateRoot = DAO_BLOCK_STATE_ROOT;
    final ExecutionContextTestFixture ctx = fixture(HOMESTEAD_ONLY, true);
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final FailingFirstAttempt blockProcessor =
        new FailingFirstAttempt(
            ctx.getProtocolSchedule().getByBlockHeader(daoBlockHeader(ctx, Hash.ZERO)),
            ctx.getProtocolSchedule());

    final BlockProcessingResult result =
        processDaoBlock(ctx, worldState, expectedStateRoot, blockProcessor);

    assertThat(blockProcessor.failedFirstAttempt).isTrue();
    assertThat(result.isSuccessful()).as(result.errorMessage.orElse("")).isTrue();
    assertDaoBalancesMoved(worldState);
  }

  private static void assertDaoBalancesMoved(final MutableWorldState worldState) {
    assertThat(worldState.get(DAO_ACCOUNT_1).getBalance()).isEqualTo(Wei.ZERO);
    assertThat(worldState.get(DAO_ACCOUNT_2).getBalance()).isEqualTo(Wei.ZERO);
    assertThat(worldState.get(DAO_REFUND_CONTRACT).getBalance()).isEqualTo(Wei.fromEth(3));
  }

  private static BlockProcessor blockProcessor(final ExecutionContextTestFixture ctx) {
    return ctx.getProtocolSchedule()
        .getByBlockHeader(daoBlockHeader(ctx, Hash.ZERO))
        .getBlockProcessor();
  }

  private static BlockHeader daoBlockHeader(
      final ExecutionContextTestFixture ctx, final Hash stateRoot) {
    return new BlockHeaderTestFixture()
        .number(1)
        .parentHash(ctx.getBlockchain().getChainHeadHeader().getHash())
        .coinbase(COINBASE)
        .stateRoot(stateRoot)
        .gasLimit(30_000_000L)
        .buildHeader();
  }

  private static BlockProcessingResult processDaoBlock(
      final ExecutionContextTestFixture ctx,
      final MutableWorldState worldState,
      final Hash stateRoot,
      final BlockProcessor blockProcessor) {
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
        new Block(
            daoBlockHeader(ctx, stateRoot),
            new BlockBody(List.of(transfer), List.of(), Optional.empty()));
    return blockProcessor.processBlock(
        ctx.getProtocolContext(), ctx.getBlockchain(), worldState, block);
  }

  private static ExecutionContextTestFixture fixture(
      final String forks, final boolean parallelTxProcessingEnabled) {
    final GenesisConfig genesis = genesis(forks);
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

  /** Homestead from genesis plus {@code forks}, with the DAO fork at block 1. */
  private static GenesisConfig genesis(final String forks) {
    return GenesisConfig.fromConfig(
        """
        {
          "config": {
            "chainId": 1337,
            "homesteadBlock": 0,
            %s
            "daoForkBlock": 1,
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
                forks,
                SENDER.toHexString(),
                DAO_ACCOUNT_1.toHexString(),
                DAO_ACCOUNT_2.toHexString()));
  }

  /**
   * A parallel processor whose first attempt reports a transaction using more gas than the block
   * allows, so the block falls back to sequential processing.
   */
  private static final class FailingFirstAttempt extends MainnetParallelBlockProcessor {

    private boolean failedFirstAttempt;

    FailingFirstAttempt(final ProtocolSpec spec, final ProtocolSchedule protocolSchedule) {
      super(
          spec.getTransactionProcessor(),
          spec.getTransactionReceiptFactory(),
          spec.getMiningBeneficiaryCalculator(),
          protocolSchedule,
          BalConfiguration.DEFAULT,
          new NoOpMetricsSystem());
    }

    @Override
    protected TransactionProcessingResult getTransactionProcessingResult(
        final Optional<PreprocessingContext> preProcessingContext,
        final BlockProcessingContext blockProcessingContext,
        final WorldUpdater transactionUpdater,
        final Wei blobGasPrice,
        final Address miningBeneficiary,
        final Transaction transaction,
        final int location,
        final BlockHashLookup blockHashLookup,
        final Optional<AccessLocationTracker> accessLocationTracker) {
      if (!failedFirstAttempt) {
        failedFirstAttempt = true;
        final long gasUsed = blockProcessingContext.getBlockHeader().getGasLimit() + 1;
        // gasLimit - gasRemaining, the pre-London block gas accounting, comes to gasUsed.
        return TransactionProcessingResult.successful(
            List.of(),
            gasUsed,
            transaction.getGasLimit() - gasUsed,
            Bytes.EMPTY,
            Optional.empty(),
            ValidationResult.valid());
      }
      return super.getTransactionProcessingResult(
          preProcessingContext,
          blockProcessingContext,
          transactionUpdater,
          blobGasPrice,
          miningBeneficiary,
          transaction,
          location,
          blockHashLookup,
          accessLocationTracker);
    }
  }
}
