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
package org.hyperledger.besu.ethereum.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator.BlockCreationResult;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.AbstractPendingTransactionsSorter;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.GasPricePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidatorFactory;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Integration tests for block creation with and without Block Access List (BAL). These tests verify
 * the full block creation flow similar to what testing_buildBlockV1 would do.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestingBuildBlockIntegrationTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  protected final GenesisConfig genesisConfig =
      GenesisConfig.fromResource("/block-creation-genesis.json");

  protected final List<GenesisAccount> accounts =
      genesisConfig.streamAllocations().filter(ga -> ga.privateKey() != null).toList();

  protected EthScheduler ethScheduler = new DeterministicEthScheduler();

  @Test
  void shouldCreateBlockWithBAL() {
    final TestContext context = createTestContextWithBAL();
    final GenesisAccount sender = accounts.get(1);
    final GenesisAccount recipient = accounts.get(2);
    final KeyPair keyPair =
        SIGNATURE_ALGORITHM.createKeyPair(SECPPrivateKey.create(sender.privateKey(), "ECDSA"));

    final Transaction txn =
        new TransactionTestFixture()
            .sender(sender.address())
            .to(Optional.of(recipient.address()))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(sender.nonce())
            .createTransaction(keyPair);

    final BlockCreationResult result =
        context.blockCreator.createBlock(
            Optional.of(List.of(txn)),
            Optional.empty(),
            context.parentHeader.getTimestamp() + 1L,
            context.parentHeader);

    assertThat(result).isNotNull();
    assertThat(result.getBlock()).isNotNull();
    assertThat(result.getBlock().getBody().getTransactions()).hasSize(1);

    final Optional<BlockAccessList> maybeBAL = result.getBlockAccessList();
    assertThat(maybeBAL).isPresent();

    final BlockAccessList bal = maybeBAL.get();
    assertThat(bal.accountChanges()).isNotEmpty();

    final List<AccountChanges> accountChanges = bal.accountChanges();
    assertThat(accountChanges.size()).isGreaterThanOrEqualTo(2);

    boolean foundSender = false;
    boolean foundRecipient = false;
    for (AccountChanges ac : accountChanges) {
      if (ac.address().equals(sender.address())) {
        foundSender = true;
        assertThat(ac.balanceChanges()).isNotEmpty();
        assertThat(ac.nonceChanges()).isNotEmpty();
      }
      if (ac.address().equals(recipient.address())) {
        foundRecipient = true;
        assertThat(ac.balanceChanges()).isNotEmpty();
      }
    }
    assertThat(foundSender).isTrue();
    assertThat(foundRecipient).isTrue();
  }

  @Test
  void shouldCreateBlockWithoutBAL() {
    final TestContext context = createTestContextWithoutBAL();
    final GenesisAccount sender = accounts.get(1);
    final GenesisAccount recipient = accounts.get(2);
    final KeyPair keyPair =
        SIGNATURE_ALGORITHM.createKeyPair(SECPPrivateKey.create(sender.privateKey(), "ECDSA"));

    final Transaction txn =
        new TransactionTestFixture()
            .sender(sender.address())
            .to(Optional.of(recipient.address()))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(sender.nonce())
            .createTransaction(keyPair);

    final BlockCreationResult result =
        context.blockCreator.createBlock(
            Optional.of(List.of(txn)),
            Optional.empty(),
            context.parentHeader.getTimestamp() + 1L,
            context.parentHeader);

    assertThat(result).isNotNull();
    assertThat(result.getBlock()).isNotNull();
    assertThat(result.getBlock().getBody().getTransactions()).hasSize(1);

    final Optional<BlockAccessList> maybeBAL = result.getBlockAccessList();
    assertThat(maybeBAL).isEmpty();
  }

  @Test
  void shouldCreateEmptyBlockWithBAL() {
    final TestContext context = createTestContextWithBAL();

    final BlockCreationResult result =
        context.blockCreator.createBlock(
            Optional.of(Collections.emptyList()),
            Optional.empty(),
            context.parentHeader.getTimestamp() + 1L,
            context.parentHeader);

    assertThat(result).isNotNull();
    assertThat(result.getBlock()).isNotNull();
    assertThat(result.getBlock().getBody().getTransactions()).isEmpty();

    final Optional<BlockAccessList> maybeBAL = result.getBlockAccessList();
    assertThat(maybeBAL).isPresent();
  }

  @Test
  void shouldCreateEmptyBlockWithoutBAL() {
    final TestContext context = createTestContextWithoutBAL();

    final BlockCreationResult result =
        context.blockCreator.createBlock(
            Optional.of(Collections.emptyList()),
            Optional.empty(),
            context.parentHeader.getTimestamp() + 1L,
            context.parentHeader);

    assertThat(result).isNotNull();
    assertThat(result.getBlock()).isNotNull();
    assertThat(result.getBlock().getBody().getTransactions()).isEmpty();

    final Optional<BlockAccessList> maybeBAL = result.getBlockAccessList();
    assertThat(maybeBAL).isEmpty();
  }

  @Test
  void shouldHaveValidBALStructure() {
    final TestContext context = createTestContextWithBAL();
    final GenesisAccount sender = accounts.get(1);
    final GenesisAccount recipient = accounts.get(2);
    final KeyPair keyPair =
        SIGNATURE_ALGORITHM.createKeyPair(SECPPrivateKey.create(sender.privateKey(), "ECDSA"));

    final Transaction txn =
        new TransactionTestFixture()
            .sender(sender.address())
            .to(Optional.of(recipient.address()))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(sender.nonce())
            .createTransaction(keyPair);

    final BlockCreationResult result =
        context.blockCreator.createBlock(
            Optional.of(List.of(txn)),
            Optional.empty(),
            context.parentHeader.getTimestamp() + 1L,
            context.parentHeader);

    final Optional<BlockAccessList> maybeBAL = result.getBlockAccessList();
    assertThat(maybeBAL).isPresent();

    final BlockAccessList bal = maybeBAL.get();
    assertThat(bal.accountChanges()).isNotNull();
    assertThat(bal.accountChanges()).isNotEmpty();

    for (AccountChanges ac : bal.accountChanges()) {
      assertThat(ac.address()).isNotNull();
      assertThat(ac.balanceChanges()).isNotNull();
      assertThat(ac.nonceChanges()).isNotNull();
      assertThat(ac.storageChanges()).isNotNull();
      assertThat(ac.storageReads()).isNotNull();
      assertThat(ac.codeChanges()).isNotNull();
    }
  }

  @Test
  void shouldIncludeMultipleTransactionsInBAL() {
    final TestContext context = createTestContextWithBAL();
    final GenesisAccount sender = accounts.get(1);
    final GenesisAccount recipient1 = accounts.get(2);
    final GenesisAccount recipient2 = accounts.get(0);
    final KeyPair keyPair =
        SIGNATURE_ALGORITHM.createKeyPair(SECPPrivateKey.create(sender.privateKey(), "ECDSA"));

    final Transaction txn1 =
        new TransactionTestFixture()
            .sender(sender.address())
            .to(Optional.of(recipient1.address()))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(sender.nonce())
            .createTransaction(keyPair);

    final Transaction txn2 =
        new TransactionTestFixture()
            .sender(sender.address())
            .to(Optional.of(recipient2.address()))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(sender.nonce() + 1)
            .createTransaction(keyPair);

    final BlockCreationResult result =
        context.blockCreator.createBlock(
            Optional.of(List.of(txn1, txn2)),
            Optional.empty(),
            context.parentHeader.getTimestamp() + 1L,
            context.parentHeader);

    assertThat(result).isNotNull();
    assertThat(result.getBlock()).isNotNull();
    assertThat(result.getBlock().getBody().getTransactions()).hasSize(2);

    final Optional<BlockAccessList> maybeBAL = result.getBlockAccessList();
    assertThat(maybeBAL).isPresent();

    final BlockAccessList bal = maybeBAL.get();
    assertThat(bal.accountChanges().size()).isGreaterThanOrEqualTo(3);
  }

  @Test
  void shouldTrackNonceChangesInBAL() {
    final TestContext context = createTestContextWithBAL();
    final GenesisAccount sender = accounts.get(1);
    final GenesisAccount recipient = accounts.get(2);
    final KeyPair keyPair =
        SIGNATURE_ALGORITHM.createKeyPair(SECPPrivateKey.create(sender.privateKey(), "ECDSA"));

    final Transaction txn =
        new TransactionTestFixture()
            .sender(sender.address())
            .to(Optional.of(recipient.address()))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(sender.nonce())
            .createTransaction(keyPair);

    final BlockCreationResult result =
        context.blockCreator.createBlock(
            Optional.of(List.of(txn)),
            Optional.empty(),
            context.parentHeader.getTimestamp() + 1L,
            context.parentHeader);

    final Optional<BlockAccessList> maybeBAL = result.getBlockAccessList();
    assertThat(maybeBAL).isPresent();

    final BlockAccessList bal = maybeBAL.get();
    final Optional<AccountChanges> senderChanges =
        bal.accountChanges().stream()
            .filter(ac -> ac.address().equals(sender.address()))
            .findFirst();

    assertThat(senderChanges).isPresent();
    assertThat(senderChanges.get().nonceChanges()).isNotEmpty();
    assertThat(senderChanges.get().nonceChanges().get(0).newNonce()).isEqualTo(sender.nonce() + 1);
  }

  record TestContext(AbstractBlockCreator blockCreator, BlockHeader parentHeader) {}

  private TestContext createTestContextWithBAL() {
    return createTestContext(true);
  }

  private TestContext createTestContextWithoutBAL() {
    return createTestContext(false);
  }

  private TestContext createTestContext(final boolean withBAL) {
    final var alwaysValidTransactionValidatorFactory = mock(TransactionValidatorFactory.class);
    when(alwaysValidTransactionValidatorFactory.get())
        .thenReturn(new AlwaysValidTransactionValidator());

    final ProtocolSpecAdapters protocolSpecAdapters =
        ProtocolSpecAdapters.create(
            0,
            specBuilder -> {
              specBuilder.isReplayProtectionSupported(true);
              if (withBAL) {
                specBuilder.blockAccessListFactory(new BlockAccessListFactory());
              }
              specBuilder.transactionValidatorFactoryBuilder(
                  (evm, gasLimitCalculator, feeMarket) -> alwaysValidTransactionValidatorFactory);
              return specBuilder;
            });

    final ExecutionContextTestFixture executionContextTestFixture =
        ExecutionContextTestFixture.builder(genesisConfig)
            .protocolSchedule(
                new ProtocolScheduleBuilder(
                        genesisConfig.getConfigOptions(),
                        Optional.of(BigInteger.valueOf(42)),
                        protocolSpecAdapters,
                        false,
                        EvmConfiguration.DEFAULT,
                        MiningConfiguration.MINING_DISABLED,
                        new BadBlockManager(),
                        false,
                        ImmutableBalConfiguration.builder()
                            .isBalStateRootTrusted(withBAL)
                            .isPerfectParallelizationEnabled(withBAL)
                            .build(),
                        new NoOpMetricsSystem())
                    .createProtocolSchedule())
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();

    final MutableBlockchain blockchain = executionContextTestFixture.getBlockchain();
    final BlockHeader parentHeader = blockchain.getChainHeadHeader();
    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(100).build();
    final AbstractPendingTransactionsSorter sorter =
        new GasPricePendingTransactionsSorter(
            poolConf,
            Clock.systemUTC(),
            new NoOpMetricsSystem(),
            Suppliers.ofInstance(parentHeader));

    final EthContext ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    final TransactionPool transactionPool =
        new TransactionPool(
            () -> sorter,
            executionContextTestFixture.getProtocolSchedule(),
            executionContextTestFixture.getProtocolContext(),
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(new NoOpMetricsSystem()),
            poolConf,
            new BlobCache());
    transactionPool.setEnabled();

    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                MutableInitValues.builder()
                    .extraData(Bytes.fromHexString("deadbeef"))
                    .minTransactionGasPrice(Wei.ONE)
                    .coinbase(Address.ZERO)
                    .build())
            .build();

    final TestBlockCreator blockCreator =
        new TestBlockCreator(
            miningConfiguration,
            (__, ___) -> Address.ZERO,
            __ -> Bytes.fromHexString("deadbeef"),
            transactionPool,
            executionContextTestFixture.getProtocolContext(),
            executionContextTestFixture.getProtocolSchedule(),
            ethScheduler);

    return new TestContext(blockCreator, parentHeader);
  }

  static class TestBlockCreator extends AbstractBlockCreator {

    protected TestBlockCreator(
        final MiningConfiguration miningConfiguration,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final ExtraDataCalculator extraDataCalculator,
        final TransactionPool transactionPool,
        final org.hyperledger.besu.ethereum.ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final EthScheduler ethScheduler) {
      super(
          miningConfiguration,
          miningBeneficiaryCalculator,
          extraDataCalculator,
          transactionPool,
          protocolContext,
          protocolSchedule,
          ethScheduler);
    }

    @Override
    protected BlockHeader createFinalBlockHeader(final SealableBlockHeader sealableBlockHeader) {
      return BlockHeaderBuilder.create()
          .difficulty(Difficulty.ZERO)
          .populateFrom(sealableBlockHeader)
          .mixHash(org.hyperledger.besu.datatypes.Hash.EMPTY)
          .nonce(0L)
          .blockHeaderFunctions(blockHeaderFunctions)
          .buildBlockHeader();
    }
  }

  static class AlwaysValidTransactionValidator implements TransactionValidator {

    @Override
    public ValidationResult<TransactionInvalidReason> validate(
        final Transaction transaction,
        final Optional<Wei> baseFee,
        final Optional<Wei> blobBaseFee,
        final TransactionValidationParams transactionValidationParams) {
      return ValidationResult.valid();
    }

    @Override
    public ValidationResult<TransactionInvalidReason> validateForSender(
        final Transaction transaction,
        final Account sender,
        final TransactionValidationParams validationParams) {
      return ValidationResult.valid();
    }
  }
}
