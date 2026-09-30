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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.testing;

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
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.GasPricePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidatorFactory;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.time.Clock;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestingCommitBlockV1IntegrationTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private static final GenesisConfig GENESIS_CONFIG =
      GenesisConfig.fromResource("/testing-commit-block-genesis.json");

  private final DeterministicEthScheduler ethScheduler = new DeterministicEthScheduler();

  @Test
  void shouldCommitEmptyBlockAndAdvanceChainHead() {
    final TestContext ctx = buildTestContext();
    final MutableBlockchain blockchain = ctx.fixture.getBlockchain();
    final long genesisTimestamp = blockchain.getChainHeadHeader().getTimestamp();

    final JsonRpcResponse response =
        ctx.method.response(buildRequest(genesisTimestamp + 1, new String[0], null));

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final String returnedHash = (String) ((JsonRpcSuccessResponse) response).getResult();

    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
    assertThat(blockchain.getChainHeadHash().toHexString()).isEqualTo(returnedHash);
  }

  @Test
  void shouldCommitTwoConsecutiveBlocksAndAdvanceChainHeadTwice() {
    final TestContext ctx = buildTestContext();
    final MutableBlockchain blockchain = ctx.fixture.getBlockchain();
    final long t0 = blockchain.getChainHeadHeader().getTimestamp();

    final JsonRpcResponse r1 = ctx.method.response(buildRequest(t0 + 1, new String[0], null));
    assertThat(r1).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
    final String hash1 = (String) ((JsonRpcSuccessResponse) r1).getResult();

    final JsonRpcResponse r2 = ctx.method.response(buildRequest(t0 + 2, new String[0], null));
    assertThat(r2).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(2L);
    final String hash2 = (String) ((JsonRpcSuccessResponse) r2).getResult();

    assertThat(hash1).isNotEqualTo(hash2);
    assertThat(blockchain.getChainHeadHash().toHexString()).isEqualTo(hash2);
  }

  @Test
  void shouldCommitBlockWithTransactionAndUpdateWorldState() {
    final TestContext ctx = buildTestContext();
    final MutableBlockchain blockchain = ctx.fixture.getBlockchain();

    final List<GenesisAccount> accounts =
        GENESIS_CONFIG.streamAllocations().filter(ga -> ga.privateKey() != null).toList();
    final GenesisAccount sender = accounts.get(0);
    final GenesisAccount recipient = accounts.get(1);

    final KeyPair keyPair =
        SIGNATURE_ALGORITHM.createKeyPair(SECPPrivateKey.create(sender.privateKey(), "ECDSA"));
    final Transaction tx =
        new TransactionTestFixture()
            .sender(sender.address())
            .to(Optional.of(recipient.address()))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(sender.nonce())
            .createTransaction(keyPair);

    final long genesisTimestamp = blockchain.getChainHeadHeader().getTimestamp();
    final JsonRpcResponse response =
        ctx.method.response(
            buildRequest(genesisTimestamp + 1, new String[] {tx.encoded().toHexString()}, null));

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final String returnedHash = (String) ((JsonRpcSuccessResponse) response).getResult();

    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
    assertThat(blockchain.getChainHeadHash().toHexString()).isEqualTo(returnedHash);
    assertThat(
            blockchain
                .getBlockByHash(blockchain.getChainHeadHash())
                .map(b -> b.getBody().getTransactions())
                .orElseThrow())
        .hasSize(1);
  }

  // ---- helpers ----

  private JsonRpcRequestContext buildRequest(
      final long timestamp, final Object transactions, final Object extraData) {
    final Map<String, Object> attrs = new LinkedHashMap<>();
    attrs.put("timestamp", Bytes.ofUnsignedLong(timestamp).toQuantityHexString());
    attrs.put("prevRandao", Bytes32.ZERO.toHexString());
    attrs.put("suggestedFeeRecipient", "0x0000000000000000000000000000000000000000");
    attrs.put("withdrawals", Collections.emptyList());
    attrs.put("parentBeaconBlockRoot", Bytes32.ZERO.toHexString());
    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0",
            RpcMethod.TESTING_COMMIT_BLOCK_V1.getMethodName(),
            new Object[] {attrs, transactions, extraData}));
  }

  private record TestContext(ExecutionContextTestFixture fixture, TestingCommitBlockV1 method) {}

  private TestContext buildTestContext() {
    final var alwaysValidFactory = mock(TransactionValidatorFactory.class);
    when(alwaysValidFactory.get()).thenReturn(new AlwaysValidTransactionValidator());

    final ProtocolSpecAdapters adapters =
        ProtocolSpecAdapters.create(
            0,
            specBuilder -> {
              specBuilder.isReplayProtectionSupported(true);
              specBuilder.transactionValidatorFactoryBuilder(
                  (evm, gasLimitCalculator, feeMarket) -> alwaysValidFactory);
              return specBuilder;
            });

    final ExecutionContextTestFixture fixture =
        ExecutionContextTestFixture.builder(GENESIS_CONFIG)
            .protocolSchedule(
                new ProtocolScheduleBuilder(
                        GENESIS_CONFIG.getConfigOptions(),
                        Optional.of(BigInteger.valueOf(42)),
                        adapters,
                        false,
                        EvmConfiguration.DEFAULT,
                        MiningConfiguration.MINING_DISABLED,
                        new BadBlockManager(),
                        false,
                        ImmutableBalConfiguration.builder().build(),
                        new NoOpMetricsSystem())
                    .createProtocolSchedule())
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();

    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(100).build();
    final GasPricePendingTransactionsSorter sorter =
        new GasPricePendingTransactionsSorter(
            poolConf,
            Clock.systemUTC(),
            new NoOpMetricsSystem(),
            Suppliers.ofInstance(fixture.getBlockchain().getChainHeadHeader()));

    final EthContext ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    final TransactionPool txPool =
        new TransactionPool(
            () -> sorter,
            fixture.getProtocolSchedule(),
            fixture.getProtocolContext(),
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(new NoOpMetricsSystem()),
            poolConf,
            new BlobCache());
    txPool.setEnabled();

    final MiningConfiguration miningConfig =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                MutableInitValues.builder()
                    .extraData(Bytes.EMPTY)
                    .minTransactionGasPrice(Wei.ONE)
                    .coinbase(Address.ZERO)
                    .build())
            .build();

    final TestingCommitBlockV1 method =
        new TestingCommitBlockV1(
            fixture.getProtocolContext(),
            fixture.getProtocolSchedule(),
            miningConfig,
            txPool,
            ethScheduler);

    return new TestContext(fixture, method);
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
