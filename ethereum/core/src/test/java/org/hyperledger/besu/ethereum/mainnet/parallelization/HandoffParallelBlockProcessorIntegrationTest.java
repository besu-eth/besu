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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.GENESIS_CONFIG;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.PARALLEL_TEST_CONTRACT;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link HandoffTransactionProcessor}, which processes the blocks without a
 * block access list: the blocks run before Amsterdam, on EVM v1 and on EVM v2, and each is checked
 * against sequential processing. The refreshes run either on their worker, whenever it gets to
 * them, or before each transaction, so that every invalidated result is refreshed on the writes
 * before it.
 */
class HandoffParallelBlockProcessorIntegrationTest {

  private static final BalConfiguration CONFIG =
      ImmutableBalConfiguration.builder().isPerfectParallelizationEnabled(false).build();

  /** Funded senders besides the genesis accounts, so that a block can have many senders. */
  static final List<KeyPair> SENDERS = new ArrayList<>();

  static {
    for (int i = 1; i <= 6; i++) {
      SENDERS.add(
          ParallelBlockProcessorTestSupport.generateKeyPair(
              String.format("%064x", BigInteger.valueOf(0x7e57_0000L + i))));
    }
  }

  /** The integration test genesis before Amsterdam, with the extra senders funded. */
  static ExecutionContextTestFixture createOsakaContext(final boolean evmV2) {
    final ObjectNode genesis;
    try (InputStream in =
        HandoffParallelBlockProcessorIntegrationTest.class.getResourceAsStream(GENESIS_CONFIG)) {
      genesis = (ObjectNode) new ObjectMapper().readTree(in);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }

    ((ObjectNode) genesis.get("config")).remove("amsterdamTime");
    final ObjectNode alloc = (ObjectNode) genesis.get("alloc");
    for (final KeyPair sender : SENDERS) {
      final ObjectNode account =
          alloc.putObject(Util.publicKeyToAddress(sender.getPublicKey()).toHexString());
      account.put("balance", "0x3635c9adc5dea00000");
    }

    final GenesisConfig genesisConfig = GenesisConfig.fromConfig(genesis);
    final ExecutionContextTestFixture.Builder fixture =
        ExecutionContextTestFixture.builder(genesisConfig)
            .dataStorageFormat(DataStorageFormat.BONSAI);
    if (evmV2) {
      fixture.protocolSchedule(
          new ProtocolScheduleBuilder(
                  genesisConfig.getConfigOptions(),
                  Optional.of(BigInteger.valueOf(42)),
                  ProtocolSpecAdapters.create(0, Function.identity()),
                  false,
                  new EvmConfiguration(
                      32_000L, EvmConfiguration.WorldUpdaterMode.STACKED, true, true),
                  MiningConfiguration.MINING_DISABLED,
                  new BadBlockManager(),
                  false,
                  ImmutableBalConfiguration.builder().build(),
                  new NoOpMetricsSystem())
              .createProtocolSchedule());
    }

    return fixture.build();
  }

  private static String variant(final boolean evmV2, final String refreshes) {
    return "Handoff (EVM " + (evmV2 ? "v2" : "v1") + ", before Amsterdam, " + refreshes + ")";
  }

  @AfterEach
  void refreshOnWorker() {
    HandoffTransactionProcessor.refreshSynchronously(false);
  }

  @Nested
  @DisplayName("Simple Transfers, refreshed on the worker")
  class SimpleTransfers extends AbstractSimpleTransferTest {
    boolean onEvmV2() {
      return false;
    }

    @Override
    protected String getVariantName() {
      return variant(onEvmV2(), "refreshed on the worker");
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return CONFIG;
    }

    @Override
    protected boolean buildsBlockAccessLists() {
      return false;
    }

    @Override
    protected ExecutionContextTestFixture createFreshContext() {
      return createOsakaContext(onEvmV2());
    }
  }

  @Nested
  @DisplayName("Simple Transfers, refreshed on the worker, on EVM v2")
  class SimpleTransfersOnEvmV2 extends SimpleTransfers {
    @Override
    boolean onEvmV2() {
      return true;
    }
  }

  @Nested
  @DisplayName("Contract Storage, refreshed on the worker")
  class ContractStorage extends AbstractContractStorageTest {
    boolean onEvmV2() {
      return false;
    }

    @Override
    protected String getVariantName() {
      return variant(onEvmV2(), "refreshed on the worker");
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return CONFIG;
    }

    @Override
    protected boolean buildsBlockAccessLists() {
      return false;
    }

    @Override
    protected ExecutionContextTestFixture createFreshContext() {
      return createOsakaContext(onEvmV2());
    }
  }

  @Nested
  @DisplayName("Contract Storage, refreshed on the worker, on EVM v2")
  class ContractStorageOnEvmV2 extends ContractStorage {
    @Override
    boolean onEvmV2() {
      return true;
    }
  }

  @Nested
  @DisplayName("Storage Dependency, refreshed on the worker")
  class StorageDependency extends AbstractStorageDependencyTest {
    boolean onEvmV2() {
      return false;
    }

    @Override
    protected String getVariantName() {
      return variant(onEvmV2(), "refreshed on the worker");
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return CONFIG;
    }

    @Override
    protected boolean buildsBlockAccessLists() {
      return false;
    }

    @Override
    protected ExecutionContextTestFixture createFreshContext() {
      return createOsakaContext(onEvmV2());
    }
  }

  @Nested
  @DisplayName("Storage Dependency, refreshed on the worker, on EVM v2")
  class StorageDependencyOnEvmV2 extends StorageDependency {
    @Override
    boolean onEvmV2() {
      return true;
    }
  }

  @Nested
  @DisplayName("Contract Storage, refreshed before each transaction")
  class ContractStorageRefreshedInOrder extends ContractStorage {
    @BeforeEach
    void refreshInOrder() {
      HandoffTransactionProcessor.refreshSynchronously(true);
    }

    @Override
    protected String getVariantName() {
      return variant(onEvmV2(), "refreshed before each transaction");
    }
  }

  @Nested
  @DisplayName("Contract Storage, refreshed before each transaction, on EVM v2")
  class ContractStorageRefreshedInOrderOnEvmV2 extends ContractStorageRefreshedInOrder {
    @Override
    boolean onEvmV2() {
      return true;
    }
  }

  @Nested
  @DisplayName("Storage Dependency, refreshed before each transaction")
  class StorageDependencyRefreshedInOrder extends StorageDependency {
    @BeforeEach
    void refreshInOrder() {
      HandoffTransactionProcessor.refreshSynchronously(true);
    }

    @Override
    protected String getVariantName() {
      return variant(onEvmV2(), "refreshed before each transaction");
    }
  }

  @Nested
  @DisplayName("Storage Dependency, refreshed before each transaction, on EVM v2")
  class StorageDependencyRefreshedInOrderOnEvmV2 extends StorageDependencyRefreshedInOrder {
    @Override
    boolean onEvmV2() {
      return true;
    }
  }

  @Nested
  @DisplayName("Refreshed results")
  class RefreshedResults extends AbstractParallelBlockProcessorIntegrationTest {
    private final Address counter = Address.fromHexStringStrict(PARALLEL_TEST_CONTRACT);

    boolean onEvmV2() {
      return false;
    }

    @BeforeEach
    void refreshInOrder() {
      HandoffTransactionProcessor.refreshSynchronously(true);
    }

    @Override
    protected String getVariantName() {
      return variant(onEvmV2(), "refreshed before each transaction");
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return CONFIG;
    }

    @Override
    protected boolean buildsBlockAccessLists() {
      return false;
    }

    @Override
    protected ExecutionContextTestFixture createFreshContext() {
      return createOsakaContext(onEvmV2());
    }

    private Transaction increment(final KeyPair sender) {
      return createContractCallTransaction(0, counter, "incrementSlot1", sender, Optional.empty());
    }

    private Transaction set(final KeyPair sender, final int value) {
      return createContractCallTransaction(0, counter, "setSlot1", sender, Optional.of(value));
    }

    @Test
    @DisplayName("Increments of one slot from different senders are all taken, refreshed in turn")
    void incrementsFromDifferentSendersAreTakenRefreshed() {
      final Transaction[] increments =
          SENDERS.stream().map(this::increment).toArray(Transaction[]::new);

      final ComparisonResult result = executeAndCompare(Wei.of(5), increments);

      assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), counter, 0);
      assertContractStorage(result.parWorldState(), counter, 0, 10 + increments.length);
      // the first runs on the parent state, each later one on what the one before it leaves
      assertThat(result.parResult().getNbParallelizedTransactions()).hasValue(increments.length);
    }

    @Test
    @DisplayName("A result refreshed on a write the block does not make is executed instead")
    void refreshOnAWriteTheBlockDoesNotMakeIsNotTaken() {
      // the second sender sets the slot before the third increments it, so the third's refresh
      // must follow the set, not the increment before it
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5),
              increment(SENDERS.get(0)),
              set(SENDERS.get(1), 500),
              increment(SENDERS.get(2)));

      assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), counter, 0);
      assertContractStorage(result.parWorldState(), counter, 0, 501);
    }

    @Test
    @DisplayName("Interleaved sets and increments of one slot match sequential execution")
    void interleavedSetsAndIncrementsMatchSequential() {
      final List<Transaction> transactions = new ArrayList<>();
      for (int i = 0; i < SENDERS.size(); i++) {
        transactions.add(i % 2 == 0 ? increment(SENDERS.get(i)) : set(SENDERS.get(i), 100 + i));
      }

      final ComparisonResult result =
          executeAndCompare(Wei.of(5), transactions.toArray(Transaction[]::new));

      assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), counter, 0);
      assertContractStorage(result.parWorldState(), counter, 0, 105);
    }
  }

  @Nested
  @DisplayName("Refreshed results on EVM v2")
  class RefreshedResultsOnEvmV2 extends RefreshedResults {
    @Override
    boolean onEvmV2() {
      return true;
    }
  }
}
