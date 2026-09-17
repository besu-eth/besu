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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.transactionreceipts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockReceiptsResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.JsonRpcResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionReceiptResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionReceiptsSubscriptionServiceTest {

  @Captor ArgumentCaptor<Long> subscriptionIdCaptor;
  @Captor ArgumentCaptor<JsonRpcResult> responseCaptor;

  private final BlockDataGenerator gen = new BlockDataGenerator();
  private final BlockchainStorage blockchainStorage =
      new KeyValueStoragePrefixedKeyBlockchainStorage(
          new InMemoryKeyValueStorage(),
          new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
          new MainnetBlockHeaderFunctions(),
          false);
  private final Block genesisBlock = gen.genesisBlock();
  private final MutableBlockchain blockchain =
      DefaultBlockchain.createMutable(genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);

  @Spy
  private final SubscriptionManager subscriptionManagerSpy =
      new SubscriptionManager(new NoOpMetricsSystem());

  @Mock ProtocolSchedule protocolSchedule;
  @Mock ProtocolSpec protocolSpec;
  @Mock GasCalculator gasCalculator;

  @Spy
  private final BlockchainQueries blockchainQueriesSpy =
      Mockito.spy(
          new BlockchainQueries(
              protocolSchedule,
              blockchain,
              createInMemoryWorldStateArchive(),
              MiningConfiguration.newDefault()));

  @BeforeEach
  public void before() {
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient().when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);

    final TransactionReceiptsSubscriptionService service =
        new TransactionReceiptsSubscriptionService(
            subscriptionManagerSpy, blockchainQueriesSpy, protocolSchedule);
    blockchain.observeBlockAdded(service);
  }

  @Test
  public void shouldSendReceiptsWhenBlockAddedOnCanonicalChain() {
    final TransactionReceiptsSubscription subscription = createSubscription(null);
    mockSubscriptionManagerNotifyMethod(subscription);

    final Block testBlock = appendBlockWithParent(blockchain, blockchain.getChainHeadBlock());

    verify(subscriptionManagerSpy)
        .sendMessage(subscriptionIdCaptor.capture(), responseCaptor.capture());
    assertThat(subscriptionIdCaptor.getValue()).isEqualTo(subscription.getSubscriptionId());

    final JsonRpcResult result = responseCaptor.getValue();
    assertThat(result).isInstanceOf(BlockReceiptsResult.class);

    final BlockReceiptsResult receiptListResult = (BlockReceiptsResult) result;
    assertThat(receiptListResult.getResults())
        .hasSize(testBlock.getBody().getTransactions().size());
  }

  @Test
  public void shouldNotSendReceiptsWhenBlockAddedIsNotOnCanonicalChain() {
    final TransactionReceiptsSubscription subscription = createSubscription(null);
    mockSubscriptionManagerNotifyMethod(subscription);

    appendBlockWithParent(blockchain, genesisBlock);
    final BlockOptions options =
        new BlockOptions()
            .setBlockNumber(genesisBlock.getHeader().getNumber() + 1)
            .setParentHash(genesisBlock.getHash())
            .setDifficulty(genesisBlock.getHeader().getDifficulty().divide(100L));
    appendBlockWithParent(blockchain, options);

    verify(subscriptionManagerSpy, times(1)).notifySubscribersOnWorkerThread(any(), any(), any());
    verify(subscriptionManagerSpy, times(1))
        .sendMessage(subscriptionIdCaptor.capture(), responseCaptor.capture());
    assertThat(subscriptionIdCaptor.getValue()).isEqualTo(subscription.getSubscriptionId());
  }

  @Test
  public void shouldSendReceiptsForEachBlockWhenReorgOccurs() {
    final TransactionReceiptsSubscription subscription = createSubscription(null);
    mockSubscriptionManagerNotifyMethod(subscription);

    appendBlockWithParent(blockchain, genesisBlock);
    final BlockOptions options =
        new BlockOptions()
            .setBlockNumber(genesisBlock.getHeader().getNumber() + 1)
            .setParentHash(genesisBlock.getHash())
            .setDifficulty(genesisBlock.getHeader().getDifficulty().divide(100L));
    final Block forkBlock = appendBlockWithParent(blockchain, options);
    options.setDifficulty(forkBlock.getHeader().getDifficulty().divide(100L));
    appendBlockWithParent(blockchain, options);
    options.setDifficulty(blockchain.getChainHeadBlock().getHeader().getDifficulty().multiply(2L));
    appendBlockWithParent(blockchain, options);

    verify(subscriptionManagerSpy, times(2)).notifySubscribersOnWorkerThread(any(), any(), any());
    verify(subscriptionManagerSpy, times(2))
        .sendMessage(subscriptionIdCaptor.capture(), responseCaptor.capture());

    final List<JsonRpcResult> results = responseCaptor.getAllValues();
    assertThat(results).hasSize(2);
    assertThat(results.get(0)).isInstanceOf(BlockReceiptsResult.class);
    assertThat(results.get(1)).isInstanceOf(BlockReceiptsResult.class);
  }

  @Test
  public void shouldSendToAllSubscribers() {
    final TransactionReceiptsSubscription subscription1 = createSubscription(1L, null);
    final TransactionReceiptsSubscription subscription2 = createSubscription(2L, null);
    final TransactionReceiptsSubscription subscription3 = createSubscription(3L, null);
    mockSubscriptionManagerNotifyMethod(subscription1, subscription2, subscription3);

    appendBlockWithParent(blockchain, blockchain.getChainHeadBlock());

    verify(subscriptionManagerSpy, times(3))
        .sendMessage(subscriptionIdCaptor.capture(), responseCaptor.capture());
    assertThat(subscriptionIdCaptor.getAllValues())
        .containsExactly(
            subscription1.getSubscriptionId(),
            subscription2.getSubscriptionId(),
            subscription3.getSubscriptionId());
  }

  @Test
  public void shouldFilterByTransactionHashes() {
    final Block testBlock =
        gen.block(
            new BlockOptions()
                .setBlockNumber(blockchain.getChainHeadBlockNumber() + 1)
                .setParentHash(blockchain.getChainHeadHash()));
    assertThat(testBlock.getBody().getTransactions()).isNotEmpty();

    final Hash targetTxHash = testBlock.getBody().getTransactions().get(0).getHash();
    final TransactionReceiptsSubscription subscription =
        createSubscription(1L, List.of(targetTxHash));
    mockSubscriptionManagerNotifyMethod(subscription);

    final List<TransactionReceipt> receipts = gen.receipts(testBlock);
    blockchain.appendBlock(testBlock, receipts);

    verify(subscriptionManagerSpy)
        .sendMessage(subscriptionIdCaptor.capture(), responseCaptor.capture());

    final BlockReceiptsResult receiptListResult = (BlockReceiptsResult) responseCaptor.getValue();
    assertThat(receiptListResult.getResults()).hasSize(1);
    assertThat(receiptListResult.getResults().get(0).getTransactionHash())
        .isEqualTo(targetTxHash.toString());
  }

  @Test
  public void shouldNotSendWhenFilterHasNoMatches() {
    final TransactionReceiptsSubscription subscription =
        createSubscription(
            1L,
            List.of(
                Hash.fromHexString(
                    "0x0000000000000000000000000000000000000000000000000000000000000001")));
    mockSubscriptionManagerNotifyMethod(subscription);

    appendBlockWithParent(blockchain, blockchain.getChainHeadBlock());

    verify(subscriptionManagerSpy, never()).sendMessage(any(), any());
  }

  @Test
  public void shouldSendReceiptsWithCorrectBlockHash() {
    final TransactionReceiptsSubscription subscription = createSubscription(null);
    mockSubscriptionManagerNotifyMethod(subscription);

    final Block testBlock = appendBlockWithParent(blockchain, blockchain.getChainHeadBlock());

    verify(subscriptionManagerSpy)
        .sendMessage(subscriptionIdCaptor.capture(), responseCaptor.capture());

    final BlockReceiptsResult receiptListResult = (BlockReceiptsResult) responseCaptor.getValue();
    for (final TransactionReceiptResult receipt : receiptListResult.getResults()) {
      assertThat(receipt.getBlockHash()).isEqualTo(testBlock.getHash().toString());
    }
  }

  @Test
  public void shouldMemoizeReceiptBuilding() {
    final TransactionReceiptsSubscription subscription1 = createSubscription(1L, null);
    final TransactionReceiptsSubscription subscription2 = createSubscription(2L, null);
    final TransactionReceiptsSubscription subscription3 = createSubscription(3L, null);
    mockSubscriptionManagerNotifyMethod(subscription1, subscription2, subscription3);

    appendBlockWithParent(blockchain, blockchain.getChainHeadBlock());

    verify(subscriptionManagerSpy, times(3))
        .sendMessage(subscriptionIdCaptor.capture(), responseCaptor.capture());

    final BlockReceiptsResult result1 = (BlockReceiptsResult) responseCaptor.getAllValues().get(0);
    final BlockReceiptsResult result2 = (BlockReceiptsResult) responseCaptor.getAllValues().get(1);
    final BlockReceiptsResult result3 = (BlockReceiptsResult) responseCaptor.getAllValues().get(2);

    assertThat(result1.getResults()).isEqualTo(result2.getResults());
    assertThat(result2.getResults()).isEqualTo(result3.getResults());
  }

  private void mockSubscriptionManagerNotifyMethod(
      final TransactionReceiptsSubscription... subscriptions) {
    doAnswer(
            invocation -> {
              Consumer<List<TransactionReceiptsSubscription>> consumer = invocation.getArgument(2);
              consumer.accept(List.of(subscriptions));
              return null;
            })
        .when(subscriptionManagerSpy)
        .notifySubscribersOnWorkerThread(any(), any(), any());
  }

  private TransactionReceiptsSubscription createSubscription(final List<Hash> transactionHashes) {
    return new TransactionReceiptsSubscription(1L, "conn", transactionHashes);
  }

  private TransactionReceiptsSubscription createSubscription(
      final Long id, final List<Hash> transactionHashes) {
    return new TransactionReceiptsSubscription(id, "conn", transactionHashes);
  }

  private Block appendBlockWithParent(final MutableBlockchain blockchain, final Block parent) {
    final BlockOptions options =
        new BlockOptions()
            .setBlockNumber(parent.getHeader().getNumber() + 1)
            .setParentHash(parent.getHash());

    return appendBlockWithParent(blockchain, options);
  }

  private Block appendBlockWithParent(
      final MutableBlockchain blockchain, final BlockOptions options) {
    final Block newBlock = gen.block(options);
    final List<TransactionReceipt> receipts = gen.receipts(newBlock);
    blockchain.appendBlock(newBlock, receipts);

    return newBlock;
  }
}
