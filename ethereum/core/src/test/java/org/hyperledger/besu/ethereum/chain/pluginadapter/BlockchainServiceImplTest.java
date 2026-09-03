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
package org.hyperledger.besu.ethereum.chain.pluginadapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.PropagatedBlockSource;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.data.AddedBlockContext;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.LogWithMetadata;
import org.hyperledger.besu.plugin.data.PropagatedBlockContext;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.util.Subscribers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockchainServiceImplTest {

  private final BlockDataGenerator gen = new BlockDataGenerator();
  private final BadBlockManager badBlockManager = new BadBlockManager();
  private final FakePropagatedBlockSource propagatedBlocks = new FakePropagatedBlockSource();
  private MutableBlockchain blockchain;
  private BlockchainServiceImpl service;

  @BeforeEach
  void setUp() {
    blockchain = InMemoryKeyValueStorageProvider.createInMemoryBlockchain(gen.genesisBlock());
    service = new BlockchainServiceImpl();
    service.init(blockchain, mock(ProtocolSchedule.class), propagatedBlocks, badBlockManager);
  }

  @Test
  void blockAddedFiresUntilClosed() {
    final AtomicReference<AddedBlockContext> result = new AtomicReference<>();
    final Subscription subscription = service.subscribeBlockAdded(result::set);

    final Block block = appendBlockOnGenesis();
    assertThat(result.get()).isNotNull();
    assertThat(result.get().getBlockHeader()).isEqualTo(block.getHeader());
    assertThat(result.get().getEventType()).isEqualTo(AddedBlockContext.EventType.HEAD_ADVANCED);

    result.set(null);
    subscription.close();
    appendBlockOn(block);
    assertThat(result.get()).isNull();
  }

  @Test
  void blockReorgFiresUntilClosed() {
    final AtomicReference<AddedBlockContext> result = new AtomicReference<>();
    final Subscription subscription = service.subscribeBlockReorg(result::set);

    final Block reorgBlock = appendReorg();
    assertThat(result.get()).isNotNull();
    assertThat(result.get().getBlockHeader()).isEqualTo(reorgBlock.getHeader());
    assertThat(result.get().getEventType()).isEqualTo(AddedBlockContext.EventType.CHAIN_REORG);

    result.set(null);
    subscription.close();
    appendReorgOn(reorgBlock);
    assertThat(result.get()).isNull();
  }

  @Test
  void closingReorgSubscriptionLeavesBlockAddedSubscriptionAlone() {
    final AtomicReference<AddedBlockContext> added = new AtomicReference<>();
    final AtomicReference<AddedBlockContext> reorged = new AtomicReference<>();
    service.subscribeBlockAdded(added::set);
    service.subscribeBlockReorg(reorged::set).close();

    appendReorg();
    assertThat(added.get()).isNotNull();
    assertThat(reorged.get()).isNull();
  }

  @Test
  void logsFireForMatchingFilterUntilClosed() {
    final Block block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash())
                .setBlockNumber(1));
    final List<TransactionReceipt> receipts =
        block.getBody().getTransactions().stream().map(tx -> gen.receipt(gen.logs(2, 2))).toList();
    final Log sample = receipts.get(0).getLogsList().get(0);
    final Address matchingAddress = sample.getLogger();
    final LogTopic matchingTopic = sample.getTopics().get(0);

    final List<LogWithMetadata> all = new ArrayList<>();
    final List<LogWithMetadata> byAddress = new ArrayList<>();
    final List<LogWithMetadata> byTopic = new ArrayList<>();
    final List<LogWithMetadata> none = new ArrayList<>();
    final Subscription subscription = service.subscribeLogs(List.of(), List.of(), all::add);
    service.subscribeLogs(List.of(matchingAddress), List.of(), byAddress::add);
    service.subscribeLogs(
        List.of(),
        List.of(List.of(Bytes32.wrap(matchingTopic.getBytes().toArray()))),
        byTopic::add);
    service.subscribeLogs(List.of(Address.fromHexString("0x01")), List.of(), none::add);

    blockchain.appendBlock(block, receipts);
    assertThat(all).isNotEmpty();
    assertThat(byAddress).isNotEmpty().allMatch(log -> log.getLogger().equals(matchingAddress));
    assertThat(byTopic).isNotEmpty().allMatch(log -> log.getTopics().get(0).equals(matchingTopic));
    assertThat(none).isEmpty();

    all.clear();
    subscription.close();
    appendBlockOn(block);
    assertThat(all).isEmpty();
  }

  @Test
  void badBlockFiresUntilClosed() {
    final AtomicReference<BlockHeader> header = new AtomicReference<>();
    final AtomicReference<org.hyperledger.besu.plugin.data.BadBlockCause> cause =
        new AtomicReference<>();
    final Subscription subscription =
        service.subscribeBadBlock(
            (badHeader, badCause) -> {
              header.set(badHeader);
              cause.set(badCause);
            });

    final BadBlockCause blockCause = BadBlockCause.fromValidationFailure("failed");
    final Block bad = gen.block(new BlockDataGenerator.BlockOptions());
    badBlockManager.addBadBlock(bad, blockCause);
    assertThat(header.get()).isEqualTo(bad.getHeader());
    assertThat(cause.get()).isEqualTo(blockCause);

    header.set(null);
    subscription.close();
    badBlockManager.addBadHeader(
        gen.block(new BlockDataGenerator.BlockOptions()).getHeader(),
        BadBlockCause.fromValidationFailure("again"));
    assertThat(header.get()).isNull();
  }

  @Test
  void blockPropagatedFiresUntilClosed() {
    final AtomicReference<PropagatedBlockContext> result = new AtomicReference<>();
    final Subscription subscription = service.subscribeBlockPropagated(result::set);

    final Block block = gen.block(new BlockDataGenerator.BlockOptions());
    propagatedBlocks.propagate(block, Difficulty.of(42));
    assertThat(result.get()).isNotNull();
    assertThat(result.get().getBlockHeader()).isEqualTo(block.getHeader());
    assertThat(result.get().getTotalDifficulty()).isEqualTo(Difficulty.of(42).toUInt256());

    result.set(null);
    subscription.close();
    propagatedBlocks.propagate(block, Difficulty.of(43));
    assertThat(result.get()).isNull();
  }

  @Test
  void closingTwiceIsANoOpAndLeavesOtherSubscriptionsAlone() {
    final AtomicInteger first = new AtomicInteger();
    final AtomicInteger second = new AtomicInteger();
    final Subscription a = service.subscribeBlockAdded(ctx -> first.incrementAndGet());
    service.subscribeBlockAdded(ctx -> second.incrementAndGet());

    a.close();
    a.close();
    appendBlockOnGenesis();
    assertThat(first.get()).isZero();
    assertThat(second.get()).isEqualTo(1);
  }

  @Test
  void closingFromInsideTheCallbackStopsFurtherDelivery() {
    final AtomicInteger deliveries = new AtomicInteger();
    final AtomicReference<Subscription> self = new AtomicReference<>();
    self.set(
        service.subscribeBlockAdded(
            ctx -> {
              deliveries.incrementAndGet();
              self.get().close();
            }));

    final Block block = appendBlockOnGenesis();
    appendBlockOn(block);
    assertThat(deliveries.get()).isEqualTo(1);
  }

  private Block appendBlockOnGenesis() {
    return appendBlockOn(blockchain.getGenesisBlock());
  }

  private Block appendBlockOn(final Block parent) {
    final Block block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(parent.getHash())
                .setBlockNumber(parent.getHeader().getNumber() + 1));
    blockchain.appendBlock(block, gen.receipts(block));
    return block;
  }

  private Block appendReorg() {
    return appendReorgOn(blockchain.getGenesisBlock());
  }

  private Block appendReorgOn(final Block base) {
    final Block block = appendBlockOn(base);
    final Block forkBlock =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(base.getHash())
                .setDifficulty(block.getHeader().getDifficulty().subtract(1))
                .setBlockNumber(base.getHeader().getNumber() + 1));
    blockchain.appendBlock(forkBlock, gen.receipts(forkBlock));
    final Block reorgBlock =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(forkBlock.getHash())
                .setDifficulty(Difficulty.of(10_000_000))
                .setBlockNumber(forkBlock.getHeader().getNumber() + 1));
    blockchain.appendBlock(reorgBlock, gen.receipts(reorgBlock));
    return reorgBlock;
  }

  private static final class FakePropagatedBlockSource implements PropagatedBlockSource {
    private final Subscribers<PropagatedBlockListener> subscribers = Subscribers.create();

    @Override
    public long subscribe(final PropagatedBlockListener listener) {
      return subscribers.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(final long subscriptionId) {
      return subscribers.unsubscribe(subscriptionId);
    }

    void propagate(final Block block, final Difficulty totalDifficulty) {
      subscribers.forEach(listener -> listener.onBlockPropagated(block, totalDifficulty));
    }
  }
}
