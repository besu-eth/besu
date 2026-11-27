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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockWithAccessList;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.ChainSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DownloadBlockDataStepTest {

  private static final int REQUEST_SIZE = 2;

  private final BlockDataGenerator generator = new BlockDataGenerator(1);
  private final DeterministicEthScheduler scheduler = new DeterministicEthScheduler();
  private final List<Block> blocks = new ArrayList<>();
  private final Map<Hash, BlockAccessList> bals = new HashMap<>();
  private MutableBlockchain chain;
  private ChainSender chainSender;
  private TestSender sender;

  @BeforeEach
  void setUp() {
    chain = InMemoryKeyValueStorageProvider.createInMemoryBlockchain(generator.genesisBlock());
    for (int number = 1; number <= 3; number++) {
      appendBlock(number, true);
    }
    chainSender = new ChainSender(chain);
    sender = new TestSender(chainSender);
  }

  @Test
  void downloadsTheBlocksAndTheirBals() {
    final BlockBatch batch = step().apply(headers()).join();

    assertThat(batch.blocks())
        .extracting(Block::getHash)
        .containsExactly(hash(0), hash(1), hash(2));
    assertThat(batch.blocks().get(2).getBody()).isEqualTo(blocks.get(2).getBody());
    assertThat(batch.blockAccessLists()).isEqualTo(bals);
    // in requests of REQUEST_SIZE blocks, which the scheduler spreads over the peers
    assertThat(requests(EthProtocolMessages.GET_BLOCK_BODIES)).isEqualTo(2);
    assertThat(requests(EthProtocolMessages.GET_BLOCK_ACCESS_LISTS)).isEqualTo(2);
  }

  @Test
  void asksAgainForWhatAResponseLeftOut() {
    sender.firstItemOnly = true;

    final BlockBatch batch = step().apply(headers()).join();

    assertThat(batch.blocks())
        .extracting(Block::getHash)
        .containsExactly(hash(0), hash(1), hash(2));
    assertThat(batch.blockAccessLists()).isEqualTo(bals);
    assertThat(requests(EthProtocolMessages.GET_BLOCK_BODIES)).isEqualTo(3);
    assertThat(requests(EthProtocolMessages.GET_BLOCK_ACCESS_LISTS)).isEqualTo(3);
  }

  @Test
  void asksTheOtherPeersForABalAPeerLacked() {
    appendBlock(4, false);

    final BlockBatch batch = step().apply(headers()).join();

    assertThat(batch.blockAccessLists()).isEqualTo(bals).doesNotContainKey(hash(3));
    final Set<EthPeer> lacking = Set.of(sender.peer);
    // blocks 1 and 2, blocks 3 and 4, then block 4 three more times without the peer
    assertThat(sender.excludedByBalRequest)
        .containsExactly(Set.of(), Set.of(), lacking, lacking, lacking);
  }

  @Test
  void asksForTheFirstBatchOfTheImportAheadOfTheOthers() {
    step(1).apply(headers()).join();
    step(0).apply(headers()).join();

    // four requests a batch: two for the bodies, two for the block access lists
    assertThat(sender.priorities)
        .containsExactly(
            Priority.HEADERS,
            Priority.HEADERS,
            Priority.HEADERS,
            Priority.HEADERS,
            Priority.BODIES,
            Priority.BODIES,
            Priority.BODIES,
            Priority.BODIES);
  }

  @Test
  void keepsTheBlocksWhenTheBalsFail() {
    chainSender.fail(message -> message.getCode() == EthProtocolMessages.GET_BLOCK_ACCESS_LISTS);

    final BlockBatch batch = step().apply(headers()).join();

    assertThat(batch.blocks()).hasSize(3);
    assertThat(batch.blockAccessLists()).isEmpty();
  }

  @Test
  void stopsWaitingForTheBalsAfterTheTimeoutAndKeepsWhatCame() {
    sender.unansweredBalRequests = 1;

    final CompletableFuture<BlockBatch> batch = step().apply(headers());
    assertThat(batch).isNotDone();
    scheduler.expirePendingTimeouts();

    assertThat(batch.join().blockAccessLists()).containsOnlyKeys(hash(2));
    assertThat(batch.join().blocks()).hasSize(3);
    assertThat(sender.unanswered).singleElement().matches(CompletableFuture::isCancelled);
  }

  private DownloadBlockDataStep step() {
    return step(1);
  }

  private DownloadBlockDataStep step(final long firstBlock) {
    return new DownloadBlockDataStep(
        sender,
        scheduler,
        ProtocolScheduleFixture.MAINNET,
        REQUEST_SIZE,
        Duration.ofSeconds(60),
        firstBlock);
  }

  // a block stored with its block access list or without
  private void appendBlock(final long number, final boolean withBal) {
    final BlockWithAccessList generated =
        generator.blockWithAccessList(
            new BlockOptions()
                .setBlockNumber(number)
                .setParentHash(chain.getChainHeadHash())
                .withGeneratedBlockAccessList());
    final Block block = generated.getBlock();
    final Optional<BlockAccessList> bal =
        withBal ? generated.getBlockAccessList() : Optional.empty();
    chain.appendBlock(block, generator.receipts(block), bal);
    blocks.add(block);
    bal.ifPresent(list -> bals.put(block.getHash(), list));
  }

  private List<BlockHeader> headers() {
    return blocks.stream().map(Block::getHeader).toList();
  }

  private Hash hash(final int index) {
    return blocks.get(index).getHash();
  }

  private long requests(final int code) {
    return chainSender.requests().stream().filter(message -> message.getCode() == code).count();
  }

  /** Answers from the chain, and can shorten the answers or hold some back. */
  private static final class TestSender implements RequestSender {

    private final ChainSender chain;
    private final List<Set<EthPeer>> excludedByBalRequest = new ArrayList<>();
    private final List<CompletableFuture<?>> unanswered = new ArrayList<>();
    private final List<Priority> priorities = new ArrayList<>();
    private boolean firstItemOnly;
    private int unansweredBalRequests;
    private EthPeer peer;

    TestSender(final ChainSender chain) {
      this.chain = chain;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<Response<T>> submit(
        final SyncRequest<T> request, final Set<EthPeer> excluded) {
      priorities.add(request.priority());
      if (request.name().equals("block_access_lists")) {
        excludedByBalRequest.add(Set.copyOf(excluded));
        if (unansweredBalRequests > 0) {
          unansweredBalRequests--;
          final CompletableFuture<Response<T>> never = new CompletableFuture<>();
          unanswered.add(never);
          return never;
        }
      }
      return chain
          .submit(request, excluded)
          .thenApply(
              response -> {
                peer = response.peer();
                return firstItemOnly
                        && response.value() instanceof List<?> items
                        && items.size() > 1
                    ? new Response<>((T) List.of(items.getFirst()), response.peer())
                    : response;
              });
    }

    @Override
    public <T> CompletableFuture<Response<T>> submitWithRetries(
        final SyncRequest<T> request, final int attempts) {
      return submit(request, Set.of());
    }

    @Override
    public int peerCount(final Protocol protocol) {
      return 1;
    }

    @Override
    public int capacity(final Protocol protocol) {
      return 1;
    }

    @Override
    public Executor workers() {
      return Runnable::run;
    }
  }
}
