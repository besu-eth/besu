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
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.ChainSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
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
  private final Map<Hash, List<TransactionReceipt>> receipts = new HashMap<>();
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
  void downloadsTheBlocksTheirBalsAndTheReceiptsOfTheFinalizedOnes() {
    final BlockBatch batch = step(2).apply(headers()).join();

    assertThat(batch.blocks())
        .extracting(Block::getHash)
        .containsExactly(hash(0), hash(1), hash(2));
    assertThat(batch.blocks().get(2).getBody()).isEqualTo(blocks.get(2).getBody());
    assertThat(batch.blockAccessLists()).isEqualTo(bals);
    assertThat(batch.receipts())
        .containsOnlyKeys(hash(0), hash(1))
        .containsEntry(hash(1), receipts.get(hash(1)));
    // in requests of REQUEST_SIZE blocks, which the scheduler spreads over the peers
    assertThat(requests(EthProtocolMessages.GET_BLOCK_BODIES)).isEqualTo(2);
    assertThat(requests(EthProtocolMessages.GET_BLOCK_ACCESS_LISTS)).isEqualTo(2);
    assertThat(requests(EthProtocolMessages.GET_RECEIPTS)).isEqualTo(1);
  }

  @Test
  void asksAgainForWhatAResponseLeftOut() {
    sender.firstItemOnly = true;

    final BlockBatch batch = step(3).apply(headers()).join();

    assertThat(batch.blocks())
        .extracting(Block::getHash)
        .containsExactly(hash(0), hash(1), hash(2));
    assertThat(batch.blockAccessLists()).isEqualTo(bals);
    assertThat(batch.receipts()).containsOnlyKeys(hash(0), hash(1), hash(2));
    assertThat(requests(EthProtocolMessages.GET_BLOCK_BODIES)).isEqualTo(3);
    assertThat(requests(EthProtocolMessages.GET_BLOCK_ACCESS_LISTS)).isEqualTo(3);
  }

  @Test
  void asksTheOtherPeersForABalAPeerLacked() {
    appendBlock(4, false);

    final BlockBatch batch = step(-1).apply(headers()).join();

    assertThat(batch.blockAccessLists()).isEqualTo(bals).doesNotContainKey(hash(3));
    final Set<EthPeer> lacking = Set.of(sender.peer);
    // blocks 1 and 2, blocks 3 and 4, then block 4 three more times without the peer
    assertThat(sender.excludedByBalRequest)
        .containsExactly(Set.of(), Set.of(), lacking, lacking, lacking);
  }

  @Test
  void asksForTheFirstBatchOfTheImportAheadOfTheOthers() {
    step(2, 1).apply(headers()).join();
    step(2, 0).apply(headers()).join();

    // five requests a batch: two for the bodies, two for the block access lists, one for receipts
    assertThat(sender.priorities)
        .containsExactly(
            Priority.HEADERS,
            Priority.HEADERS,
            Priority.HEADERS,
            Priority.HEADERS,
            Priority.HEADERS,
            Priority.BODIES,
            Priority.BODIES,
            Priority.BODIES,
            Priority.BODIES,
            Priority.BODIES);
  }

  @Test
  void keepsTheBlocksWhenTheBalsAndReceiptsFail() {
    chainSender.fail(
        message ->
            message.getCode() == EthProtocolMessages.GET_BLOCK_ACCESS_LISTS
                || message.getCode() == EthProtocolMessages.GET_RECEIPTS);

    final BlockBatch batch = step(3).apply(headers()).join();

    assertThat(batch.blocks()).hasSize(3);
    assertThat(batch.blockAccessLists()).isEmpty();
    assertThat(batch.receipts()).isEmpty();
  }

  @Test
  void stopsWaitingForTheBalsAfterTheTimeoutAndKeepsWhatCame() {
    sender.unansweredBalRequests = 1;

    final CompletableFuture<BlockBatch> batch = step(-1).apply(headers());
    assertThat(batch).isNotDone();
    scheduler.expirePendingTimeouts();

    assertThat(batch.join().blockAccessLists()).containsOnlyKeys(hash(2));
    assertThat(batch.join().blocks()).hasSize(3);
    assertThat(sender.unanswered).singleElement().matches(CompletableFuture::isCancelled);
  }

  private DownloadBlockDataStep step(final long finalizedNumber) {
    return step(finalizedNumber, 1);
  }

  private DownloadBlockDataStep step(final long finalizedNumber, final long firstBlock) {
    return new DownloadBlockDataStep(
        sender,
        scheduler,
        ProtocolScheduleFixture.MAINNET,
        new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder()),
        REQUEST_SIZE,
        Duration.ofSeconds(60),
        finalizedNumber,
        firstBlock);
  }

  // a block whose receipts match its header, stored with its block access list or without
  private void appendBlock(final long number, final boolean withBal) {
    final BlockWithAccessList generated =
        generator.blockWithAccessList(
            new BlockOptions()
                .setBlockNumber(number)
                .setParentHash(chain.getChainHeadHash())
                .withGeneratedBlockAccessList());
    final List<TransactionReceipt> blockReceipts = generator.receipts(generated.getBlock());
    final BlockHeader header =
        BlockHeaderBuilder.fromHeader(generated.getBlock().getHeader())
            .receiptsRoot(BodyValidation.receiptsRoot(blockReceipts))
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildBlockHeader();
    final Block block = new Block(header, generated.getBlock().getBody());
    final Optional<BlockAccessList> bal =
        withBal ? generated.getBlockAccessList() : Optional.empty();
    chain.appendBlock(block, blockReceipts, bal);
    blocks.add(block);
    bal.ifPresent(list -> bals.put(block.getHash(), list));
    receipts.put(block.getHash(), blockReceipts);
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
