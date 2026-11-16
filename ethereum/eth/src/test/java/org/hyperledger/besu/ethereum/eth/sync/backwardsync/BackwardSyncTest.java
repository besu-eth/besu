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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.ChainSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 120, unit = TimeUnit.SECONDS)
class BackwardSyncTest {

  private static final int MAX_ROUNDS = 10_000;
  private static final BlockAccessList EMPTY_LIST = new BlockAccessList(List.of());
  private static final BlockBody EMPTY_BODY =
      new BlockBody(List.of(), List.of(), Optional.of(List.of()));

  @Test
  void importsATargetThreeHundredBlocksAhead() {
    final Node node = new Node(false);
    final List<Block> blocks = chainOn(node.genesis, 310, 0, Optional.empty());
    node.source.append(blocks);
    node.local.append(blocks.subList(0, 10));
    node.start();

    node.sync.onNewHead(blocks.getLast().getHash(), Hash.ZERO);
    node.runUntil(() -> node.events.completed > 0);

    assertThat(node.local.chain.getChainHeadHash()).isEqualTo(blocks.getLast().getHash());
    assertThat(node.local.chain.getChainHeadBlockNumber()).isEqualTo(310);
    assertThat(node.events.failures).isEmpty();
    assertThat(node.events.syncing).isPositive();
    assertThat(node.pending.isEmpty()).isTrue();
    verify(node.validator, times(300))
        .validateAndProcessBlock(any(), any(), any(), any(), any(), any(), anyBoolean());
  }

  @Test
  void importsAForkOfTheTip() {
    final Node node = new Node(false);
    final List<Block> main = chainOn(node.genesis, 10, 0, Optional.empty());
    // three blocks on the eighth, the main chain has two above it
    final List<Block> fork = chainOn(main.get(7), 3, 1, Optional.empty());
    node.source.append(main);
    node.source.append(fork);
    // the peer serves its canonical chain
    node.source.chain.rewindToBlock(fork.getLast().getHash());
    node.local.append(main);
    node.start();

    node.sync.onNewHead(fork.getLast().getHash(), Hash.ZERO);
    node.runUntil(() -> node.events.completed > 0);

    assertThat(node.local.chain.getChainHeadHash()).isEqualTo(fork.getLast().getHash());
    assertThat(node.local.chain.getChainHeadBlockNumber()).isEqualTo(11);
    for (final Block block : fork) {
      assertThat(node.local.chain.getBlockHashByNumber(block.getHeader().getNumber()))
          .contains(block.getHash());
    }
    assertThat(node.events.failures).isEmpty();
  }

  @Test
  void anInvalidBlockFailsTheSyncAndReportsItsDescendants() {
    final Node node = new Node(false);
    final List<Block> blocks = chainOn(node.genesis, 20, 0, Optional.empty());
    node.source.append(blocks);
    final Block invalid = blocks.get(4);
    node.rejectAsInvalid(invalid);
    node.start();

    node.sync.onNewHead(blocks.getLast().getHash(), Hash.ZERO);
    node.runUntil(() -> !node.events.failures.isEmpty());

    assertThat(node.events.failures).hasSize(1);
    assertThat(node.events.badChains).hasSize(1);
    final BadChain bad = node.events.badChains.getFirst();
    assertThat(bad.header()).isEqualTo(invalid.getHeader());
    assertThat(
            Stream.concat(bad.blocks().stream().map(Block::getHeader), bad.headers().stream())
                .map(BlockHeader::getHash))
        .containsExactlyInAnyOrderElementsOf(
            blocks.subList(5, 20).stream().map(Block::getHash).toList());
    assertThat(node.local.chain.getChainHeadBlockNumber()).isEqualTo(4);
    assertThat(node.pending.header(5L)).isEmpty();
    assertThat(node.pending.header(6L)).isEmpty();
    assertThat(node.pending.linkedTop().orElse(-1)).isLessThanOrEqualTo(4);
  }

  @Test
  void anUnavailableWorldStateFailsTheSyncAndTheNextHeadRetries() {
    final Node node = new Node(false);
    final List<Block> blocks = chainOn(node.genesis, 6, 0, Optional.empty());
    node.source.append(blocks);
    node.worldStateUnavailableAt(blocks.get(2));
    node.start();

    node.sync.onNewHead(blocks.getLast().getHash(), Hash.ZERO);
    node.runUntil(() -> !node.events.failures.isEmpty());

    assertThat(node.events.failures.getFirst().getMessage()).contains("debug_resyncWorldState");
    assertThat(node.events.badChains).isEmpty();
    assertThat(node.local.chain.getChainHeadBlockNumber()).isEqualTo(2);

    node.importsEverything();
    node.sync.onNewHead(blocks.getLast().getHash(), Hash.ZERO);
    node.runUntil(() -> node.events.completed > 0);

    assertThat(node.local.chain.getChainHeadHash()).isEqualTo(blocks.getLast().getHash());
    assertThat(node.events.failures).hasSize(1);
  }

  @Test
  void aNewPayloadBlockIsNotRequestedFromPeers() {
    final Node node = new Node(false);
    final List<Block> blocks = chainOn(node.genesis, 5, 0, Optional.empty());
    node.source.append(blocks);
    node.start();

    node.sync.onNewPayload(blocks.getLast(), Optional.empty());
    node.runUntil(() -> node.events.completed > 0);

    assertThat(node.local.chain.getChainHeadHash()).isEqualTo(blocks.getLast().getHash());
    final List<Hash> requested = node.requestedBodies();
    assertThat(requested)
        .containsExactlyInAnyOrderElementsOf(
            blocks.subList(0, 4).stream().map(Block::getHash).toList());
  }

  @Test
  void sendsNothingBeforeTheNodeIsReady() {
    final Node node = new Node(false);
    final List<Block> blocks = chainOn(node.genesis, 5, 0, Optional.empty());
    node.source.append(blocks);
    node.sync.initialize();

    node.sync.onNewHead(blocks.getLast().getHash(), Hash.ZERO);
    node.settle();

    assertThat(node.events.syncing).isPositive();
    assertThat(node.sender.requests()).isEmpty();
    assertThat(node.events.completed).isZero();

    node.sync.onReadiness(true);
    node.runUntil(() -> node.events.completed > 0);

    assertThat(node.local.chain.getChainHeadHash()).isEqualTo(blocks.getLast().getHash());
  }

  @Test
  void importsFromTheBlockAccessListUpToTheFinalizedBlock() {
    final Node node = new Node(true);
    final List<Block> blocks = chainOn(node.genesis, 3, 0, Optional.of(EMPTY_LIST));
    node.source.append(blocks);
    node.cache(blocks);
    node.start();

    node.sync.onNewHead(blocks.get(2).getHash(), blocks.get(1).getHash());
    node.runUntil(() -> node.events.completed > 0);

    assertThat(node.local.chain.getChainHeadHash()).isEqualTo(blocks.get(2).getHash());
    for (final Block applied : blocks.subList(0, 2)) {
      verify(node.validator).validateAndApplyBlockAccessList(any(), eq(applied), any(), any());
      verify(node.validator, never())
          .validateAndProcessBlock(any(), eq(applied), any(), any(), any(), any(), anyBoolean());
    }
    final Block executed = blocks.get(2);
    verify(node.validator, never())
        .validateAndApplyBlockAccessList(any(), eq(executed), any(), any());
    verify(node.validator)
        .validateAndProcessBlock(any(), eq(executed), any(), any(), any(), any(), anyBoolean());
  }

  @Test
  void neverImportsFromTheBlockAccessListWhenItIsDisabled() {
    final Node node = new Node(false);
    final List<Block> blocks = chainOn(node.genesis, 3, 0, Optional.of(EMPTY_LIST));
    node.source.append(blocks);
    node.cache(blocks);
    node.start();

    node.sync.onNewHead(blocks.get(2).getHash(), blocks.get(1).getHash());
    node.runUntil(() -> node.events.completed > 0);

    assertThat(node.local.chain.getChainHeadHash()).isEqualTo(blocks.get(2).getHash());
    verify(node.validator, never()).validateAndApplyBlockAccessList(any(), any(), any(), any());
    verify(node.validator, times(3))
        .validateAndProcessBlock(any(), any(), any(), any(), any(), any(), anyBoolean());
  }

  // a block on the parent, a variant giving siblings different hashes
  private static List<Block> chainOn(
      final Block parent,
      final int count,
      final int variant,
      final Optional<BlockAccessList> blockAccessList) {
    final List<Block> blocks = new ArrayList<>();
    BlockHeader parentHeader = parent.getHeader();
    for (int i = 0; i < count; i++) {
      final long number = parentHeader.getNumber() + 1;
      final BlockHeaderTestFixture fixture =
          new BlockHeaderTestFixture()
              .number(number)
              .parentHash(parentHeader.getHash())
              .timestamp(number * 12)
              .gasLimit(30_000_000)
              .baseFeePerGas(Wei.of(7))
              .withdrawalsRoot(Hash.EMPTY_TRIE_HASH)
              .blobGasUsed(0L)
              .excessBlobGas(BlobGas.ZERO)
              .parentBeaconBlockRoot(Optional.of(Bytes32.ZERO))
              .requestsHash(Hash.EMPTY_REQUESTS_HASH)
              .extraData(Bytes.of(variant));
      blockAccessList.ifPresent(list -> fixture.balHash(BodyValidation.balHash(list)));
      final Block block = new Block(fixture.buildHeader(), EMPTY_BODY);
      blocks.add(block);
      parentHeader = block.getHeader();
    }
    return blocks;
  }

  private record BadChain(BlockHeader header, List<Block> blocks, List<BlockHeader> headers) {}

  private static final class Events implements BackwardSync.Listener {
    private int syncing;
    private int completed;
    private final List<BackwardSyncException> failures = new ArrayList<>();
    private final List<BadChain> badChains = new ArrayList<>();

    @Override
    public void syncing() {
      syncing++;
    }

    @Override
    public void completed() {
      completed++;
    }

    @Override
    public void failed(final BackwardSyncException e) {
      failures.add(e);
    }

    @Override
    public void badChain(
        final BlockHeader bad, final List<Block> blocks, final List<BlockHeader> headers) {
      badChains.add(new BadChain(bad, blocks, headers));
    }
  }

  private record Store(MutableBlockchain chain) {
    void append(final List<Block> blocks) {
      blocks.forEach(block -> chain.appendBlock(block, List.of()));
    }
  }

  // a node on its own chain, with a peer that serves the source chain
  private static final class Node {
    private final Block genesis =
        new Block(new BlockHeaderTestFixture().number(0).buildHeader(), EMPTY_BODY);
    private final Store source =
        new Store(InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesis));
    private final Store local =
        new Store(InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesis));
    private final ChainSender sender = new ChainSender(source.chain);
    private final BadBlockManager badBlocks = new BadBlockManager();
    private final BlockValidator validator = mock(BlockValidator.class);
    private final ManualScheduledExecutor loop = new ManualScheduledExecutor();
    private final ThreadPoolExecutor headerExecutor =
        new ThreadPoolExecutor(0, Integer.MAX_VALUE, 1, TimeUnit.SECONDS, new SynchronousQueue<>());
    private final PendingChain pending =
        new PendingChain(
            new InMemoryKeyValueStorage(),
            new InMemoryKeyValueStorage(),
            new InMemoryKeyValueStorage(),
            new MainnetBlockHeaderFunctions());
    private final TrustedPayloads payloads = new TrustedPayloads(1L << 20);
    private final Events events = new Events();
    private final BackwardSync sync;

    Node(final boolean balApplyEnabled) {
      final BackwardSyncConfiguration config =
          new BackwardSyncConfiguration(
              50,
              4,
              8L << 20,
              64,
              Duration.ofSeconds(10),
              3,
              3,
              Duration.ofSeconds(1),
              3,
              10,
              1L << 20,
              Duration.ofSeconds(30),
              balApplyEnabled);
      final ProtocolContext context = mock(ProtocolContext.class);
      when(context.getBlockchain()).thenReturn(local.chain);
      when(context.getBadBlockManager()).thenReturn(badBlocks);
      final ProtocolSchedule schedule = mock(ProtocolSchedule.class);
      final ProtocolSpec spec = mock(ProtocolSpec.class);
      when(schedule.getByBlockHeader(any())).thenReturn(spec);
      when(spec.getBlockValidator()).thenReturn(validator);
      importsEverything();

      final NoOpMetricsSystem metrics = new NoOpMetricsSystem();
      final BackwardSyncMetrics backwardSyncMetrics =
          new BackwardSyncMetrics(metrics, () -> 0, () -> 0, () -> 0, () -> 0);
      final HeaderFetcher headerFetcher =
          new HeaderFetcher(
              sender,
              ProtocolScheduleFixture.MAINNET,
              new ChainHeadSource() {
                @Override
                public Optional<Hash> headHash() {
                  return Optional.empty();
                }

                @Override
                public Optional<BlockHeader> knownHeader(final Hash hash) {
                  return payloads.block(hash).map(Block::getHeader);
                }
              },
              config.headersPerRequest());
      final AtomicReference<BackwardSync> engine = new AtomicReference<>();
      final BlockDataFetcher fetcher =
          new BlockDataFetcher(
              sender,
              mock(EthPeers.class),
              ProtocolScheduleFixture.MAINNET,
              pending,
              payloads,
              loop,
              loop::nanoTime,
              config,
              () -> engine.get().scheduleImport());
      final HeaderSync headerSync =
          new HeaderSync(
              sender,
              ProtocolScheduleFixture.MAINNET,
              pending,
              local.chain,
              badBlocks,
              headerFetcher,
              headerExecutor,
              config,
              metrics);
      sync =
          new BackwardSync(
              context,
              pending,
              payloads,
              headerSync,
              headerFetcher,
              fetcher,
              new ChainImporter(context, schedule, backwardSyncMetrics),
              loop,
              loop::nanoTime,
              config,
              backwardSyncMetrics,
              events);
      engine.set(sync);
    }

    void start() {
      sync.initialize();
      sync.onReadiness(true);
    }

    // newPayload gave the node these blocks and their lists
    void cache(final List<Block> blocks) {
      for (final Block block : blocks) {
        payloads.add(block, Optional.of(EMPTY_LIST));
      }
    }

    // the validator accepts every block, applying a list or executing it
    void importsEverything() {
      doAnswer(
              invocation -> {
                final Optional<BlockAccessList> list = invocation.getArgument(4);
                return success(List.of(), list);
              })
          .when(validator)
          .validateAndProcessBlock(any(), any(), any(), any(), any(), any(), anyBoolean());
      doAnswer(
              invocation -> {
                final BlockAccessList list = invocation.getArgument(2);
                final List<TransactionReceipt> receipts = invocation.getArgument(3);
                return success(receipts, Optional.of(list));
              })
          .when(validator)
          .validateAndApplyBlockAccessList(any(), any(), any(), any());
    }

    // the validator records the block as bad, as it does for a block that breaks the rules
    void rejectAsInvalid(final Block block) {
      importsEverything();
      doAnswer(
              invocation -> {
                badBlocks.addBadBlock(block, BadBlockCause.fromValidationFailure("invalid"));
                return new BlockProcessingResult("invalid");
              })
          .when(validator)
          .validateAndProcessBlock(any(), eq(block), any(), any(), any(), any(), anyBoolean());
    }

    void worldStateUnavailableAt(final Block block) {
      importsEverything();
      doReturn(BlockProcessingResult.worldStateUnavailable("no state"))
          .when(validator)
          .validateAndProcessBlock(any(), eq(block), any(), any(), any(), any(), anyBoolean());
    }

    List<Hash> requestedBodies() {
      return sender.requests().stream()
          .filter(message -> message.getCode() == EthProtocolMessages.GET_BLOCK_BODIES)
          .flatMap(
              message ->
                  StreamSupport.stream(
                      GetBlockBodiesMessage.readFrom(message).hashes().spliterator(), false))
          .toList();
    }

    void runUntil(final BooleanSupplier done) {
      for (int round = 0; !done.getAsBoolean(); round++) {
        assertThat(round).as("rounds").isLessThan(MAX_ROUNDS);
        step();
      }
    }

    void settle() {
      for (int round = 0; round < 100; round++) {
        step();
      }
    }

    private void step() {
      loop.runPending();
      // the header download runs on its own threads and reports back to the loop
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (headerExecutor.getActiveCount() > 0 && System.nanoTime() < deadline) {
        Thread.onSpinWait();
      }
      assertThat(headerExecutor.getActiveCount()).as("running header tasks").isZero();
      loop.runPending();
    }

    private static BlockProcessingResult success(
        final List<TransactionReceipt> receipts, final Optional<BlockAccessList> list) {
      return new BlockProcessingResult(
          Optional.of(
              new BlockProcessingOutputs(
                  mock(MutableWorldState.class), receipts, Optional.empty(), list)));
    }
  }
}
