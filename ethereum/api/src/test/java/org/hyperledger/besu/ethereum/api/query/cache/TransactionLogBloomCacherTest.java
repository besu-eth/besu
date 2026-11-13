/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.query.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher.BLOCKS_PER_BLOOM_CACHE;
import static org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher.BLOOM_BITS_LENGTH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("unused")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TransactionLogBloomCacherTest {

  @TempDir private Path cacheDir;

  private Hash testHash;
  private static LogsBloomFilter testLogsBloomFilter;

  @Mock MutableBlockchain blockchain;
  @Mock EthScheduler scheduler;
  private TransactionLogBloomCacher transactionLogBloomCacher;

  @BeforeAll
  public static void setupClass() {
    final Address testAddress = Address.fromHexString("0x123456");
    final Bytes testMessage = Bytes.fromHexString("0x9876");
    final Log testLog = new Log(testAddress, testMessage, List.of());
    testLogsBloomFilter = LogsBloomFilter.builder().insertLog(testLog).build();
  }

  private static void writeThreeEntries(final LogsBloomFilter filter, final RandomAccessFile file)
      throws IOException {
    file.write(filter.getBytes().toArray());
    file.write(filter.getBytes().toArray());
    file.write(filter.getBytes().toArray());
  }

  @SuppressWarnings({"unchecked", "ReturnValueIgnored"})
  @BeforeEach
  public void setup() throws IOException {
    final BlockHeader fakeHeader =
        new BlockHeader(
            Hash.EMPTY,
            Hash.EMPTY,
            Address.ZERO,
            Hash.EMPTY,
            Hash.EMPTY,
            Hash.EMPTY,
            testLogsBloomFilter,
            Difficulty.ZERO,
            0L,
            0L,
            0L,
            0L,
            Bytes.EMPTY,
            null,
            Bytes32.wrap(Hash.EMPTY.getBytes()),
            0L,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new MainnetBlockHeaderFunctions());
    testHash = fakeHeader.getHash();
    when(blockchain.getBlockHeader(anyLong())).thenReturn(Optional.of(fakeHeader));
    when(scheduler.scheduleFutureTask(any(Supplier.class), any(Duration.class)))
        .thenAnswer(
            invocation -> {
              invocation.getArgument(0, Supplier.class).get();
              return null;
            });
    when(scheduler.scheduleComputationTask(any(Supplier.class)))
        .thenAnswer(
            invocation -> {
              invocation.getArgument(0, Supplier.class).get();
              return null;
            });
    transactionLogBloomCacher = new TransactionLogBloomCacher(blockchain, cacheDir, scheduler);
  }

  /**
   * The segment loop used to run from the start block all the way to the caller's stop block,
   * opening a FileOutputStream per BLOCKS_PER_BLOOM_CACHE segment and leaving an empty cache file
   * behind for every segment past the head. With a stop block of Long.MAX_VALUE -- which
   * admin_generateLogBloomCache used to produce for `latest` -- that is roughly 9.2e13 segments,
   * enough to exhaust the inodes on the cache filesystem. The stop block is now clamped to the
   * chain head inside generateLogBloomCache, so no caller can trigger it.
   *
   * <p>The stop block here is deliberately a few segments past the head rather than Long.MAX_VALUE:
   * it exercises exactly the same clamp, but a regression fails on the file count in milliseconds
   * instead of filling the test machine's temp filesystem first. (Long.MAX_VALUE really does fill
   * it -- that is how this was confirmed, and it cost a million inodes.) The bound handed to the
   * cacher is covered separately by AdminGenerateLogBloomCacheTest.
   *
   * <p>The clamp is to head + 1, not to the head, because the segment loop treats the stop block as
   * an exclusive bound. When the head sits exactly on a BLOCKS_PER_BLOOM_CACHE boundary, the
   * segment that starts at the head holds the head block itself; clamping to the head would drop it
   * and no file would be written at all.
   */
  @Test
  public void shouldStillCacheTheSegmentStartingAtAChainHeadOnASegmentBoundary() {
    // a segment start whose hex form has an even number of digits, which createBlock requires
    final long chainHead = 11 * BLOCKS_PER_BLOOM_CACHE;
    when(blockchain.getChainHeadBlockNumber()).thenReturn(chainHead);

    final BlockHeader headHeader = createBlock(chainHead);
    when(blockchain.getBlockHeader(anyLong()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(0, Long.class) == chainHead
                    ? Optional.of(headHeader)
                    : Optional.empty());

    transactionLogBloomCacher.generateLogBloomCache(chainHead, Long.MAX_VALUE);

    assertThat(cacheDir.toFile().list()).containsExactly("logBloom-11.cache");
    assertThat(cacheDir.resolve("logBloom-11.cache").toFile().length())
        .isEqualTo(BLOOM_BITS_LENGTH);
  }

  @Test
  public void shouldClampStopBlockToChainHead() {
    final long chainHead = 5L;
    when(blockchain.getChainHeadBlockNumber()).thenReturn(chainHead);

    // Build the headers first. createBlock() stubs the mock itself, so calling it from inside an
    // Answer on that same mock recurses until the stack overflows.
    final Map<Long, BlockHeader> headers = new HashMap<>();
    for (long i = 0; i <= chainHead; i++) {
      headers.put(i, createBlock(i));
    }
    when(blockchain.getBlockHeader(anyLong()))
        .thenAnswer(
            invocation -> Optional.ofNullable(headers.get(invocation.getArgument(0, Long.class))));

    assertThat(cacheDir.toFile().list()).isEmpty();

    transactionLogBloomCacher.generateLogBloomCache(0, chainHead + 5L * BLOCKS_PER_BLOOM_CACHE);

    // one segment file covering blocks 0..chainHead, not one per segment past the head
    assertThat(cacheDir.toFile().list()).hasSize(1);
    assertThat(cacheDir.resolve("logBloom-0.cache").toFile().length())
        .isEqualTo((long) BLOOM_BITS_LENGTH * (chainHead + 1));
  }

  @Test
  public void shouldSplitLogsIntoSeveralFiles() {

    when(blockchain.getChainHeadBlockNumber()).thenReturn(200003L);
    assertThat(cacheDir.toFile().list().length).isEqualTo(0);
    transactionLogBloomCacher.cacheAll();
    assertThat(cacheDir.toFile().list().length).isEqualTo(2);
  }

  @Test
  public void shouldNotRemoveSegmentsWhileCachingIsInProgress() throws IOException {
    final File logBloom = Files.createFile(cacheDir.resolve("logBloom-0.cache")).toFile();
    createLogBloomCache(logBloom);

    transactionLogBloomCacher.getCachingStatus().cachingCount.incrementAndGet();
    try {
      assertThat(transactionLogBloomCacher.removeSegments(0L, (long) BLOCKS_PER_BLOOM_CACHE))
          .isFalse();
      assertThat(logBloom).exists();
    } finally {
      transactionLogBloomCacher.getCachingStatus().cachingCount.decrementAndGet();
    }

    assertThat(transactionLogBloomCacher.removeSegments(0L, (long) BLOCKS_PER_BLOOM_CACHE))
        .isTrue();
    assertThat(logBloom).doesNotExist();
  }

  @Test
  public void shouldReportPartialCacheRemovalFailure() throws IOException {
    final Path blockedSegment = Files.createDirectory(cacheDir.resolve("logBloom-0.cache"));
    Files.createFile(blockedSegment.resolve("child"));
    final Path removableSegment = Files.createFile(cacheDir.resolve("logBloom-1.cache"));

    assertThatThrownBy(
            () -> transactionLogBloomCacher.removeSegments(0L, (long) BLOCKS_PER_BLOOM_CACHE))
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseInstanceOf(IOException.class);

    assertThat(blockedSegment).exists();
    assertThat(removableSegment).doesNotExist();
  }

  @Test
  public void shouldUpdateCacheWhenBlockAdded() throws IOException {
    final File logBloom = Files.createFile(cacheDir.resolve("logBloom-0.cache")).toFile();

    createLogBloomCache(logBloom);

    final BlockHeader header = createBlock(3L);

    assertThat(logBloom.length()).isEqualTo(BLOOM_BITS_LENGTH * 3);

    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        header, Optional.empty(), Optional.of(logBloom));

    assertThat(logBloom.length()).isEqualTo(BLOOM_BITS_LENGTH * 4);
    assertThat(cacheDir.toFile().list().length).isEqualTo(1);
  }

  @Test
  public void shouldReloadCacheWhenBLockIsMissing() throws IOException {
    final File logBloom = Files.createFile(cacheDir.resolve("logBloom-0.cache")).toFile();

    createLogBloomCache(logBloom);
    assertThat(logBloom.length()).isEqualTo(BLOOM_BITS_LENGTH * 3);

    final List<BlockHeader> blockHeaders = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      blockHeaders.add(createBlock(i));
    }

    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        blockHeaders.get(4), Optional.empty(), Optional.of(logBloom));

    for (int i = 0; i < 5; i++) {
      assertThat(blockHeaders.get(i).getLogsBloom().getBytes().toArray())
          .containsExactly(readLogBloomCache(logBloom, i));
    }

    assertThat(logBloom.length()).isEqualTo(BLOOM_BITS_LENGTH * 5);
    assertThat(cacheDir.toFile().list().length).isEqualTo(1);
  }

  @Test
  public void shouldReloadCacheWhenFileIsInvalid() throws IOException {
    final File logBloom = Files.createFile(cacheDir.resolve("logBloom-0.cache")).toFile();

    final File logBloom1 = Files.createFile(cacheDir.resolve("logBloom-1.cache")).toFile();

    when(blockchain.getChainHeadBlockNumber()).thenReturn(100003L);
    assertThat(logBloom.length()).isEqualTo(0);
    assertThat(logBloom1.length()).isEqualTo(0);

    assertThat(cacheDir.toFile().list().length).isEqualTo(2);

    transactionLogBloomCacher.cacheAll();

    assertThat(logBloom.length()).isEqualTo(BLOOM_BITS_LENGTH * BLOCKS_PER_BLOOM_CACHE);
    assertThat(logBloom1.length()).isEqualTo(0);

    assertThat(cacheDir.toFile().list().length).isEqualTo(2);
  }

  @Test
  public void shouldUpdateCacheWhenChainReorgFired() throws IOException {
    final File logBloom = Files.createFile(cacheDir.resolve("logBloom-0.cache")).toFile();

    final List<BlockHeader> firstBranch = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      firstBranch.add(createBlock(i));
    }

    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        firstBranch.get(4), Optional.empty(), Optional.of(logBloom));
    assertThat(logBloom.length()).isEqualTo(BLOOM_BITS_LENGTH * 5);
    for (int i = 0; i < 5; i++) {
      assertThat(firstBranch.get(i).getLogsBloom().getBytes().toArray())
          .containsExactly(readLogBloomCache(logBloom, i));
    }

    final List<BlockHeader> forkBranch = new ArrayList<>();
    forkBranch.add(firstBranch.get(0));
    forkBranch.add(firstBranch.get(1));
    for (int i = 2; i < 5; i++) {
      forkBranch.add(createBlock(i, Optional.of("111111111111111111111111")));
    }

    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        forkBranch.get(4), Optional.of(firstBranch.get(1)), Optional.of(logBloom));
    assertThat(logBloom.length()).isEqualTo(BLOOM_BITS_LENGTH * 5);
    for (int i = 0; i < 5; i++) {
      assertThat(forkBranch.get(i).getLogsBloom().getBytes().toArray())
          .containsExactly(readLogBloomCache(logBloom, i));
    }

    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        forkBranch.get(1), Optional.empty(), Optional.of(logBloom));
    assertThat(logBloom.length()).isEqualTo(BLOOM_BITS_LENGTH * 2);

    assertThat(cacheDir.toFile().list().length).isEqualTo(1);
  }

  @Test
  public void shouldStopCachingPreviousSegmentsWhenTheSchedulerStops() throws Exception {
    final long chainHead = 2L * BLOCKS_PER_BLOOM_CACHE + 5;
    assertNoStorageReadsAfterTheSchedulerStops(
        chainHead, cacher -> cacher.ensurePreviousSegmentsArePresent(chainHead, false));

    // the cacher works down from the segment below the head and must not start the next one
    assertThat(cacheDir.resolve("logBloom-0.cache")).doesNotExist();
  }

  @Test
  public void shouldStopRequestedCachingWhenTheSchedulerStops() throws Exception {
    final long chainHead = 2L * BLOCKS_PER_BLOOM_CACHE + 5;
    assertNoStorageReadsAfterTheSchedulerStops(
        chainHead, cacher -> cacher.requestCaching(0, chainHead));

    assertThat(cacheDir.resolve("logBloom-1.cache")).doesNotExist();
  }

  /**
   * Stops the scheduler in the order Runner.stopServices uses: EthScheduler.stop() and awaitStop()
   * run before the storage is closed, so a caching task still reading after that point hits a
   * closed storage once per remaining segment.
   */
  private void assertNoStorageReadsAfterTheSchedulerStops(
      final long chainHead, final Consumer<TransactionLogBloomCacher> startCaching)
      throws InterruptedException {
    final BlockHeader header = createBlock(0);
    final CountDownLatch readInProgress = new CountDownLatch(1);
    final CountDownLatch schedulerStopped = new CountDownLatch(1);
    final AtomicLong readsAfterStop = new AtomicLong();
    when(blockchain.getChainHeadBlockNumber()).thenReturn(chainHead);
    when(blockchain.getBlockHeader(anyLong()))
        .thenAnswer(
            invocation -> {
              if (schedulerStopped.getCount() == 0) {
                readsAfterStop.incrementAndGet();
              }
              if (readInProgress.getCount() > 0) {
                readInProgress.countDown();
                // a RocksDB read does not react to the interrupt from EthScheduler.stop()
                Uninterruptibles.awaitUninterruptibly(schedulerStopped);
              }
              return Optional.of(header);
            });

    final EthScheduler ethScheduler = new EthScheduler(1, 1, 1, 1, new NoOpMetricsSystem());
    startCaching.accept(new TransactionLogBloomCacher(blockchain, cacheDir, ethScheduler));
    assertThat(readInProgress.await(10, TimeUnit.SECONDS)).isTrue();

    ethScheduler.stop();
    schedulerStopped.countDown();
    ethScheduler.awaitStop();

    assertThat(readsAfterStop).hasValue(0);
  }

  @Test
  public void shouldRewriteABlockReplacedByAReorgDroppedWhileCachingIsInProgress()
      throws IOException {
    final File logBloom = cacheDir.resolve("logBloom-0.cache").toFile();
    final List<BlockHeader> firstBranch = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      firstBranch.add(createBlock(i));
    }
    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        firstBranch.get(5), Optional.empty(), Optional.of(logBloom));
    assertThat(readLogBloomCache(logBloom, 5))
        .containsExactly(firstBranch.get(5).getLogsBloom().getBytes().toArray());

    // a reorg replaces block 5 while a cache generation holds the cacher
    final BlockHeader forkBlock5 = createBlock(5, Optional.of("111111111111111111111111"));
    transactionLogBloomCacher.getCachingStatus().cachingCount.incrementAndGet();
    try {
      transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
          forkBlock5, Optional.of(firstBranch.get(4)), Optional.of(logBloom));
    } finally {
      transactionLogBloomCacher.getCachingStatus().cachingCount.decrementAndGet();
    }

    // the next block builds on the fork and is the first event the cacher processes again
    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        createBlock(6), Optional.of(forkBlock5), Optional.of(logBloom));

    assertThat(readLogBloomCache(logBloom, 5))
        .containsExactly(forkBlock5.getLogsBloom().getBytes().toArray());
    assertThat(logBloom.length()).isEqualTo(BLOOM_BITS_LENGTH * 7);
  }

  @Test
  public void shouldRebuildTheSegmentOfTheLastBlockOfASegmentAfterADroppedReorg()
      throws IOException {
    // createBlock needs block numbers with an even number of hex digits
    final long lastBlock = 11L * BLOCKS_PER_BLOOM_CACHE - 1;
    final File logBloom = cacheDir.resolve("logBloom-10.cache").toFile();
    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        createBlock(lastBlock - 1), Optional.empty(), Optional.empty());

    // a reorg replaces the block before the last one while a cache generation holds the cacher
    final BlockHeader forkBlock =
        createBlock(lastBlock - 1, Optional.of("111111111111111111111111"));
    transactionLogBloomCacher.getCachingStatus().cachingCount.incrementAndGet();
    try {
      transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
          forkBlock, Optional.of(createBlock(lastBlock - 2)), Optional.empty());
    } finally {
      transactionLogBloomCacher.getCachingStatus().cachingCount.decrementAndGet();
    }

    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        createBlock(lastBlock), Optional.of(forkBlock), Optional.empty());

    assertThat(readLogBloomCache(logBloom, (lastBlock - 1) % BLOCKS_PER_BLOOM_CACHE))
        .containsExactly(forkBlock.getLogsBloom().getBytes().toArray());
    assertThat(logBloom.length()).isEqualTo((long) BLOCKS_PER_BLOOM_CACHE * BLOOM_BITS_LENGTH);
    assertThat(cacheDir.resolve("logBloom-11.cache")).doesNotExist();
  }

  @Test
  public void shouldRewriteTheLastBlockOfAFullSegmentReplacedByAReorgIntoTheNextSegment()
      throws IOException {
    // createBlock needs block numbers with an even number of hex digits
    final long lastBlock = 11L * BLOCKS_PER_BLOOM_CACHE - 1;
    final File logBloom = cacheDir.resolve("logBloom-10.cache").toFile();
    fillSegmentUpTo(lastBlock);

    // one reorg event replaces the last block and adds a block of the next segment
    final BlockHeader forkLastBlock =
        createBlock(lastBlock, Optional.of("111111111111111111111111"));
    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        createBlock(lastBlock + 1), Optional.of(createBlock(lastBlock - 1)), Optional.empty());

    assertThat(readLogBloomCache(logBloom, lastBlock % BLOCKS_PER_BLOOM_CACHE))
        .containsExactly(forkLastBlock.getLogsBloom().getBytes().toArray());
    assertThat(logBloom.length()).isEqualTo((long) BLOCKS_PER_BLOOM_CACHE * BLOOM_BITS_LENGTH);
  }

  @Test
  public void shouldRewriteTheLastBlockOfAFullSegmentReplacedByADroppedReorg() throws IOException {
    // createBlock needs block numbers with an even number of hex digits
    final long lastBlock = 11L * BLOCKS_PER_BLOOM_CACHE - 1;
    final File logBloom = cacheDir.resolve("logBloom-10.cache").toFile();
    fillSegmentUpTo(lastBlock);

    // a reorg replaces the last block and the next block arrives, both while a cache generation
    // holds the cacher
    final BlockHeader forkLastBlock =
        createBlock(lastBlock, Optional.of("111111111111111111111111"));
    final BlockHeader firstBlockOfNextSegment = createBlock(lastBlock + 1);
    transactionLogBloomCacher.getCachingStatus().cachingCount.incrementAndGet();
    try {
      transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
          forkLastBlock, Optional.of(createBlock(lastBlock - 1)), Optional.empty());
      transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
          firstBlockOfNextSegment, Optional.of(forkLastBlock), Optional.empty());
    } finally {
      transactionLogBloomCacher.getCachingStatus().cachingCount.decrementAndGet();
    }

    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        createBlock(lastBlock + 2), Optional.of(firstBlockOfNextSegment), Optional.empty());

    assertThat(readLogBloomCache(logBloom, lastBlock % BLOCKS_PER_BLOOM_CACHE))
        .containsExactly(forkLastBlock.getLogsBloom().getBytes().toArray());
    assertThat(logBloom.length()).isEqualTo((long) BLOCKS_PER_BLOOM_CACHE * BLOOM_BITS_LENGTH);
  }

  private void fillSegmentUpTo(final long lastBlock) {
    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        createBlock(lastBlock - 1), Optional.empty(), Optional.empty());
    transactionLogBloomCacher.cacheLogsBloomForBlockHeader(
        createBlock(lastBlock), Optional.of(createBlock(lastBlock - 1)), Optional.empty());
  }

  private void createLogBloomCache(final File logBloom) throws IOException {
    try (final RandomAccessFile randomAccessFile = new RandomAccessFile(logBloom, "rws")) {
      writeThreeEntries(testLogsBloomFilter, randomAccessFile);
    }
  }

  private byte[] readLogBloomCache(final File logBloom, final long number) throws IOException {
    try (final RandomAccessFile randomAccessFile = new RandomAccessFile(logBloom, "r")) {
      randomAccessFile.seek(BLOOM_BITS_LENGTH * number);
      final byte[] retrievedLog = new byte[BLOOM_BITS_LENGTH];
      randomAccessFile.read(retrievedLog);
      return retrievedLog;
    }
  }

  private BlockHeader createBlock(final long number) {
    return createBlock(number, Optional.empty());
  }

  private BlockHeader createBlock(final long number, final Optional<String> message) {
    final Address testAddress =
        Address.fromHexString(message.orElse(String.format("%02X", number)));
    final Bytes testMessage = Bytes.fromHexString(String.format("%02X", number));
    final Log testLog = new Log(testAddress, testMessage, List.of());
    final BlockHeader fakeHeader =
        new BlockHeader(
            Hash.EMPTY,
            Hash.EMPTY,
            Address.ZERO,
            Hash.EMPTY,
            Hash.EMPTY,
            Hash.EMPTY,
            LogsBloomFilter.builder().insertLog(testLog).build(),
            Difficulty.ZERO,
            number,
            0,
            0,
            0,
            Bytes.EMPTY,
            null,
            Bytes32.wrap(Hash.EMPTY.getBytes()),
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new MainnetBlockHeaderFunctions());
    testHash = fakeHeader.getHash();
    when(blockchain.getBlockHeader(number)).thenReturn(Optional.of(fakeHeader));
    return fakeHeader;
  }

  @Test
  public void isDiskFull_returnsTrueWhenMessageMatches() {
    final IOException e = new IOException("write failed: No space left on device");

    assertThat(TransactionLogBloomCacher.isDiskFull(e)).isTrue();
  }

  @Test
  public void isDiskFull_returnsFalseForUnrelatedMessage() {
    final IOException e = new IOException("permission denied");

    assertThat(TransactionLogBloomCacher.isDiskFull(e)).isFalse();
  }

  @Test
  public void isDiskFull_returnsFalseWhenMessageIsNull() {
    final IOException e = new IOException();

    assertThat(e.getMessage()).isNull();
    assertThat(TransactionLogBloomCacher.isDiskFull(e)).isFalse();
  }
}
