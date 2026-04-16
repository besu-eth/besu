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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

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

  @Test
  public void shouldSplitLogsIntoSeveralFiles() {

    when(blockchain.getChainHeadBlockNumber()).thenReturn(200003L);
    assertThat(cacheDir.toFile().list().length).isEqualTo(0);
    transactionLogBloomCacher.cacheAll();
    assertThat(cacheDir.toFile().list().length).isEqualTo(2);
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
