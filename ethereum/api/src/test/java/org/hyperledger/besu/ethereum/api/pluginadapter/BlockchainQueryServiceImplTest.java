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
package org.hyperledger.besu.ethereum.api.pluginadapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.handlers.RpcMethodTimeoutException;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;
import org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.plugin.data.LogWithMetadata;
import org.hyperledger.besu.plugin.services.BlockchainQueryService;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the service over the hive test chain, comparing results both to known values of that chain
 * and to logs read directly from the stored receipts.
 */
class BlockchainQueryServiceImplTest {

  /** The contract that emits every log in the hive test chain. */
  private static final Address LOG_CONTRACT =
      Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f");

  private static final Address OTHER_ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000deadbeef");

  /** Topic 0 of the log in block 0x17. */
  private static final Bytes32 TOPIC_A =
      Bytes32.fromHexString("0x65c9ac8011e286e89d02a269890f41d67ca2cc597b2c76c7c69321ff492be580");

  private static final Bytes32 TOPIC_UNUSED =
      Bytes32.fromHexString("0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b");

  private static final Hash BLOCK_23_TX =
      Hash.fromHexString("0x97a385bf570ced7821c6495b3877ddd2afd5c452f350f0d4876e98d9161389c6");
  private static final Hash BLOCK_24_TX =
      Hash.fromHexString("0x5ecd942096ab3f70c5bcc8f3a98f88c4ff0a3bd986417df9948eb1819db76d0e");

  private static BlockchainSetupUtil blockchainSetupUtil;

  private MutableBlockchain blockchain;
  private BlockchainQueries blockchainQueries;
  private BlockchainQueryService service;
  private long head;

  @BeforeAll
  static void setupChain() {
    blockchainSetupUtil = BlockchainSetupUtil.forHiveTesting(DataStorageFormat.BONSAI);
    blockchainSetupUtil.importAllBlocks();
  }

  @BeforeEach
  void setup() {
    blockchain = blockchainSetupUtil.getBlockchain();
    blockchainQueries =
        new BlockchainQueries(
            blockchainSetupUtil.getProtocolSchedule(),
            blockchain,
            blockchainSetupUtil.getWorldArchive(),
            MiningConfiguration.newDefault());
    service = new BlockchainQueryServiceImpl(blockchainQueries);
    head = blockchain.getChainHeadBlockNumber();
  }

  // --- fee suggestions -------------------------------------------------------------------------

  @Test
  void gasPriceIsTheBlockchainQueriesSuggestion() {
    assertThat(service.getOracleGasPrice()).isEqualTo(blockchainQueries.gasPrice());
  }

  @Test
  void gasPriceFollowsMiningConfiguration() {
    final Wei minGasPrice = Wei.of(123_456_789_000L);
    final BlockchainQueries queries =
        new BlockchainQueries(
            blockchainSetupUtil.getProtocolSchedule(),
            blockchain,
            blockchainSetupUtil.getWorldArchive(),
            MiningConfiguration.newDefault().setMinTransactionGasPrice(minGasPrice));
    final BlockchainQueryService configured = new BlockchainQueryServiceImpl(queries);

    assertThat(configured.getOracleGasPrice()).isEqualTo(queries.gasPrice());
    assertThat(configured.getOracleGasPrice()).isGreaterThanOrEqualTo(minGasPrice);
  }

  @Test
  void maxPriorityFeeIsTheBlockchainQueriesSuggestion() {
    assertThat(service.getOracleMaxPriorityFeePerGas())
        .isEqualTo(blockchainQueries.gasPriorityFee());
  }

  @Test
  void feeSuggestionsMatchKnownValuesWithZeroMinGasPrice() {
    // With no minimum gas price, the hive test chain yields these suggestions.
    final BlockchainQueryService zeroMinGasPrice =
        new BlockchainQueryServiceImpl(
            new BlockchainQueries(
                blockchainSetupUtil.getProtocolSchedule(),
                blockchain,
                blockchainSetupUtil.getWorldArchive(),
                Optional.empty(),
                Optional.empty(),
                ImmutableApiConfiguration.builder().build(),
                MiningConfiguration.newDefault().setMinTransactionGasPrice(Wei.ZERO)));

    assertThat(zeroMinGasPrice.getOracleGasPrice()).isEqualTo(Wei.fromHexString("0x2dbc88c1"));
    assertThat(zeroMinGasPrice.getOracleMaxPriorityFeePerGas())
        .isEqualTo(Wei.fromHexString("0x3b9aca00"));
  }

  @Test
  void feeSuggestionsAreNotClampedByGasAndPriorityFeeLimiting() {
    final BlockchainQueryService limited =
        new BlockchainQueryServiceImpl(
            new BlockchainQueries(
                blockchainSetupUtil.getProtocolSchedule(),
                blockchain,
                blockchainSetupUtil.getWorldArchive(),
                Optional.empty(),
                Optional.empty(),
                ImmutableApiConfiguration.builder()
                    .isGasAndPriorityFeeLimitingEnabled(true)
                    .lowerBoundGasAndPriorityFeeCoefficient(1_000L)
                    .upperBoundGasAndPriorityFeeCoefficient(1_001L)
                    .build(),
                MiningConfiguration.newDefault().setMinTransactionGasPrice(Wei.ZERO)));

    assertThat(limited.getOracleGasPrice()).isEqualTo(Wei.fromHexString("0x2dbc88c1"));
    assertThat(limited.getOracleMaxPriorityFeePerGas()).isEqualTo(Wei.fromHexString("0x3b9aca00"));
  }

  // --- range logs ------------------------------------------------------------------------------

  @Test
  void rangeLogsMatchKnownLogs() {
    // Blocks 20-24 with empty topics and addresses: the logs of blocks 0x17 and 0x18.
    final List<LogWithMetadata> logs = service.getLogs(20, 24, List.of(), List.of(), () -> true);

    assertThat(logs).extracting(LogWithMetadata::getBlockNumber).containsExactly(0x17L, 0x18L);
    assertThat(logs)
        .extracting(LogWithMetadata::getTransactionHash)
        .containsExactly(BLOCK_23_TX, BLOCK_24_TX);
    assertThat(logs).extracting(LogWithMetadata::getLogIndex).containsExactly(0, 0);
    assertThat(logs).extracting(LogWithMetadata::getLogger).containsOnly(LOG_CONTRACT);
    assertThat(logs.get(0).getTopics()).containsExactly(LogTopic.wrap(TOPIC_A));
    assertThat(logs.get(1).getTopics()).isEmpty();
    assertThat(logs.get(0).getData())
        .isEqualTo(
            Bytes.fromHexString(
                "0x000000000000000000000000000000000000000000000000000000000000002a"));
    assertThat(logs).extracting(LogWithMetadata::isRemoved).containsOnly(false);
  }

  @Test
  void rangeLogsWithoutFilterReturnEveryLogInTheRange() {
    final List<LogWithMetadata> logs = service.getLogs(0, head, List.of(), List.of(), () -> true);

    assertThat(logs).isNotEmpty();
    assertThat(keys(logs)).isEqualTo(expectedKeys(0, head, log -> true));
  }

  @Test
  void rangeLogsAreTheSameAsBlockchainQueriesQuery() {
    final List<List<Bytes32>> topics = List.of(List.of(TOPIC_A, TOPIC_UNUSED));
    final List<LogWithMetadata> logs =
        service.getLogs(0, head, List.of(LOG_CONTRACT), topics, () -> true);

    // The same filter, queried directly through BlockchainQueries.
    final LogsQuery coreQuery =
        new LogsQuery.Builder()
            .addresses(List.of(LOG_CONTRACT))
            .topics(List.of(List.of(LogTopic.wrap(TOPIC_A), LogTopic.wrap(TOPIC_UNUSED))))
            .build();
    final List<org.hyperledger.besu.ethereum.core.LogWithMetadata> coreLogs =
        blockchainQueries.matchingLogs(0, head, coreQuery, () -> true);

    assertThat(logs).isNotEmpty();
    assertThat(logs).containsExactlyElementsOf(coreLogs);
  }

  @Test
  void rangeLogsFilterByAddress() {
    final List<LogWithMetadata> matching =
        service.getLogs(0, head, List.of(LOG_CONTRACT), List.of(), () -> true);
    assertThat(matching).isNotEmpty();
    assertThat(keys(matching))
        .isEqualTo(expectedKeys(0, head, log -> log.logger().equals(LOG_CONTRACT)));

    assertThat(service.getLogs(0, head, List.of(OTHER_ADDRESS), List.of(), () -> true)).isEmpty();

    // Any of several addresses.
    assertThat(
            keys(
                service.getLogs(
                    0, head, List.of(OTHER_ADDRESS, LOG_CONTRACT), List.of(), () -> true)))
        .isEqualTo(keys(matching));
  }

  @Test
  void rangeLogsFilterByExactTopic() {
    final List<LogWithMetadata> logs =
        service.getLogs(0, head, List.of(), List.of(List.of(TOPIC_A)), () -> true);

    assertThat(logs).isNotEmpty();
    assertThat(logs)
        .allSatisfy(log -> assertThat(log.getTopics().get(0)).isEqualTo(LogTopic.wrap(TOPIC_A)));
    assertThat(keys(logs)).isEqualTo(expectedKeys(0, head, topicAt(0, TOPIC_A)));
    assertThat(logs).extracting(LogWithMetadata::getTransactionHash).contains(BLOCK_23_TX);
    assertThat(logs).extracting(LogWithMetadata::getTransactionHash).doesNotContain(BLOCK_24_TX);

    assertThat(service.getLogs(0, head, List.of(), List.of(List.of(TOPIC_UNUSED)), () -> true))
        .isEmpty();
  }

  @Test
  void rangeLogsWithEmptyTopicListMatchAnyTopics() {
    assertThat(keys(service.getLogs(0, head, List.of(), List.of(), () -> true)))
        .isEqualTo(expectedKeys(0, head, log -> true));
  }

  @Test
  void emptyInnerTopicListIsAWildcardForThatPosition() {
    // As in subscribeLogs, a wildcard position still requires the log to have a
    // topic at that position, so the topic-less log in block 0x18 is excluded.
    final List<List<Bytes32>> topics = List.of(List.of());
    final List<LogWithMetadata> logs = service.getLogs(20, 24, List.of(), topics, () -> true);

    assertThat(logs).extracting(LogWithMetadata::getTransactionHash).containsExactly(BLOCK_23_TX);
    assertThat(keys(service.getLogs(0, head, List.of(), topics, () -> true)))
        .isEqualTo(expectedKeys(0, head, log -> !log.topics().isEmpty()));
  }

  @Test
  void nullInnerTopicListIsAWildcardForThatPosition() {
    final List<List<Bytes32>> topics = Collections.singletonList(null);
    final List<List<Bytes32>> emptyPosition = List.of(List.of());

    assertThat(keys(service.getLogs(0, head, List.of(), topics, () -> true)))
        .isEqualTo(keys(service.getLogs(0, head, List.of(), emptyPosition, () -> true)));
  }

  @Test
  void nullTopicEntryIsAWildcard() {
    final List<List<Bytes32>> nullEntry = List.of(Arrays.asList(TOPIC_UNUSED, null));
    final List<List<Bytes32>> emptyPosition = List.of(List.of());

    assertThat(keys(service.getLogs(0, head, List.of(), nullEntry, () -> true)))
        .isNotEmpty()
        .isEqualTo(keys(service.getLogs(0, head, List.of(), emptyPosition, () -> true)));
  }

  @Test
  void wildcardPositionBeforeExactTopic() {
    // Position 0 any, position 1 must be TOPIC_A.
    final List<List<Bytes32>> topics = List.of(List.of(), List.of(TOPIC_A));
    assertThat(keys(service.getLogs(0, head, List.of(), topics, () -> true)))
        .isEqualTo(expectedKeys(0, head, topicAt(1, TOPIC_A)));
  }

  @Test
  void nullFilterArgumentsAreRejected() {
    assertThatThrownBy(() -> service.getLogs(0, head, null, List.of(), () -> true))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("addresses");
    assertThatThrownBy(() -> service.getLogs(0, head, List.of(), null, () -> true))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("topics");
    assertThatThrownBy(() -> service.getLogs(Hash.ZERO, null, List.of()))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("addresses");
    assertThatThrownBy(() -> service.getLogs(Hash.ZERO, List.of(), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("topics");
  }

  @Test
  void rangeBeyondHeadStopsAtHead() {
    assertThat(keys(service.getLogs(20, head + 1_000, List.of(), List.of(), () -> true)))
        .isEqualTo(keys(service.getLogs(20, head, List.of(), List.of(), () -> true)));

    assertThat(service.getLogs(head + 1, head + 1_000, List.of(), List.of(), () -> true)).isEmpty();
  }

  @Test
  void unboundedRangeIsClampedToTheHead() {
    final AtomicInteger checks = new AtomicInteger();
    // Fails the query if the service walks segments far past the head instead of clamping.
    final BooleanSupplier aliveForAFewChecks = () -> checks.incrementAndGet() < 1_000;

    final List<LogWithMetadata> logs =
        service.getLogs(0, Long.MAX_VALUE, List.of(), List.of(), aliveForAFewChecks);

    assertThat(keys(logs)).isEqualTo(expectedKeys(0, head, log -> true));
    assertThat(checks.get()).isLessThan(1_000);
  }

  @Test
  void unboundedRangeStartingPastTheHeadIsEmpty() {
    assertThat(
            service.getLogs(head + 1, Long.MAX_VALUE, List.of(), List.of(), () -> checkNotCalled()))
        .isEmpty();
  }

  @Test
  void negativeFromBlockNumberIsRejected() {
    assertThatThrownBy(() -> service.getLogs(-1, head, List.of(), List.of(), () -> true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fromBlockNumber");
  }

  @Test
  void nullIsQueryAliveIsRejected() {
    assertThatThrownBy(() -> service.getLogs(0, head, List.of(), List.of(), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("isQueryAlive");
  }

  @Test
  void invertedRangeIsEmpty() {
    assertThat(service.getLogs(24, 20, List.of(), List.of(), () -> true)).isEmpty();
  }

  @Test
  void deadQueryAbortsWithIllegalStateException() {
    assertThatThrownBy(() -> service.getLogs(0, head, List.of(), List.of(), () -> false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Error retrieving matching logs")
        .hasRootCauseInstanceOf(RpcMethodTimeoutException.class);
  }

  @Test
  void queryThatDiesPartwayAbortsWithIllegalStateException() {
    final AtomicInteger checks = new AtomicInteger();
    final BooleanSupplier aliveForAWhile = () -> checks.incrementAndGet() < 5;

    assertThatThrownBy(() -> service.getLogs(0, head, List.of(), List.of(), aliveForAWhile))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Error retrieving matching logs")
        .hasRootCauseInstanceOf(RpcMethodTimeoutException.class);
    assertThat(checks.get()).isGreaterThan(1);
  }

  @Test
  void returnedListIsUnmodifiable() {
    final List<LogWithMetadata> logs = service.getLogs(20, 24, List.of(), List.of(), () -> true);
    assertThatThrownBy(logs::clear).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void rangeLogsUseTheLogBloomCacheWithTheSameResults(@TempDir final Path cacheDir) {
    new TransactionLogBloomCacher(blockchain, cacheDir, null).generateLogBloomCache(0, head + 1);
    assertThat(cacheDir.resolve("logBloom-0.cache")).isRegularFile();
    final BlockchainQueryService cached =
        new BlockchainQueryServiceImpl(
            new BlockchainQueries(
                blockchainSetupUtil.getProtocolSchedule(),
                blockchain,
                blockchainSetupUtil.getWorldArchive(),
                Optional.of(cacheDir),
                Optional.empty(),
                MiningConfiguration.newDefault()));

    for (final List<List<Bytes32>> topics :
        List.<List<List<Bytes32>>>of(
            List.of(),
            List.of(List.of(TOPIC_A)),
            List.of(Arrays.asList(TOPIC_UNUSED, null)),
            List.of(List.of(), List.of(TOPIC_UNUSED)))) {
      assertThat(keys(cached.getLogs(0, head + 10, List.of(LOG_CONTRACT), topics, () -> true)))
          .as("topics %s", topics)
          .isEqualTo(keys(service.getLogs(0, head, List.of(LOG_CONTRACT), topics, () -> true)));
    }
  }

  // --- block logs ------------------------------------------------------------------------------

  @Test
  void blockLogsMatchKnownLog() {
    // Block 0x17, topic 0 either of two values.
    final Hash blockHash = blockchain.getBlockHashByNumber(0x17).orElseThrow();
    final List<LogWithMetadata> logs =
        service.getLogs(blockHash, List.of(), List.of(List.of(TOPIC_UNUSED, TOPIC_A)));

    assertThat(logs).hasSize(1);
    final LogWithMetadata log = logs.get(0);
    assertThat(log.getTransactionHash()).isEqualTo(BLOCK_23_TX);
    assertThat(log.getBlockHash()).isEqualTo(blockHash);
    assertThat(log.getBlockNumber()).isEqualTo(0x17L);
    assertThat(log.getLogIndex()).isZero();
    assertThat(log.getLogger()).isEqualTo(LOG_CONTRACT);
    assertThat(log.isRemoved()).isFalse();
  }

  @Test
  void blockLogsMatchRangeLogsForEveryBlock() {
    for (long number = 0; number <= head; number++) {
      final Hash blockHash = blockchain.getBlockHashByNumber(number).orElseThrow();
      assertThat(service.getLogs(blockHash, List.of(), List.of()))
          .as("block %d", number)
          .containsExactlyElementsOf(
              service.getLogs(number, number, List.of(), List.of(), () -> true));
    }
  }

  @Test
  void blockLogsAreTheSameAsBlockchainQueriesQuery() {
    final Hash blockHash = blockchain.getBlockHashByNumber(0x17).orElseThrow();
    final LogsQuery coreQuery =
        new LogsQuery.Builder()
            .addresses(List.of(LOG_CONTRACT))
            .topics(List.of(Collections.singletonList(null)))
            .build();

    assertThat(service.getLogs(blockHash, List.of(LOG_CONTRACT), List.of(List.of())))
        .isNotEmpty()
        .containsExactlyElementsOf(
            blockchainQueries.matchingLogs(blockHash, coreQuery, () -> true));
  }

  @Test
  void blockLogsFilterByAddressAndTopic() {
    final Hash blockHash = blockchain.getBlockHashByNumber(0x17).orElseThrow();

    assertThat(service.getLogs(blockHash, List.of(OTHER_ADDRESS), List.of())).isEmpty();
    assertThat(service.getLogs(blockHash, List.of(), List.of(List.of(TOPIC_UNUSED)))).isEmpty();
    assertThat(service.getLogs(blockHash, List.of(LOG_CONTRACT), List.of(List.of(TOPIC_A))))
        .hasSize(1);
  }

  @Test
  void blockLogsOfUnknownBlockAreEmpty() {
    assertThat(service.getLogs(Hash.ZERO, List.of(), List.of())).isEmpty();
  }

  @Test
  void blockLogsReadFailureIsAnIllegalStateException() {
    final BlockchainQueries failing = mock(BlockchainQueries.class);
    final RuntimeException cause = new RuntimeException("receipts missing");
    when(failing.matchingLogs(any(Hash.class), any(LogsQuery.class), any())).thenThrow(cause);
    final BlockchainQueryService failingService = new BlockchainQueryServiceImpl(failing);

    assertThatThrownBy(() -> failingService.getLogs(Hash.ZERO, List.of(), List.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasCause(cause);
  }

  @Test
  void blockLogsOfNonCanonicalBlockAreMarkedRemoved() {
    // Appending a side block mutates the chain, so use a chain of its own rather than the shared
    // one the other tests read.
    final BlockchainSetupUtil forkSetup =
        BlockchainSetupUtil.forHiveTesting(DataStorageFormat.BONSAI);
    forkSetup.importAllBlocks();
    final MutableBlockchain forkChain = forkSetup.getBlockchain();
    final BlockchainQueryService forkService =
        new BlockchainQueryServiceImpl(
            new BlockchainQueries(
                forkSetup.getProtocolSchedule(),
                forkChain,
                forkSetup.getWorldArchive(),
                MiningConfiguration.newDefault()));
    final Block canonical = forkChain.getBlockByNumber(0x17).orElseThrow();
    final List<TransactionReceipt> receipts =
        forkChain.getTxReceipts(canonical.getHash()).orElseThrow();
    final BlockHeader forkHeader =
        BlockHeaderBuilder.fromHeader(canonical.getHeader())
            .extraData(Bytes.fromHexString("0x666f726b"))
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildBlockHeader();
    final Block fork = new Block(forkHeader, canonical.getBody());
    forkChain.appendBlock(fork, receipts);
    assertThat(forkChain.blockIsOnCanonicalChain(fork.getHash())).isFalse();

    final List<LogWithMetadata> logs = forkService.getLogs(fork.getHash(), List.of(), List.of());

    assertThat(logs).hasSize(1);
    assertThat(logs.get(0).getBlockHash()).isEqualTo(fork.getHash());
    assertThat(logs.get(0).getTransactionHash()).isEqualTo(BLOCK_23_TX);
    assertThat(logs.get(0).isRemoved()).isTrue();
    // The range query only covers the canonical chain.
    assertThat(forkService.getLogs(0x17, 0x17, List.of(), List.of(), () -> true))
        .extracting(LogWithMetadata::getBlockHash)
        .containsExactly(canonical.getHash());
  }

  // --- helpers ---------------------------------------------------------------------------------

  private static boolean checkNotCalled() {
    throw new AssertionError("isQueryAlive should not be checked for an empty range");
  }

  /** A log identified independently of LogWithMetadata, read straight from stored receipts. */
  private record LogKey(
      long blockNumber,
      Hash blockHash,
      int transactionIndex,
      Hash transactionHash,
      int logIndex,
      Address logger,
      List<LogTopic> topics,
      Bytes data) {}

  private static List<LogKey> keys(final List<LogWithMetadata> logs) {
    return logs.stream()
        .map(
            log ->
                new LogKey(
                    log.getBlockNumber(),
                    log.getBlockHash(),
                    log.getTransactionIndex(),
                    log.getTransactionHash(),
                    log.getLogIndex(),
                    log.getLogger(),
                    List.copyOf(log.getTopics()),
                    log.getData()))
        .toList();
  }

  private List<LogKey> expectedKeys(
      final long from, final long to, final Predicate<LogKey> filter) {
    final List<LogKey> result = new ArrayList<>();
    for (long number = from; number <= Math.min(to, head); number++) {
      final Block block = blockchain.getBlockByNumber(number).orElseThrow();
      final List<TransactionReceipt> receipts =
          blockchain.getTxReceipts(block.getHash()).orElseThrow();
      final List<Transaction> transactions = block.getBody().getTransactions();
      int logIndex = 0;
      for (int i = 0; i < receipts.size(); i++) {
        for (final var log : receipts.get(i).getLogs()) {
          final LogKey key =
              new LogKey(
                  number,
                  block.getHash(),
                  i,
                  transactions.get(i).getHash(),
                  logIndex++,
                  log.getLogger(),
                  List.copyOf(log.getTopics()),
                  log.getData());
          if (filter.test(key)) {
            result.add(key);
          }
        }
      }
    }
    return result;
  }

  private static Predicate<LogKey> topicAt(final int position, final Bytes32 topic) {
    final Set<LogTopic> accepted = Set.of(LogTopic.wrap(topic));
    return key -> key.topics().size() > position && accepted.contains(key.topics().get(position));
  }
}
