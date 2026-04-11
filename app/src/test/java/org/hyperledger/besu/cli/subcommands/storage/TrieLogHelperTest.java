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
package org.hyperledger.besu.cli.subcommands.storage;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.DEFAULT_TRIE_LOG_PRUNING_WINDOW_SIZE;
import static org.hyperledger.besu.plugin.services.storage.DataStorageFormat.BONSAI;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TrieLogHelperTest {

  private static final StorageProvider storageProvider = new InMemoryKeyValueStorageProvider();
  private static BonsaiWorldStateKeyValueStorage inMemoryWorldState;
  private TrieLogHelper nonValidatingTrieLogHelper;

  private static class NonValidatingTrieLogHelper extends TrieLogHelper {
    @Override
    void validatePruneConfiguration(final DataStorageConfiguration config) {}
  }

  @Mock private MutableBlockchain blockchain;
  static BlockHeader blockHeader1;
  static BlockHeader blockHeader2;
  static BlockHeader blockHeader3;
  static BlockHeader blockHeader4;

  static BlockHeader blockHeader5;

  @BeforeEach
  public void setup() throws IOException {

    blockHeader1 = new BlockHeaderTestFixture().number(1).buildHeader();
    blockHeader2 = new BlockHeaderTestFixture().number(2).buildHeader();
    blockHeader3 = new BlockHeaderTestFixture().number(3).buildHeader();
    blockHeader4 = new BlockHeaderTestFixture().number(4).buildHeader();
    blockHeader5 = new BlockHeaderTestFixture().number(5).buildHeader();

    inMemoryWorldState =
        new BonsaiWorldStateKeyValueStorage(
            storageProvider,
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);

    createTrieLog(blockHeader1);

    var updater = inMemoryWorldState.updater();
    updater
        .getTrieLogStorageTransaction()
        .put(blockHeader1.getHash().getBytes().toArrayUnsafe(), createTrieLog(blockHeader1));
    updater
        .getTrieLogStorageTransaction()
        .put(blockHeader2.getHash().getBytes().toArrayUnsafe(), createTrieLog(blockHeader2));
    updater
        .getTrieLogStorageTransaction()
        .put(blockHeader3.getHash().getBytes().toArrayUnsafe(), createTrieLog(blockHeader3));
    updater
        .getTrieLogStorageTransaction()
        .put(blockHeader4.getHash().getBytes().toArrayUnsafe(), createTrieLog(blockHeader4));
    updater
        .getTrieLogStorageTransaction()
        .put(blockHeader5.getHash().getBytes().toArrayUnsafe(), createTrieLog(blockHeader5));
    updater.getTrieLogStorageTransaction().commit();

    nonValidatingTrieLogHelper = new NonValidatingTrieLogHelper();
  }

  private static byte[] createTrieLog(final BlockHeader blockHeader) {
    TrieLogLayer trieLogLayer = new TrieLogLayer();
    trieLogLayer.setBlockHash(blockHeader.getBlockHash());
    final BytesValueRLPOutput rlpLog = new BytesValueRLPOutput();
    TrieLogFactoryImpl.writeTo(trieLogLayer, rlpLog);
    return rlpLog.encoded().toArrayUnsafe();
  }

  void mockBlockchainBase() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(5L);
    when(blockchain.getFinalized()).thenReturn(Optional.of(blockHeader3.getBlockHash()));
    when(blockchain.getBlockHeader(any(Hash.class))).thenReturn(Optional.of(blockHeader3));
  }

  @Test
  public void pruneFailsWhenBatchFileContainsJavaSerialization(final @TempDir Path dataDir)
      throws IOException {
    Files.createDirectories(dataDir.resolve("database"));

    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(3L)
                    .limitTrieLogsEnabled(true)
                    .build())
            .build();

    mockBlockchainBase();
    when(blockchain.getBlockHeader(5)).thenReturn(Optional.of(blockHeader5));
    when(blockchain.getBlockHeader(4)).thenReturn(Optional.of(blockHeader4));
    when(blockchain.getBlockHeader(3)).thenReturn(Optional.of(blockHeader3));

    // Pre-place a Java-serialized file at the expected batch file path.
    // saveTrieLogsAsRlpInFile's exists-check will skip overwriting it.
    // readTrieLogsAsRlpFromFile will then fail to parse it as RLP during restore,
    // confirming the code no longer silently deserializes Java-serialized objects.
    Path batchFile = dataDir.resolve("database").resolve("trieLogsToRetain-1");
    try (var oos =
        new java.io.ObjectOutputStream(new java.io.FileOutputStream(batchFile.toFile()))) {
      oos.writeObject("notrlp");
    }

    assertThatThrownBy(
            () ->
                nonValidatingTrieLogHelper.prune(
                    dataStorageConfiguration, inMemoryWorldState, blockchain, dataDir))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("RLP");
  }

  @Test
  public void prune(final @TempDir Path dataDir) throws IOException {
    Files.createDirectories(dataDir.resolve("database"));

    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(3L)
                    .limitTrieLogsEnabled(true)
                    .build())
            .build();

    mockBlockchainBase();
    when(blockchain.getBlockHeader(5)).thenReturn(Optional.of(blockHeader5));
    when(blockchain.getBlockHeader(4)).thenReturn(Optional.of(blockHeader4));
    when(blockchain.getBlockHeader(3)).thenReturn(Optional.of(blockHeader3));

    // assert trie logs that will be pruned exist before prune call
    assertThat(inMemoryWorldState.getTrieLog(blockHeader1.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader1));
    assertThat(inMemoryWorldState.getTrieLog(blockHeader2.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader2));
    assertThat(inMemoryWorldState.getTrieLog(blockHeader3.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader3));

    nonValidatingTrieLogHelper.prune(
        dataStorageConfiguration, inMemoryWorldState, blockchain, dataDir);

    // assert pruned trie logs are not in the DB
    assertThat(inMemoryWorldState.getTrieLog(blockHeader1.getHash())).isEqualTo(Optional.empty());
    assertThat(inMemoryWorldState.getTrieLog(blockHeader2.getHash())).isEqualTo(Optional.empty());

    // assert retained trie logs are in the DB
    assertThat(inMemoryWorldState.getTrieLog(blockHeader3.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader3));
    assertThat(inMemoryWorldState.getTrieLog(blockHeader4.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader4));
    assertThat(inMemoryWorldState.getTrieLog(blockHeader5.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader5));
  }

  @Test
  public void cannotPruneIfNoFinalizedIsFound() {
    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(2L)
                    .limitTrieLogsEnabled(true)
                    .build())
            .build();

    when(blockchain.getChainHeadBlockNumber()).thenReturn(5L);
    when(blockchain.getFinalized()).thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                nonValidatingTrieLogHelper.prune(
                    dataStorageConfiguration, inMemoryWorldState, blockchain, Path.of("")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("No finalized block present, can't safely run trie log prune");
  }

  @Test
  public void cannotPruneIfUserRetainsMoreLayersThanExistingChainLength() {
    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(10L)
                    .limitTrieLogsEnabled(true)
                    .build())
            .build();

    when(blockchain.getChainHeadBlockNumber()).thenReturn(5L);

    assertThatThrownBy(
            () ->
                nonValidatingTrieLogHelper.prune(
                    dataStorageConfiguration, inMemoryWorldState, blockchain, Path.of("")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Trying to retain more trie logs than chain length (5), skipping pruning");
  }

  @Test
  public void cannotPruneIfUserRequiredFurtherThanFinalized(final @TempDir Path dataDir) {

    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(2L)
                    .limitTrieLogsEnabled(true)
                    .build())
            .build();

    mockBlockchainBase();

    assertThatThrownBy(
            () ->
                nonValidatingTrieLogHelper.prune(
                    dataStorageConfiguration, inMemoryWorldState, blockchain, dataDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Trying to prune more layers than the finalized block height, skipping pruning");
  }

  @Test
  public void skipPruningIfTrieLogCountIsLessThanMaxLayersToLoad() {

    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(6L)
                    .limitTrieLogsEnabled(true)
                    .build())
            .build();

    when(blockchain.getChainHeadBlockNumber()).thenReturn(5L);

    assertThatThrownBy(
            () ->
                nonValidatingTrieLogHelper.prune(
                    dataStorageConfiguration, inMemoryWorldState, blockchain, Path.of("")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Trie log count (5) is less than retention limit (6), skipping pruning");
  }

  @Test
  public void mismatchInPrunedTrieLogCountShouldNotDeleteFiles(final @TempDir Path dataDir)
      throws IOException {
    Files.createDirectories(dataDir.resolve("database"));

    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(3L)
                    .limitTrieLogsEnabled(true)
                    .build())
            .build();

    mockBlockchainBase();
    when(blockchain.getBlockHeader(5)).thenReturn(Optional.of(blockHeader5));
    when(blockchain.getBlockHeader(4)).thenReturn(Optional.of(blockHeader4));
    when(blockchain.getBlockHeader(3)).thenReturn(Optional.of(blockHeader3));

    final BonsaiWorldStateKeyValueStorage inMemoryWorldStateSpy = spy(inMemoryWorldState);
    // force a different value the second time the trie log count is called
    when(inMemoryWorldStateSpy.streamTrieLogKeys(3L + DEFAULT_TRIE_LOG_PRUNING_WINDOW_SIZE))
        .thenCallRealMethod()
        .thenReturn(Stream.empty());
    assertThatThrownBy(
            () ->
                nonValidatingTrieLogHelper.prune(
                    dataStorageConfiguration, inMemoryWorldStateSpy, blockchain, dataDir))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Remaining trie logs (0) did not match --bonsai-historical-block-limit (3)");
  }

  @Test
  public void trieLogRetentionLimitShouldBeAboveMinimum() {

    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(511L)
                    .limitTrieLogsEnabled(true)
                    .build())
            .build();

    TrieLogHelper helper = new TrieLogHelper();
    assertThatThrownBy(
            () ->
                helper.prune(dataStorageConfiguration, inMemoryWorldState, blockchain, Path.of("")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("--bonsai-historical-block-limit minimum value is 512");
  }

  @Test
  public void trieLogPruningWindowSizeShouldBePositive() {

    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(512L)
                    .limitTrieLogsEnabled(true)
                    .trieLogPruningWindowSize(0)
                    .build())
            .build();

    TrieLogHelper helper = new TrieLogHelper();
    assertThatThrownBy(
            () ->
                helper.prune(dataStorageConfiguration, inMemoryWorldState, blockchain, Path.of("")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("--bonsai-trie-logs-pruning-window-size=0 must be greater than 0");
  }

  @Test
  public void trieLogPruningWindowSizeShouldBeAboveRetentionLimit() {
    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(512L)
                    .limitTrieLogsEnabled(true)
                    .trieLogPruningWindowSize(512)
                    .build())
            .build();

    TrieLogHelper helper = new TrieLogHelper();
    assertThatThrownBy(
            () ->
                helper.prune(dataStorageConfiguration, inMemoryWorldState, blockchain, Path.of("")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "--bonsai-trie-logs-pruning-window-size=512 must be greater than --bonsai-historical-block-limit=512");
  }

  @Test
  public void exceptionWhileSavingFileStopsPruneProcess(final @TempDir Path dataDir) {

    DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(3L)
                    .limitTrieLogsEnabled(true)
                    .build())
            .build();

    mockBlockchainBase();
    when(blockchain.getBlockHeader(5)).thenReturn(Optional.of(blockHeader5));
    when(blockchain.getBlockHeader(4)).thenReturn(Optional.of(blockHeader4));
    when(blockchain.getBlockHeader(3)).thenReturn(Optional.of(blockHeader3));

    assertThatThrownBy(
            () ->
                nonValidatingTrieLogHelper.prune(
                    dataStorageConfiguration,
                    inMemoryWorldState,
                    blockchain,
                    dataDir.resolve("unknownPath")))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(java.io.IOException.class);

    // assert all trie logs are still in the DB
    assertThat(inMemoryWorldState.getTrieLog(blockHeader1.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader1));
    assertThat(inMemoryWorldState.getTrieLog(blockHeader2.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader2));
    assertThat(inMemoryWorldState.getTrieLog(blockHeader3.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader3));
    assertThat(inMemoryWorldState.getTrieLog(blockHeader4.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader4));
    assertThat(inMemoryWorldState.getTrieLog(blockHeader5.getHash()).get())
        .isEqualTo(createTrieLog(blockHeader5));
  }

  @Test
  public void exportedTrieMatchesDbTrieLog(final @TempDir Path dataDir) throws IOException {
    nonValidatingTrieLogHelper.exportTrieLog(
        inMemoryWorldState,
        singletonList(blockHeader1.getHash()),
        dataDir.resolve("trie-log-dump"));

    var trieLog =
        nonValidatingTrieLogHelper
            .readTrieLogsAsRlpFromFile(dataDir.resolve("trie-log-dump").toString())
            .entrySet()
            .stream()
            .findFirst()
            .get();

    assertThat(trieLog.getKey()).isEqualTo(blockHeader1.getHash().getBytes().toArrayUnsafe());
    assertThat(trieLog.getValue())
        .isEqualTo(inMemoryWorldState.getTrieLog(blockHeader1.getHash()).get());
  }

  @Test
  public void exportedMultipleTriesMatchDbTrieLogs(final @TempDir Path dataDir) throws IOException {
    nonValidatingTrieLogHelper.exportTrieLog(
        inMemoryWorldState,
        List.of(blockHeader1.getHash(), blockHeader2.getHash(), blockHeader3.getHash()),
        dataDir.resolve("trie-log-dump"));

    var trieLogs =
        nonValidatingTrieLogHelper
            .readTrieLogsAsRlpFromFile(dataDir.resolve("trie-log-dump").toString())
            .entrySet()
            .stream()
            .collect(Collectors.toMap(e -> Bytes.wrap(e.getKey()), Map.Entry::getValue));

    assertThat(trieLogs.get(blockHeader1.getHash().getBytes()))
        .isEqualTo(inMemoryWorldState.getTrieLog(blockHeader1.getHash()).get());
    assertThat(trieLogs.get(blockHeader2.getHash().getBytes()))
        .isEqualTo(inMemoryWorldState.getTrieLog(blockHeader2.getHash()).get());
    assertThat(trieLogs.get(blockHeader3.getHash().getBytes()))
        .isEqualTo(inMemoryWorldState.getTrieLog(blockHeader3.getHash()).get());
  }

  @Test
  public void importedTrieLogMatchesDbTrieLog(final @TempDir Path dataDir) throws IOException {
    StorageProvider tempStorageProvider = new InMemoryKeyValueStorageProvider();
    BonsaiWorldStateKeyValueStorage inMemoryWorldState2 =
        new BonsaiWorldStateKeyValueStorage(
            tempStorageProvider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_CONFIG);

    nonValidatingTrieLogHelper.exportTrieLog(
        inMemoryWorldState,
        singletonList(blockHeader1.getHash()),
        dataDir.resolve("trie-log-dump"));

    var trieLog =
        nonValidatingTrieLogHelper.readTrieLogsAsRlpFromFile(
            dataDir.resolve("trie-log-dump").toString());
    var updater = inMemoryWorldState2.updater();

    trieLog.forEach((k, v) -> updater.getTrieLogStorageTransaction().put(k, v));

    updater.getTrieLogStorageTransaction().commit();

    assertThat(inMemoryWorldState2.getTrieLog(blockHeader1.getHash()).get())
        .isEqualTo(inMemoryWorldState.getTrieLog(blockHeader1.getHash()).get());
  }

  @Test
  public void importedMultipleTriesMatchDbTrieLogs(final @TempDir Path dataDir) throws IOException {
    StorageProvider tempStorageProvider = new InMemoryKeyValueStorageProvider();
    BonsaiWorldStateKeyValueStorage inMemoryWorldState2 =
        new BonsaiWorldStateKeyValueStorage(
            tempStorageProvider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_CONFIG);

    nonValidatingTrieLogHelper.exportTrieLog(
        inMemoryWorldState,
        List.of(blockHeader1.getHash(), blockHeader2.getHash(), blockHeader3.getHash()),
        dataDir.resolve("trie-log-dump"));

    var trieLog =
        nonValidatingTrieLogHelper.readTrieLogsAsRlpFromFile(
            dataDir.resolve("trie-log-dump").toString());
    var updater = inMemoryWorldState2.updater();

    trieLog.forEach((k, v) -> updater.getTrieLogStorageTransaction().put(k, v));

    updater.getTrieLogStorageTransaction().commit();

    assertThat(inMemoryWorldState2.getTrieLog(blockHeader1.getHash()).get())
        .isEqualTo(inMemoryWorldState.getTrieLog(blockHeader1.getHash()).get());
    assertThat(inMemoryWorldState2.getTrieLog(blockHeader2.getHash()).get())
        .isEqualTo(inMemoryWorldState.getTrieLog(blockHeader2.getHash()).get());
    assertThat(inMemoryWorldState2.getTrieLog(blockHeader3.getHash()).get())
        .isEqualTo(inMemoryWorldState.getTrieLog(blockHeader3.getHash()).get());
  }
}
