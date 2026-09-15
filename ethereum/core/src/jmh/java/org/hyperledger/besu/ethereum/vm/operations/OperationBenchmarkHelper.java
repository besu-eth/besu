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
package org.hyperledger.besu.ethereum.vm.operations;

import static java.util.Collections.emptyList;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MessageFrameTestFixture;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.OptimisticRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;

public class OperationBenchmarkHelper {

  private final Path storageDirectory;
  private final KeyValueStorage keyValueStorage;
  private final MessageFrame messageFrame;
  private final Blockchain blockchain;

  private OperationBenchmarkHelper(
      final Path storageDirectory,
      final KeyValueStorage keyValueStorage,
      final MessageFrame messageFrame,
      final Blockchain blockchain) {
    this.storageDirectory = storageDirectory;
    this.keyValueStorage = keyValueStorage;
    this.messageFrame = messageFrame;
    this.blockchain = blockchain;
  }

  public static OperationBenchmarkHelper create() throws IOException {
    final Path storageDirectory = Files.createTempDirectory("benchmark");
    final OptimisticRocksDBColumnarKeyValueStorage optimisticRocksDBColumnarKeyValueStorage =
        new OptimisticRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder().databaseDir(storageDirectory).build(),
            List.of(KeyValueSegmentIdentifier.DEFAULT, KeyValueSegmentIdentifier.BLOCKCHAIN),
            emptyList(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    final KeyValueStorage keyValueStorage =
        new SegmentedKeyValueStorageAdapter(
            KeyValueSegmentIdentifier.BLOCKCHAIN, optimisticRocksDBColumnarKeyValueStorage);

    final ExecutionContextTestFixture executionContext =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource("/genesis-jmh.json"))
            .blockchainKeyValueStorage(keyValueStorage)
            .build();
    final MutableBlockchain blockchain = executionContext.getBlockchain();

    for (int i = 1; i < 256; i++) {
      blockchain.appendBlock(
          new Block(
              new BlockHeaderTestFixture()
                  .parentHash(blockchain.getChainHeadHash())
                  .number(i)
                  .difficulty(Difficulty.ONE)
                  .buildHeader(),
              new BlockBody(emptyList(), emptyList())),
          emptyList());
    }
    final MessageFrame messageFrame =
        new MessageFrameTestFixture()
            .executionContextTestFixture(executionContext)
            .blockHeader(
                new BlockHeaderTestFixture()
                    .parentHash(blockchain.getChainHeadHash())
                    .number(blockchain.getChainHeadBlockNumber() + 1)
                    .difficulty(Difficulty.ONE)
                    .buildHeader())
            .build();
    return new OperationBenchmarkHelper(
        storageDirectory, keyValueStorage, messageFrame, blockchain);
  }

  public Blockchain getBlockchain() {
    return blockchain;
  }

  public MessageFrame createMessageFrame() {
    return createMessageFrameBuilder().build();
  }

  public MessageFrame.Builder createMessageFrameBuilder() {
    final MessageFrame.Builder builder =
        MessageFrame.builder()
            .parentMessageFrame(messageFrame)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .worldUpdater(messageFrame.getWorldUpdater())
            .initialGas(messageFrame.getRemainingGas())
            .address(messageFrame.getContractAddress())
            .contract(messageFrame.getRecipientAddress())
            .inputData(messageFrame.getInputData())
            .sender(messageFrame.getSenderAddress())
            .value(messageFrame.getValue())
            .apparentValue(messageFrame.getApparentValue())
            .code(messageFrame.getCode())
            .isStatic(messageFrame.isStatic())
            .completer(frame -> {});
    messageFrame.getEip7928AccessList().ifPresent(builder::eip7928AccessList);
    return builder;
  }

  public void cleanUp() throws IOException {
    keyValueStorage.close();
    MoreFiles.deleteRecursively(storageDirectory, RecursiveDeleteOption.ALLOW_INSECURE);
  }
}
