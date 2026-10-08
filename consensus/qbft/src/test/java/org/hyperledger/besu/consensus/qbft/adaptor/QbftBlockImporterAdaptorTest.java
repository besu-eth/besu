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
package org.hyperledger.besu.consensus.qbft.adaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.ethereum.mainnet.BlockImportResult.BlockImportStatus.ALREADY_IMPORTED;
import static org.hyperledger.besu.ethereum.mainnet.BlockImportResult.BlockImportStatus.IMPORTED;
import static org.hyperledger.besu.ethereum.mainnet.BlockImportResult.BlockImportStatus.NOT_IMPORTED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QbftBlockImporterAdaptorTest {
  @Mock private BlockImporter blockImporter;
  @Mock private BlockHeaderValidator blockHeaderValidator;
  @Mock private ProtocolContext protocolContext;
  @Mock private MutableBlockchain blockchain;
  @Mock private PathBasedWorldStateProvider worldStateArchive;
  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;
  @Mock private MutableWorldState headWorldState;
  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final Block parentBlock = generator.block();
  private final Block besuBlock =
      generator.block(
          BlockDataGenerator.BlockOptions.create()
              .setParentHash(parentBlock.getHash())
              .setBlockNumber(parentBlock.getHeader().getNumber() + 1));
  private final List<TransactionReceipt> receipts = generator.receipts(besuBlock);
  private final QbftBlock block = new QbftBlockAdaptor(besuBlock);
  private final ValidatedBlockCache validatedBlockCache = new ValidatedBlockCache();

  @Test
  void importsBlockSuccessfullyWhenBesuBlockImports() {
    when(blockImporter.importBlock(
            protocolContext,
            besuBlock,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            Optional.empty()))
        .thenReturn(new BlockImportResult(IMPORTED));

    QbftBlockImporterAdaptor qbftBlockImporter =
        new QbftBlockImporterAdaptor(
            blockImporter, blockHeaderValidator, protocolContext, validatedBlockCache);
    assertThat(qbftBlockImporter.importBlock(block, Optional.empty())).isEqualTo(true);
  }

  @Test
  void importsBlockSuccessfullyWhenBesuBlockAlreadyImported() {
    when(blockImporter.importBlock(
            protocolContext,
            besuBlock,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            Optional.empty()))
        .thenReturn(new BlockImportResult(ALREADY_IMPORTED));

    QbftBlockImporterAdaptor qbftBlockImporter =
        new QbftBlockImporterAdaptor(
            blockImporter, blockHeaderValidator, protocolContext, validatedBlockCache);
    assertThat(qbftBlockImporter.importBlock(block, Optional.empty())).isEqualTo(true);
  }

  @Test
  void importsBlockFailsWhenBesuBlockNotImported() {
    when(blockImporter.importBlock(
            protocolContext,
            besuBlock,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            Optional.empty()))
        .thenReturn(new BlockImportResult(NOT_IMPORTED));

    QbftBlockImporterAdaptor qbftBlockImporter =
        new QbftBlockImporterAdaptor(
            blockImporter, blockHeaderValidator, protocolContext, validatedBlockCache);
    assertThat(qbftBlockImporter.importBlock(block, Optional.empty())).isEqualTo(false);
  }

  @Test
  void importsValidatedBlockWithoutProcessingItAgain() {
    recordValidation();
    stubFastPathPreconditions();

    assertThat(newImporter().importBlock(block, Optional.empty())).isTrue();

    verify(blockchain).appendBlock(besuBlock, receipts, Optional.empty());
    verify(worldStateArchive).getWorldState(any());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void processesValidatedBlockAgainWhenParentIsNotChainHead() {
    recordValidation();
    stubFastPathPreconditions();
    when(blockchain.getChainHeadHash()).thenReturn(besuBlock.getHash());
    stubRegularImport(IMPORTED);

    assertThat(newImporter().importBlock(block, Optional.empty())).isTrue();

    verify(blockchain, never()).appendBlock(any(), any(), any());
  }

  @Test
  void processesValidatedBlockAgainWhenTrieLogIsMissing() {
    recordValidation();
    stubFastPathPreconditions();
    when(worldStateStorage.getTrieLog(besuBlock.getHash())).thenReturn(Optional.empty());
    stubRegularImport(IMPORTED);

    assertThat(newImporter().importBlock(block, Optional.empty())).isTrue();

    verify(blockchain, never()).appendBlock(any(), any(), any());
  }

  @Test
  void processesValidatedBlockAgainWhenHeaderIsInvalid() {
    recordValidation();
    stubFastPathPreconditions();
    when(blockHeaderValidator.validateHeader(
            besuBlock.getHeader(),
            parentBlock.getHeader(),
            protocolContext,
            HeaderValidationMode.FULL))
        .thenReturn(false);
    stubRegularImport(NOT_IMPORTED);

    assertThat(newImporter().importBlock(block, Optional.empty())).isFalse();

    verify(blockchain, never()).appendBlock(any(), any(), any());
  }

  @Test
  void validatedBlockIsOnlyImportedFromCacheOnce() {
    recordValidation();
    stubFastPathPreconditions();
    final QbftBlockImporterAdaptor importer = newImporter();
    assertThat(importer.importBlock(block, Optional.empty())).isTrue();

    stubRegularImport(ALREADY_IMPORTED);
    assertThat(importer.importBlock(block, Optional.empty())).isTrue();

    verify(blockchain).appendBlock(besuBlock, receipts, Optional.empty());
  }

  @Test
  void validatedBlockImportWaitsForTheBlockImporterLock() throws InterruptedException {
    recordValidation();
    stubFastPathPreconditions();
    final QbftBlockImporterAdaptor importer = newImporter();
    final AtomicBoolean imported = new AtomicBoolean();
    final Thread importThread =
        new Thread(() -> imported.set(importer.importBlock(block, Optional.empty())));
    final BlockImporter importerLock = blockImporter;

    synchronized (importerLock) {
      importThread.start();
      await()
          .atMost(Duration.ofSeconds(10))
          .until(
              () ->
                  importThread.getState() == Thread.State.BLOCKED
                      || importThread.getState() == Thread.State.TERMINATED);
      assertThat(importThread.getState()).isEqualTo(Thread.State.BLOCKED);
      verify(blockchain, never()).appendBlock(any(), any(), any());
    }

    importThread.join(Duration.ofSeconds(10));
    assertThat(imported).isTrue();
    verify(blockchain).appendBlock(besuBlock, receipts, Optional.empty());
  }

  private QbftBlockImporterAdaptor newImporter() {
    return new QbftBlockImporterAdaptor(
        blockImporter, blockHeaderValidator, protocolContext, validatedBlockCache);
  }

  private void recordValidation() {
    validatedBlockCache.put(
        besuBlock.getHash(),
        new ValidatedBlockCache.ValidatedBlock(
            besuBlock.getHeader().getNumber(), receipts, Optional.empty()));
  }

  private void stubFastPathPreconditions() {
    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
    lenient().when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    lenient().when(blockchain.getChainHeadHash()).thenReturn(parentBlock.getHash());
    lenient()
        .when(blockchain.getBlockHeader(parentBlock.getHash()))
        .thenReturn(Optional.of(parentBlock.getHeader()));
    lenient().when(worldStateArchive.getWorldStateKeyValueStorage()).thenReturn(worldStateStorage);
    lenient()
        .when(worldStateStorage.getTrieLog(besuBlock.getHash()))
        .thenReturn(Optional.of(new byte[0]));
    lenient()
        .when(
            blockHeaderValidator.validateHeader(
                besuBlock.getHeader(),
                parentBlock.getHeader(),
                protocolContext,
                HeaderValidationMode.FULL))
        .thenReturn(true);
    lenient().when(worldStateArchive.getWorldState(any())).thenReturn(Optional.of(headWorldState));
  }

  private void stubRegularImport(final BlockImportResult.BlockImportStatus status) {
    when(blockImporter.importBlock(
            protocolContext,
            besuBlock,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            Optional.empty()))
        .thenReturn(new BlockImportResult(status));
  }
}
