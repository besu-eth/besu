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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.BodyValidationMode;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ValidatedBlockImporterTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  @Mock private BlockImporter blockImporter;
  @Mock private BlockValidator blockValidator;
  @Mock private ProtocolContext protocolContext;
  @Mock private MutableBlockchain blockchain;
  @Mock private BadBlockManager badBlockManager;
  @Mock private PathBasedWorldStateProvider worldStateArchive;
  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;
  @Mock private MutableWorldState headWorldState;
  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final Block parentBlock = generator.block();
  private final Block block =
      generator.block(
          BlockDataGenerator.BlockOptions.create()
              .setParentHash(parentBlock.getHash())
              .setBlockNumber(parentBlock.getHeader().getNumber() + 1));
  private final List<TransactionReceipt> receipts = generator.receipts(block);
  private final ValidatedBlockCache validatedBlockCache = new ValidatedBlockCache();
  private ValidatedBlockImporter importer;

  @BeforeEach
  void setUp() {
    importer = new ValidatedBlockImporter(blockImporter, blockValidator, validatedBlockCache);
    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
  }

  @Test
  void importsValidatedBlockWithoutExecutingItAgain() {
    recordValidation();
    stubFastPathPreconditions(HeaderValidationMode.FULL);

    assertThat(importBlock(block, HeaderValidationMode.FULL).getStatus()).isEqualTo(IMPORTED);

    verify(blockchain).appendBlock(block, receipts, Optional.empty());
    verify(badBlockManager).removeBadBlock(block.getHash());
    verify(worldStateArchive).getWorldState(any());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void importsValidatedBlockFromTheNetworkWithItsHeaderValidationMode() {
    recordValidation();
    // block propagation validates the header of a NewBlock, then imports it without validating it
    stubFastPathPreconditions(HeaderValidationMode.NONE);

    assertThat(importer.importBlock(protocolContext, block, HeaderValidationMode.NONE).isImported())
        .isTrue();

    verify(blockValidator)
        .validateBlockForSyncing(
            protocolContext,
            block,
            receipts,
            HeaderValidationMode.NONE,
            HeaderValidationMode.FULL,
            BodyValidationMode.NONE);
    verify(blockchain).appendBlock(block, receipts, Optional.empty());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void executesBlockThatWasNotValidated() {
    stubRegularImport(block, IMPORTED);

    assertThat(importBlock(block, HeaderValidationMode.FULL).isImported()).isTrue();

    verify(blockchain, never()).appendBlock(any(), any(), any());
  }

  @Test
  void doesNotImportBlockAgain() {
    recordValidation();
    when(blockchain.contains(block.getHash())).thenReturn(true);

    assertThat(importBlock(block, HeaderValidationMode.FULL).getStatus())
        .isEqualTo(ALREADY_IMPORTED);

    verifyNoInteractions(blockImporter, blockValidator);
    verify(blockchain, never()).appendBlock(any(), any(), any());
  }

  @Test
  void networkFindsBlockAlreadyImportedByTheRound() {
    recordValidation();
    stubFastPathPreconditions(HeaderValidationMode.FULL);
    assertThat(importBlock(block, HeaderValidationMode.FULL).isImported()).isTrue();
    when(blockchain.contains(block.getHash())).thenReturn(true);

    assertThat(importer.importBlock(protocolContext, block, HeaderValidationMode.NONE).getStatus())
        .isEqualTo(ALREADY_IMPORTED);

    verify(blockchain).appendBlock(block, receipts, Optional.empty());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void executesValidatedBlockWhenParentIsNotChainHead() {
    recordValidation();
    stubFastPathPreconditions(HeaderValidationMode.FULL);
    when(blockchain.getChainHeadHash()).thenReturn(block.getHash());
    stubRegularImport(block, IMPORTED);

    assertThat(importBlock(block, HeaderValidationMode.FULL).isImported()).isTrue();

    verify(blockchain, never()).appendBlock(any(), any(), any());
  }

  @Test
  void executesValidatedBlockWhenTrieLogIsMissing() {
    recordValidation();
    stubFastPathPreconditions(HeaderValidationMode.FULL);
    when(worldStateStorage.getTrieLog(block.getHash())).thenReturn(Optional.empty());
    stubRegularImport(block, IMPORTED);

    assertThat(importBlock(block, HeaderValidationMode.FULL).isImported()).isTrue();

    verify(blockchain, never()).appendBlock(any(), any(), any());
  }

  @Test
  void importsValidatedBlockWhenBodyIsAnEqualCopy() {
    recordValidation();
    stubFastPathPreconditions(HeaderValidationMode.FULL);
    // block decoded again from a peer: new body object, same transactions
    final Block decodedBlock =
        new Block(
            block.getHeader(),
            new BlockBody(
                block.getBody().getTransactions(),
                block.getBody().getOmmers(),
                block.getBody().getWithdrawals()));

    assertThat(importBlock(decodedBlock, HeaderValidationMode.FULL).isImported()).isTrue();

    verify(blockchain).appendBlock(decodedBlock, receipts, Optional.empty());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void executesValidatedBlockWhenBodyDiffers() {
    recordValidation();
    stubFastPathPreconditions(HeaderValidationMode.FULL);
    // same header, but not the transactions that were executed
    final Block otherBody =
        new Block(
            block.getHeader(),
            new BlockBody(List.of(generator.transaction()), block.getBody().getOmmers()));
    stubRegularImport(otherBody, IMPORTED);

    assertThat(importBlock(otherBody, HeaderValidationMode.FULL).isImported()).isTrue();

    verify(blockchain, never()).appendBlock(any(), any(), any());
  }

  @Test
  void rejectsValidatedBlockWithInvalidHeader() {
    recordValidation();
    stubFastPathPreconditions(HeaderValidationMode.FULL);
    when(blockValidator.validateBlockForSyncing(
            protocolContext,
            block,
            receipts,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            BodyValidationMode.NONE))
        .thenReturn(false);

    assertThat(importBlock(block, HeaderValidationMode.FULL).getStatus()).isEqualTo(NOT_IMPORTED);

    verify(blockchain, never()).appendBlock(any(), any(), any());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void dropsOutputsOfImportedHeight() {
    recordValidation();
    stubFastPathPreconditions(HeaderValidationMode.FULL);

    importBlock(block, HeaderValidationMode.FULL);

    assertThat(validatedBlockCache.get(block.getHash())).isEmpty();
  }

  @Test
  void networkImportWaitsForTheValidationOfTheSameBlock() throws InterruptedException {
    // the QBFT round is validating the proposal of this block
    assertThat(validatedBlockCache.tryStartExecution(block.getHash())).isTrue();
    stubFastPathPreconditions(HeaderValidationMode.NONE);
    final AtomicReference<BlockImportResult> result = new AtomicReference<>();
    final Thread networkImport =
        new Thread(
            () ->
                result.set(
                    importer.importBlock(protocolContext, block, HeaderValidationMode.NONE)));

    networkImport.start();
    await().atMost(TIMEOUT).until(() -> networkImport.getState() == Thread.State.TIMED_WAITING);
    verify(blockchain, never()).appendBlock(any(), any(), any());

    recordValidation();
    validatedBlockCache.endExecution(block.getHash());
    networkImport.join(TIMEOUT);

    assertThat(result.get().getStatus()).isEqualTo(IMPORTED);
    verify(blockchain).appendBlock(block, receipts, Optional.empty());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void networkImportExecutesBlockWhenValidationFails() throws InterruptedException {
    assertThat(validatedBlockCache.tryStartExecution(block.getHash())).isTrue();
    when(blockImporter.importBlock(
            protocolContext,
            block,
            HeaderValidationMode.NONE,
            HeaderValidationMode.FULL,
            Optional.empty()))
        .thenReturn(new BlockImportResult(NOT_IMPORTED));
    final AtomicReference<BlockImportResult> result = new AtomicReference<>();
    final Thread networkImport =
        new Thread(
            () ->
                result.set(
                    importer.importBlock(protocolContext, block, HeaderValidationMode.NONE)));

    networkImport.start();
    await().atMost(TIMEOUT).until(() -> networkImport.getState() == Thread.State.TIMED_WAITING);
    // invalid proposal: nothing recorded
    validatedBlockCache.endExecution(block.getHash());
    networkImport.join(TIMEOUT);

    assertThat(result.get().getStatus()).isEqualTo(NOT_IMPORTED);
  }

  @Test
  void executingImportMakesValidationOfTheSameBlockWait() {
    when(blockImporter.importBlock(
            protocolContext,
            block,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            Optional.empty()))
        .thenAnswer(
            invocation -> {
              // validating a proposal of this block now would wait for this import
              assertThat(validatedBlockCache.tryStartExecution(block.getHash())).isFalse();
              return new BlockImportResult(IMPORTED);
            });

    assertThat(importBlock(block, HeaderValidationMode.FULL).isImported()).isTrue();

    assertThat(validatedBlockCache.tryStartExecution(block.getHash())).isTrue();
  }

  @Test
  void validatedBlockImportWaitsForTheBlockImporterLock() throws InterruptedException {
    recordValidation();
    stubFastPathPreconditions(HeaderValidationMode.FULL);
    final AtomicReference<BlockImportResult> result = new AtomicReference<>();
    final Thread importThread =
        new Thread(() -> result.set(importBlock(block, HeaderValidationMode.FULL)));
    // MainnetBlockImporter.importBlock is synchronized: another import is in progress
    final BlockImporter importerLock = blockImporter;

    synchronized (importerLock) {
      importThread.start();
      await()
          .atMost(TIMEOUT)
          .until(
              () ->
                  importThread.getState() == Thread.State.BLOCKED
                      || importThread.getState() == Thread.State.TERMINATED);
      assertThat(importThread.getState()).isEqualTo(Thread.State.BLOCKED);
      verify(blockchain, never()).appendBlock(any(), any(), any());
    }

    importThread.join(TIMEOUT);
    assertThat(result.get().isImported()).isTrue();
    verify(blockchain).appendBlock(block, receipts, Optional.empty());
  }

  @Test
  void delegatesImportForSyncing() {
    when(blockImporter.importBlockForSyncing(
            protocolContext,
            block,
            receipts,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            BodyValidationMode.LIGHT,
            true))
        .thenReturn(new BlockImportResult(IMPORTED));

    assertThat(
            importer
                .importBlockForSyncing(
                    protocolContext,
                    block,
                    receipts,
                    HeaderValidationMode.FULL,
                    HeaderValidationMode.FULL,
                    BodyValidationMode.LIGHT,
                    true)
                .isImported())
        .isTrue();
  }

  private BlockImportResult importBlock(
      final Block blockToImport, final HeaderValidationMode headerValidationMode) {
    return importer.importBlock(
        protocolContext,
        blockToImport,
        headerValidationMode,
        HeaderValidationMode.FULL,
        Optional.empty());
  }

  private void recordValidation() {
    validatedBlockCache.put(
        block.getHash(),
        new ValidatedBlockCache.ValidatedBlock(
            block.getHeader().getNumber(), block.getBody(), receipts, Optional.empty()));
  }

  private void stubFastPathPreconditions(final HeaderValidationMode headerValidationMode) {
    lenient().when(protocolContext.getBadBlockManager()).thenReturn(badBlockManager);
    lenient().when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    lenient().when(blockchain.getChainHeadHash()).thenReturn(parentBlock.getHash());
    lenient().when(worldStateArchive.getWorldStateKeyValueStorage()).thenReturn(worldStateStorage);
    lenient()
        .when(worldStateStorage.getTrieLog(block.getHash()))
        .thenReturn(Optional.of(new byte[0]));
    lenient()
        .when(
            blockValidator.validateBlockForSyncing(
                eq(protocolContext),
                any(),
                eq(receipts),
                eq(headerValidationMode),
                eq(HeaderValidationMode.FULL),
                eq(BodyValidationMode.NONE)))
        .thenReturn(true);
    lenient().when(worldStateArchive.getWorldState(any())).thenReturn(Optional.of(headWorldState));
  }

  private void stubRegularImport(
      final Block blockToImport, final BlockImportResult.BlockImportStatus status) {
    when(blockImporter.importBlock(
            protocolContext,
            blockToImport,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            Optional.empty()))
        .thenReturn(new BlockImportResult(status));
  }
}
