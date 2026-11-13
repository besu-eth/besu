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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreationTiming;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionSelectionResults;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult.BlockImportStatus;
import org.hyperledger.besu.ethereum.mainnet.BodyValidationMode;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Real adaptors and importer sharing one cache: the block is executed only once, whatever the
 * round, and whether the QBFT round or the network imports it.
 */
@ExtendWith(MockitoExtension.class)
class ValidatedBlockCacheFastPathTest {
  @Mock private BlockCreator besuBlockCreator;
  @Mock private TransactionSelectionResults transactionSelectionResults;
  @Mock private BlockValidator blockValidator;
  @Mock private BlockImporter blockImporter;
  @Mock private ProtocolContext protocolContext;
  @Mock private MutableBlockchain blockchain;
  @Mock private BadBlockManager badBlockManager;
  @Mock private PathBasedWorldStateProvider worldStateArchive;
  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;

  private final QbftExtraDataCodec codec = new QbftExtraDataCodec();
  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final ValidatedBlockCache validatedBlockCache = new ValidatedBlockCache();
  private final BlockHeader parentHeader = new BlockHeaderTestFixture().number(1).buildHeader();
  private final BlockBody body =
      new BlockBody(List.of(generator.transaction(), generator.transaction()), emptyList());
  private final List<SECPSignature> commitSeals =
      List.of(new SECPSignature(BigInteger.ONE, BigInteger.ONE, (byte) 1));
  // the importer of the protocol spec, used by the QBFT round and the network
  private ValidatedBlockImporter specImporter;

  @BeforeEach
  void setUp() {
    specImporter = new ValidatedBlockImporter(blockImporter, blockValidator, validatedBlockCache);
    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2})
  void proposerExecutesItsBlockOnlyWhenCreatingIt(final int round) {
    final Block createdBlock = proposal(round, body);
    final List<TransactionReceipt> receipts = generator.receipts(createdBlock);
    when(transactionSelectionResults.getReceipts()).thenReturn(receipts);
    when(besuBlockCreator.createBlock(10L, parentHeader))
        .thenReturn(
            new BlockCreator.BlockCreationResult(
                createdBlock,
                transactionSelectionResults,
                new BlockCreationTiming(),
                Optional.empty()));
    final QbftBlockCreatorAdaptor creator = creator();

    final QbftBlock proposal =
        creator.createBlock(10L, new QbftBlockHeaderAdaptor(parentHeader)).block();

    // validating its own proposal does not execute the block again
    assertThat(validator().validateBlock(proposal, Optional.empty()).success()).isTrue();
    verifyNoInteractions(blockValidator);

    // the sealed block keeps the body object of the proposal
    final QbftBlock sealed = creator.createSealedBlock(proposal, round, commitSeals);
    final Block sealedBlock = AdaptorUtil.toBesuBlock(sealed);
    assertThat(sealedBlock.getBody()).isSameAs(createdBlock.getBody());

    // importing the committed block uses the receipts from block creation
    stubFastPathPreconditions(sealedBlock);
    assertThat(importer().importBlock(sealed, Optional.empty())).isTrue();
    verify(blockchain).appendBlock(sealedBlock, receipts, Optional.empty());
    verify(badBlockManager).removeBadBlock(sealedBlock.getHash());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void validatorExecutesReproposedBlockOnlyOnce() {
    final Block proposedBlock = proposal(0, body);
    final List<TransactionReceipt> receipts = generator.receipts(proposedBlock);
    when(blockValidator.validateAndProcessBlock(
            eq(protocolContext),
            any(),
            eq(HeaderValidationMode.LIGHT),
            eq(HeaderValidationMode.FULL),
            eq(Optional.empty()),
            eq(InclusionListTransactions.EMPTY),
            eq(false)))
        .thenReturn(
            new BlockProcessingResult(
                Optional.of(new BlockProcessingOutputs(mock(MutableWorldState.class), receipts))));
    assertThat(
            validator()
                .validateBlock(new QbftBlockAdaptor(proposedBlock), Optional.empty())
                .success())
        .isTrue();

    // round change: the prepared block is proposed again in round 2, decoded from a peer message
    final QbftBlock reproposal =
        new QbftBlockAdaptor(
            proposal(
                2, new BlockBody(body.getTransactions(), body.getOmmers(), body.getWithdrawals())));
    assertThat(validator().validateBlock(reproposal, Optional.empty()).success()).isTrue();

    final QbftBlock sealed = creator().createSealedBlock(reproposal, 2, commitSeals);
    final Block sealedBlock = AdaptorUtil.toBesuBlock(sealed);
    stubFastPathPreconditions(sealedBlock);
    assertThat(importer().importBlock(sealed, Optional.empty())).isTrue();

    // processed once, with the committed block hash, so import finds the trie log it saved
    verify(blockValidator)
        .validateAndProcessBlock(
            eq(protocolContext),
            argThat(processed -> processed.getHash().equals(sealedBlock.getHash())),
            any(),
            any(),
            any(),
            eq(InclusionListTransactions.EMPTY),
            eq(false));
    verify(blockValidator)
        .validateBlockForSyncing(
            protocolContext,
            sealedBlock,
            receipts,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            BodyValidationMode.NONE);
    verifyNoMoreInteractions(blockValidator);
    verify(blockchain).appendBlock(sealedBlock, receipts, Optional.empty());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void newBlockReceivedWhileValidatingIsImportedWithoutExecutingIt() throws Exception {
    final Block proposedBlock = proposal(0, body);
    final List<TransactionReceipt> receipts = generator.receipts(proposedBlock);
    final CountDownLatch executing = new CountDownLatch(1);
    final CountDownLatch executionDone = new CountDownLatch(1);
    stubSlowValidation(receipts, executing, executionDone);
    final QbftBlock sealed =
        creator().createSealedBlock(new QbftBlockAdaptor(proposedBlock), 0, commitSeals);
    final Block sealedBlock = AdaptorUtil.toBesuBlock(sealed);
    final Block announcedBlock = decodedCopy(sealedBlock);
    stubFastPathPreconditions(sealedBlock, HeaderValidationMode.NONE);

    // the QBFT round validates the proposal, slowly
    final CompletableFuture<Boolean> validation =
        CompletableFuture.supplyAsync(
                () ->
                    validator()
                        .validateBlock(new QbftBlockAdaptor(proposedBlock), Optional.empty()))
            .thenApply(QbftBlockValidator.ValidationResult::success);
    assertThat(executing.await(10, TimeUnit.SECONDS)).isTrue();

    // meanwhile, block propagation imports the NewBlock announcement of the committed block
    final AtomicReference<BlockImportResult> networkImport = new AtomicReference<>();
    final Thread networkThread =
        new Thread(
            () ->
                networkImport.set(
                    specImporter.importBlock(
                        protocolContext, announcedBlock, HeaderValidationMode.NONE)));
    networkThread.start();
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> networkThread.getState() == Thread.State.TIMED_WAITING);
    verify(blockchain, never()).appendBlock(any(), any(), any());

    executionDone.countDown();
    assertThat(validation.get(10, TimeUnit.SECONDS)).isTrue();
    networkThread.join(Duration.ofSeconds(10));

    // the network appends the block with the outputs of the validation
    assertThat(networkImport.get().getStatus()).isEqualTo(BlockImportStatus.IMPORTED);
    verify(blockchain).appendBlock(announcedBlock, receipts, Optional.empty());

    // then the round commits, and finds the block imported
    when(blockchain.contains(sealedBlock.getHash())).thenReturn(true);
    assertThat(importer().importBlock(sealed, Optional.empty())).isTrue();

    verify(blockValidator)
        .validateAndProcessBlock(any(), any(), any(), any(), any(), any(), anyBoolean());
    verify(blockchain).appendBlock(any(), any(), any());
    verifyNoInteractions(blockImporter);
  }

  @Test
  void proposalReceivedWhileTheNetworkImportsTheBlockIsNotExecutedAgain() throws Exception {
    // a validator one block behind: the NewBlock is imported before the proposal is validated
    final Block proposedBlock = proposal(0, body);
    final QbftBlock sealed =
        creator().createSealedBlock(new QbftBlockAdaptor(proposedBlock), 0, commitSeals);
    final Block announcedBlock = decodedCopy(AdaptorUtil.toBesuBlock(sealed));
    final CountDownLatch executing = new CountDownLatch(1);
    final CountDownLatch executionDone = new CountDownLatch(1);
    final AtomicBoolean imported = new AtomicBoolean();
    when(blockchain.getBlockBody(announcedBlock.getHash()))
        .thenAnswer(
            invocation ->
                imported.get() ? Optional.of(announcedBlock.getBody()) : Optional.empty());
    when(blockImporter.importBlock(
            protocolContext,
            announcedBlock,
            HeaderValidationMode.NONE,
            HeaderValidationMode.FULL,
            Optional.empty()))
        .thenAnswer(
            invocation -> {
              executing.countDown();
              assertThat(executionDone.await(10, TimeUnit.SECONDS)).isTrue();
              imported.set(true);
              return new BlockImportResult(BlockImportStatus.IMPORTED);
            });

    final CompletableFuture<BlockImportResult> networkImport =
        CompletableFuture.supplyAsync(
            () ->
                specImporter.importBlock(
                    protocolContext, announcedBlock, HeaderValidationMode.NONE));
    assertThat(executing.await(10, TimeUnit.SECONDS)).isTrue();

    // the QBFT round validates the proposal of the block being imported
    final AtomicReference<QbftBlockValidator.ValidationResult> validation = new AtomicReference<>();
    final Thread qbftThread =
        new Thread(
            () ->
                validation.set(
                    validator()
                        .validateBlock(new QbftBlockAdaptor(proposedBlock), Optional.empty())));
    qbftThread.start();
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> qbftThread.getState() == Thread.State.TIMED_WAITING);

    executionDone.countDown();
    assertThat(networkImport.get(10, TimeUnit.SECONDS).isImported()).isTrue();
    qbftThread.join(Duration.ofSeconds(10));

    assertThat(validation.get().success()).isTrue();
    verifyNoInteractions(blockValidator);
  }

  // a proposal of the given round, hashed like BftBlockCreator and QbftBlockCodecAdaptor do
  private Block proposal(final int round, final BlockBody blockBody) {
    final BftExtraData extraData =
        new BftExtraData(
            Bytes.wrap(new byte[32]), emptyList(), Optional.empty(), round, List.of(Address.ZERO));
    return new Block(
        new BlockHeaderTestFixture()
            .number(2)
            .parentHash(parentHeader.getHash())
            .extraData(codec.encode(extraData))
            .blockHeaderFunctions(BftBlockHeaderFunctions.forCommittedSeal(codec))
            .buildHeader(),
        blockBody);
  }

  private QbftBlockCreatorAdaptor creator() {
    return new QbftBlockCreatorAdaptor(besuBlockCreator, codec, validatedBlockCache);
  }

  private QbftBlockValidatorAdaptor validator() {
    return new QbftBlockValidatorAdaptor(
        blockValidator, protocolContext, codec, validatedBlockCache);
  }

  private QbftBlockImporterAdaptor importer() {
    return new QbftBlockImporterAdaptor(specImporter, protocolContext);
  }

  // a block received from a peer: decoded again, so new body object with the same content
  private static Block decodedCopy(final Block block) {
    return new Block(
        block.getHeader(),
        new BlockBody(
            block.getBody().getTransactions(),
            block.getBody().getOmmers(),
            block.getBody().getWithdrawals()));
  }

  // block execution that takes a while: signals when it starts and waits to be allowed to finish
  private void stubSlowValidation(
      final List<TransactionReceipt> receipts,
      final CountDownLatch executing,
      final CountDownLatch executionDone) {
    when(blockValidator.validateAndProcessBlock(
            eq(protocolContext),
            any(),
            eq(HeaderValidationMode.LIGHT),
            eq(HeaderValidationMode.FULL),
            eq(Optional.empty()),
            eq(InclusionListTransactions.EMPTY),
            eq(false)))
        .thenAnswer(
            invocation -> {
              executing.countDown();
              assertThat(executionDone.await(10, TimeUnit.SECONDS)).isTrue();
              return new BlockProcessingResult(
                  Optional.of(new BlockProcessingOutputs(mock(MutableWorldState.class), receipts)));
            });
  }

  // the trie log was saved under the on-chain hash, at block creation or proposal validation
  private void stubFastPathPreconditions(final Block sealedBlock) {
    stubFastPathPreconditions(sealedBlock, HeaderValidationMode.FULL);
  }

  // the network imports a NewBlock without validating its header again
  private void stubFastPathPreconditions(
      final Block sealedBlock, final HeaderValidationMode headerValidationMode) {
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(protocolContext.getBadBlockManager()).thenReturn(badBlockManager);
    when(blockchain.getChainHeadHash()).thenReturn(parentHeader.getHash());
    when(worldStateArchive.getWorldStateKeyValueStorage()).thenReturn(worldStateStorage);
    when(worldStateStorage.getTrieLog(sealedBlock.getHash())).thenReturn(Optional.of(new byte[0]));
    when(blockValidator.validateBlockForSyncing(
            eq(protocolContext),
            any(),
            any(),
            eq(headerValidationMode),
            eq(HeaderValidationMode.FULL),
            eq(BodyValidationMode.NONE)))
        .thenReturn(true);
  }
}
