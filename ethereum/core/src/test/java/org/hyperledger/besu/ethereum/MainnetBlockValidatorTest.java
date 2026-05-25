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
package org.hyperledger.besu.ethereum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BlockAccessListValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockBodyValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BodyValidationMode;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

public class MainnetBlockValidatorTest {

  private final BlockchainSetupUtil chainUtil = BlockchainSetupUtil.forMainnet();
  private final Block block = chainUtil.getBlock(3);
  private final Block blockParent = chainUtil.getBlock(2);

  private final MutableBlockchain blockchain = spy(chainUtil.getBlockchain());
  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
  private final MutableWorldState worldState = mock(MutableWorldState.class);
  private final BadBlockManager badBlockManager =
      chainUtil.getProtocolContext().getBadBlockManager();
  private final BlockProcessor blockProcessor = mock(BlockProcessor.class);
  private final BlockHeaderValidator blockHeaderValidator = mock(BlockHeaderValidator.class);
  private final BlockBodyValidator blockBodyValidator = mock(BlockBodyValidator.class);
  private final BlockAccessListValidator blockAccessListValidator =
      mock(BlockAccessListValidator.class);

  private final BlockValidator mainnetFrontierBlockValidator =
      MainnetBlockValidatorBuilder.frontier(
          blockHeaderValidator, blockBodyValidator, blockProcessor, blockAccessListValidator);

  public static Stream<Arguments> getStorageExceptions() {
    return Stream.of(
        Arguments.of("StorageException", new StorageException("Database closed")),
        Arguments.of("MerkleTrieException", new MerkleTrieException("Missing trie node")));
  }

  public static Stream<Arguments> getBlockProcessingErrors() {
    return Stream.of(
        Arguments.of("StorageException", new StorageException("Database closed")),
        Arguments.of("MerkleTrieException", new MerkleTrieException("Missing trie node")),
        Arguments.of("RuntimeException", new RuntimeException("Oops")));
  }

  @BeforeEach
  public void setup() {
    chainUtil.importFirstBlocks(4);
    final BlockProcessingResult successfulProcessingResult =
        new BlockProcessingResult(Optional.empty(), false);

    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolContext.getBadBlockManager()).thenReturn(badBlockManager);
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(worldStateArchive.getWorldState(any())).thenReturn(Optional.of(worldState));
    when(worldStateArchive.getWorldState(any())).thenReturn(Optional.of(worldState));
    when(worldStateArchive.getWorldState()).thenReturn(worldState);
    when(blockHeaderValidator.validateHeader(any(), any(), any())).thenReturn(true);
    when(blockHeaderValidator.validateHeader(any(), any(), any(), any())).thenReturn(true);
    when(blockBodyValidator.validateBody(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(true);
    when(blockBodyValidator.validateBodyLight(any(), any(), any(), any(), any())).thenReturn(true);
    when(blockAccessListValidator.validate(any(), any(), anyInt())).thenReturn(true);
    when(blockProcessor.processBlock(
            eq(protocolContext), any(), any(), any(), eq(Optional.empty())))
        .thenReturn(successfulProcessingResult);
    when(blockProcessor.processBlock(
            eq(protocolContext), any(), any(), any(), eq(Optional.empty())))
        .thenReturn(successfulProcessingResult);

    assertNoBadBlocks();
  }

  @Test
  public void validateAndProcessBlock_onStateRootMismatch() {
    var spyBlock = chainUtil.getBlock(4);
    BlockHeader badStateRootHeader =
        BlockHeaderBuilder.fromHeader(spyBlock.getHeader())
            .stateRoot(Hash.EMPTY_TRIE_HASH)
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildBlockHeader();

    Block stateRootMismatchBlock = new Block(badStateRootHeader, spyBlock.getBody());

    var spec = chainUtil.getProtocolSchedule().getByBlockHeader(badStateRootHeader);

    BlockProcessingResult result =
        spec.getBlockValidator()
            .validateAndProcessBlock(
                chainUtil.getProtocolContext(),
                stateRootMismatchBlock,
                HeaderValidationMode.NONE,
                HeaderValidationMode.NONE);

    assertThat(result.isSuccessful()).isFalse();
    assertThat(badBlockManager.getBadBlock(stateRootMismatchBlock.getHash())).isPresent();
    assertThat(badBlockManager.getBadBlocks()).containsExactly(stateRootMismatchBlock);
  }

  @Test
  public void validateAndProcessBlock_onSuccess() {
    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    assertThat(result.isSuccessful()).isTrue();
    assertNoBadBlocks();
  }

  @Test
  public void validateAndProcessBlock_whenBalValidationFails() {
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    Address.fromHexString("0x1000000000000000000000000000000000000001"),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of())));
    when(blockAccessListValidator.validate(eq(Optional.of(bal)), any(), anyInt()))
        .thenReturn(false);

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY,
            Optional.of(bal),
            true);

    assertValidationFailed(result, "Block access list validation failed");
    assertBadBlockIsTracked(block);
  }

  @Test
  public void validateAndProcessBlock_whenBalPresentAndValidationPasses() {
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    Address.fromHexString("0x1000000000000000000000000000000000000001"),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of())));
    final Optional<BlockAccessList> optionalBal = Optional.of(bal);
    when(blockAccessListValidator.validate(eq(optionalBal), any(), anyInt())).thenReturn(true);
    when(blockProcessor.processBlock(eq(protocolContext), any(), any(), any(), eq(optionalBal)))
        .thenReturn(new BlockProcessingResult(Optional.empty(), false));

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY,
            optionalBal,
            true);

    assertThat(result.isSuccessful()).isTrue();
    assertNoBadBlocks();
  }

  @Test
  public void validateAndProcessBlock_whenParentBlockNotPresent() {
    final Hash parentHash = blockParent.getHash();
    doReturn(Optional.empty()).when(blockchain).getBlockHeader(eq(parentHash));

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    final String expectedError = "Parent block with hash " + parentHash + " not present";
    assertValidationFailed(result, expectedError);
    assertNoBadBlocks();
  }

  @Test
  public void validateAndProcessBlock_whenHeaderInvalid() {
    when(blockHeaderValidator.validateHeader(
            any(BlockHeader.class),
            any(BlockHeader.class),
            eq(protocolContext),
            eq(HeaderValidationMode.DETACHED_ONLY)))
        .thenReturn(false);

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    final String expectedError = "Header validation failed (DETACHED_ONLY)";
    assertValidationFailed(result, expectedError);
    assertBadBlockIsTracked(block);
  }

  @Test
  public void validateAndProcessBlock_whenBlockBodyInvalid() {
    when(blockBodyValidator.validateBody(any(), eq(block), any(), any(), any(), any(), any()))
        .thenReturn(false);

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    final String expectedError = "failed to validate output of imported block";
    assertValidationFailed(result, expectedError);
    assertBadBlockIsTracked(block);
  }

  @Test
  public void validateAndProcessBlock_whenParentWorldStateNotAvailable() {
    final ArgumentCaptor<WorldStateQueryParams> captor =
        ArgumentCaptor.forClass(WorldStateQueryParams.class);
    when(worldStateArchive.getWorldState(captor.capture()))
        .thenAnswer(
            invocation -> {
              WorldStateQueryParams capturedParams = captor.getValue();
              if (capturedParams.getBlockHeader().equals(blockParent.getHeader())) {
                return Optional.empty();
              }
              return null;
            });

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    final String expectedError =
        "Unable to process block because parent world state "
            + blockParent.getHeader().getStateRoot()
            + " is not available";
    assertValidationFailed(result, expectedError);
    assertNoBadBlocks();
  }

  @Test
  public void validateAndProcessBlock_whenProcessBlockFails() {
    when(blockProcessor.processBlock(
            eq(protocolContext),
            eq(blockchain),
            any(MutableWorldState.class),
            eq(block),
            eq(Optional.empty())))
        .thenReturn(BlockProcessingResult.FAILED);

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    final String expectedError = "processing failed";
    assertValidationFailed(result, expectedError);
    assertBadBlockIsTracked(block);
  }

  @Test
  public void validateAndProcessBlock_whenStorageExceptionThrownGettingParent() {
    final Throwable storageException = new StorageException("Database closed");
    final Hash parentHash = blockParent.getHash();
    doThrow(storageException).when(blockchain).getBlockHeader(eq(parentHash));

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    assertValidationFailedExceptionally(result, storageException);
    assertNoBadBlocks();
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("getStorageExceptions")
  public void validateAndProcessBlock_whenStorageExceptionThrownProcessingBlock(
      final String caseName, final Exception storageException) {
    doThrow(storageException)
        .when(blockProcessor)
        .processBlock(
            eq(protocolContext),
            eq(blockchain),
            any(MutableWorldState.class),
            eq(block),
            eq(Optional.empty()));

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    assertValidationFailedExceptionally(result, storageException);
    assertNoBadBlocks();
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("getStorageExceptions")
  public void validateAndProcessBlock_whenStorageExceptionThrownGettingWorldState(
      final String caseName, final Exception storageException) {
    final BlockHeader parentHeader = blockParent.getHeader();
    doThrow(storageException)
        .when(worldStateArchive)
        .getWorldState(eq(withBlockHeaderAndUpdateNodeHead(parentHeader)));

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    assertValidationFailedExceptionally(result, storageException);
    assertNoBadBlocks();
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("getBlockProcessingErrors")
  public void validateAndProcessBlock_whenProcessBlockYieldsExceptionalResult(
      final String caseName, final Exception cause) {
    final BlockProcessingResult exceptionalResult =
        new BlockProcessingResult(Optional.empty(), cause);
    when(blockProcessor.processBlock(
            eq(protocolContext),
            eq(blockchain),
            any(MutableWorldState.class),
            eq(block),
            eq(Optional.empty())))
        .thenReturn(exceptionalResult);

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    assertValidationFailedExceptionally(result, cause);
    assertNoBadBlocks();
  }

  @Test
  public void validateAndProcessBlock_withShouldRecordBadBlockFalse() {
    when(blockProcessor.processBlock(
            eq(protocolContext),
            eq(blockchain),
            any(MutableWorldState.class),
            eq(block),
            eq(Optional.empty())))
        .thenReturn(BlockProcessingResult.FAILED);

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY,
            Optional.empty(),
            false,
            false);

    assertThat(result.isFailed()).isTrue();
    assertNoBadBlocks();
  }

  @Test
  public void validateAndProcessBlock_withShouldRecordBadBlockTrue() {
    when(blockProcessor.processBlock(
            eq(protocolContext),
            eq(blockchain),
            any(MutableWorldState.class),
            eq(block),
            eq(Optional.empty())))
        .thenReturn(BlockProcessingResult.FAILED);

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY,
            Optional.empty(),
            false,
            true);

    assertThat(result.isFailed()).isTrue();
    assertBadBlockIsTracked(block);
  }

  @Test
  public void validateAndProcessBlock_withShouldRecordBadBlockNotSet() {
    when(blockProcessor.processBlock(
            eq(protocolContext),
            eq(blockchain),
            any(MutableWorldState.class),
            eq(block),
            eq(Optional.empty())))
        .thenReturn(BlockProcessingResult.FAILED);

    BlockProcessingResult result =
        mainnetFrontierBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY,
            Optional.empty(),
            false);

    assertThat(result.isFailed()).isTrue();
    assertBadBlockIsTracked(block);
  }

  @Test
  public void validateBlockForSyncing_onSuccess() {
    final boolean isValid =
        mainnetFrontierBlockValidator.validateBlockForSyncing(
            protocolContext,
            block,
            Collections.emptyList(),
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            BodyValidationMode.LIGHT);

    assertThat(isValid).isTrue();
    assertNoBadBlocks();
  }

  @Test
  public void validateBlockValidation_onFailedHeaderForSyncing() {
    final HeaderValidationMode headerValidationMode = HeaderValidationMode.FULL;
    when(blockHeaderValidator.validateHeader(
            any(BlockHeader.class), eq(protocolContext), eq(headerValidationMode)))
        .thenReturn(false);

    final boolean isValid =
        mainnetFrontierBlockValidator.validateBlockForSyncing(
            protocolContext,
            block,
            Collections.emptyList(),
            headerValidationMode,
            headerValidationMode,
            BodyValidationMode.LIGHT);

    assertThat(isValid).isFalse();
    assertBadBlockIsTracked(block);
  }

  @Test
  public void validateBlockValidation_onFailedBodyForSyncing() {
    final HeaderValidationMode headerValidationMode = HeaderValidationMode.FULL;
    when(blockBodyValidator.validateBodyLight(
            eq(protocolContext), eq(block), any(), eq(headerValidationMode), any()))
        .thenReturn(false);

    final boolean isValid =
        mainnetFrontierBlockValidator.validateBlockForSyncing(
            protocolContext,
            block,
            Collections.emptyList(),
            headerValidationMode,
            headerValidationMode,
            BodyValidationMode.LIGHT);

    assertThat(isValid).isFalse();
    assertBadBlockIsTracked(block);
  }

  @Test
  public void shouldThrowIfValidateForSyncingWithFullBodyValidation() {
    final HeaderValidationMode headerValidationMode = HeaderValidationMode.FULL;
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            mainnetFrontierBlockValidator.validateBlockForSyncing(
                protocolContext,
                block,
                Collections.emptyList(),
                headerValidationMode,
                headerValidationMode,
                BodyValidationMode.FULL));
  }

  private void assertNoBadBlocks() {
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
  }

  private void assertBadBlockIsTracked(final Block badBlock) {
    assertThat(badBlockManager.getBadBlocks()).containsExactly(badBlock);
    assertThat(badBlockManager.getBadBlock(badBlock.getHash())).contains(block);
  }

  private void assertValidationFailed(
      final BlockProcessingResult result, final String expectedError) {
    assertThat(result.isFailed()).isTrue();
    assertThat(result.errorMessage).isPresent();
    assertThat(result.errorMessage.get()).containsIgnoringWhitespaces(expectedError);
  }

  private void assertValidationFailedExceptionally(
      final BlockProcessingResult result, final Throwable exception) {
    assertThat(result.isFailed()).isTrue();
    assertThat(result.causedBy()).containsSame(exception);
    assertThat(result.errorMessage).isPresent();
    assertThat(result.errorMessage.get())
        .containsIgnoringWhitespaces(exception.getLocalizedMessage());
  }

  @Test
  void validationFailsForBlockGreaterThanMaxBlockSize() {
    BlockDataGenerator generator = new BlockDataGenerator();
    BadBlockManager badBlockManager = new BadBlockManager();
    when(protocolContext.getBadBlockManager()).thenReturn(badBlockManager);
    final BlockValidator blockValidator =
        MainnetBlockValidatorBuilder.osaka(
            blockHeaderValidator, blockBodyValidator, blockProcessor, blockAccessListValidator);
    int maxRlpBlockSize = blockValidator.maxRlpBlockSize();

    final Transaction transaction = generator.transaction(Bytes.random(maxRlpBlockSize + 1));
    BlockDataGenerator.BlockOptions blockOptions =
        new BlockDataGenerator.BlockOptions().setBlockNumber(1).addTransaction(transaction);
    Block block = generator.block(blockOptions);

    BlockProcessingResult result =
        blockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(badBlockManager.getBadBlock(block.getHash())).isPresent();
    assertThat(badBlockManager.getBadBlocks()).containsExactly(block);
  }

  @Test
  void validationSuccessfulForBlockLessThanMaxBlockSize() {
    var blockchainSetupUtil = BlockchainSetupUtil.forTesting(DataStorageFormat.BONSAI);
    blockchainSetupUtil.importAllBlocks();
    final BadBlockManager badBlockManager =
        blockchainSetupUtil.getProtocolContext().getBadBlockManager();
    final MutableBlockchain blockchain = spy(blockchainSetupUtil.getBlockchain());
    final BlockProcessingResult successfulProcessingResult =
        new BlockProcessingResult(Optional.empty(), false);
    when(protocolContext.getBadBlockManager()).thenReturn(badBlockManager);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolContext.getWorldStateArchive()).thenReturn(blockchainSetupUtil.getWorldArchive());
    when(blockHeaderValidator.validateHeader(any(), any(), any())).thenReturn(true);
    when(blockHeaderValidator.validateHeader(any(), any(), any(), any())).thenReturn(true);
    when(blockProcessor.processBlock(
            eq(protocolContext), any(), any(), any(), eq(Optional.empty())))
        .thenReturn(successfulProcessingResult);
    when(blockBodyValidator.validateBody(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(true);

    final Block block = blockchainSetupUtil.getBlock(2);
    final BlockValidator blockSizeBlockValidator =
        MainnetBlockValidatorBuilder.osaka(
            blockHeaderValidator, blockBodyValidator, blockProcessor, blockAccessListValidator);

    BlockProcessingResult result =
        blockSizeBlockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.DETACHED_ONLY,
            HeaderValidationMode.DETACHED_ONLY);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
  }
}
