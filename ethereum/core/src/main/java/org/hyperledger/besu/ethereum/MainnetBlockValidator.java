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

import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockAccessListValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockBodyValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BodyValidationMode;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MainnetBlockValidator class implements the BlockValidator interface for the Mainnet Ethereum
 * network. It validates and processes blocks according to the rules of the Mainnet Ethereum
 * network.
 */
public class MainnetBlockValidator implements BlockValidator {

  private static final Logger LOG = LoggerFactory.getLogger(MainnetBlockValidator.class);

  /** The BlockHeaderValidator used to validate block headers. */
  protected final BlockHeaderValidator blockHeaderValidator;

  /** The BlockBodyValidator used to validate block bodies. */
  protected final BlockBodyValidator blockBodyValidator;

  /** The BlockProcessor used to process blocks. */
  protected final BlockProcessor blockProcessor;

  /** The BlockAccessListValidator used to validate block access lists. */
  protected final BlockAccessListValidator blockAccessListValidator;

  /** The maximum size of a block in RLP encoding. */
  private final int maxRlpBlockSize;

  /**
   * Constructs a new MainnetBlockValidator with the given BlockHeaderValidator, BlockBodyValidator,
   * BlockProcessor, BlockAccessListValidator, and maximum RLP block size.
   *
   * @param blockHeaderValidator the BlockHeaderValidator used to validate block headers
   * @param blockBodyValidator the BlockBodyValidator used to validate block bodies
   * @param blockProcessor the BlockProcessor used to process blocks
   * @param blockAccessListValidator the BlockAccessListValidator used to validate block access
   *     lists
   * @param maxRlpBlockSize the maximum size of a block in RLP encoding
   */
  protected MainnetBlockValidator(
      final BlockHeaderValidator blockHeaderValidator,
      final BlockBodyValidator blockBodyValidator,
      final BlockProcessor blockProcessor,
      final BlockAccessListValidator blockAccessListValidator,
      final int maxRlpBlockSize) {
    this.blockHeaderValidator = blockHeaderValidator;
    this.blockBodyValidator = blockBodyValidator;
    this.blockProcessor = blockProcessor;
    this.blockAccessListValidator = blockAccessListValidator;
    this.maxRlpBlockSize = maxRlpBlockSize;
  }

  /**
   * Performs a full validation and processing of a block
   *
   * @param context the {@link ProtocolContext}
   * @param block the block being validated and processed
   * @param headerValidationMode the {@link HeaderValidationMode} used for validating the header
   * @param ommerValidationMode the {@link HeaderValidationMode} used for validating the ommers
   * @return an optional containing the {@link BlockProcessingOutputs} with the output of processing
   *     the block, empty if the block was deemed invalid or couldn't be processed
   */
  @Override
  public BlockProcessingResult validateAndProcessBlock(
      final ProtocolContext context,
      final Block block,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode) {
    return validateAndProcessBlock(
        context, block, headerValidationMode, ommerValidationMode, Optional.empty(), true, true);
  }

  @Override
  public BlockProcessingResult validateAndProcessBlock(
      final ProtocolContext context,
      final Block block,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode,
      final Optional<BlockAccessList> blockAccessList,
      final boolean shouldPersist) {
    return validateAndProcessBlock(
        context,
        block,
        headerValidationMode,
        ommerValidationMode,
        blockAccessList,
        shouldPersist,
        true);
  }

  @Override
  public BlockProcessingResult validateAndProcessBlock(
      final ProtocolContext context,
      final Block block,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode,
      final Optional<BlockAccessList> blockAccessList,
      final boolean shouldUpdateHead,
      final boolean shouldRecordBadBlock) {

    final int blockSize = block.getSize();
    if (blockSize > maxRlpBlockSize) {
      final String errorMessage =
          "Block size of " + blockSize + " bytes exceeds limit of " + maxRlpBlockSize + " bytes";
      var retval = new BlockProcessingResult(errorMessage);
      handleFailedBlockProcessing(block, blockAccessList, retval, true, context);
      return retval;
    }

    final BlockHeader header = block.getHeader();
    final BlockHeader parentHeader;

    try {
      final MutableBlockchain blockchain = context.getBlockchain();
      final Optional<BlockHeader> maybeParentHeader =
          blockchain.getBlockHeader(header.getParentHash());
      if (maybeParentHeader.isEmpty()) {
        var retval =
            new BlockProcessingResult(
                "Parent block with hash " + header.getParentHash() + " not present");
        // Blocks should not be marked bad due to missing data
        handleFailedBlockProcessing(block, blockAccessList, retval, false, context);
        return retval;
      }
      parentHeader = maybeParentHeader.get();

      if (!blockHeaderValidator.validateHeader(
          header, parentHeader, context, headerValidationMode)) {
        final String error = String.format("Header validation failed (%s)", headerValidationMode);
        var retval = new BlockProcessingResult(error);
        handleFailedBlockProcessing(block, blockAccessList, retval, shouldRecordBadBlock, context);
        return retval;
      }
    } catch (StorageException ex) {
      var retval = new BlockProcessingResult(Optional.empty(), ex);
      // Blocks should not be marked bad due to a local storage failure
      handleFailedBlockProcessing(block, blockAccessList, retval, false, context);
      return retval;
    }

    final WorldStateQueryParams worldStateQueryParams =
        WorldStateQueryParams.newBuilder()
            .withBlockHeader(parentHeader)
            .withShouldWorldStateUpdateHead(shouldUpdateHead)
            .build();
    try (final var worldState =
        context.getWorldStateArchive().getWorldState(worldStateQueryParams).orElse(null)) {

      if (worldState == null) {
        var retval =
            BlockProcessingResult.worldStateUnavailable(
                "Unable to process block because parent world state "
                    + parentHeader.getStateRoot()
                    + " is not available");
        // Blocks should not be marked bad due to missing data
        handleFailedBlockProcessing(block, blockAccessList, retval, false, context);
        return retval;
      }

      if (!blockAccessListValidator.validate(
          blockAccessList, block.getHeader(), block.getBody().getTransactions().size())) {
        var result =
            new BlockProcessingResult(
                String.format(
                    "Block access list validation failed for block %s",
                    block.getHeader().getBlockHash()));
        handleFailedBlockProcessing(block, blockAccessList, result, shouldRecordBadBlock, context);
        return result;
      }

      var result = processBlock(context, worldState, block, blockAccessList);
      if (result.isFailed()) {
        handleFailedBlockProcessing(block, blockAccessList, result, shouldRecordBadBlock, context);
        return result;
      } else {
        List<TransactionReceipt> receipts =
            result.getYield().map(BlockProcessingOutputs::getReceipts).orElse(new ArrayList<>());
        Optional<List<Request>> maybeRequests =
            result.getYield().flatMap(BlockProcessingOutputs::getRequests);
        Optional<BlockAccessList> processedBlockAccessList =
            result.getYield().flatMap(BlockProcessingOutputs::getBlockAccessList);
        long cumulativeBlockGasUsed =
            result.getYield().map(BlockProcessingOutputs::getCumulativeBlockGasUsed).orElse(0L);
        if (!blockBodyValidator.validateBody(
            context,
            block,
            receipts,
            worldState.rootHash(),
            ommerValidationMode,
            BodyValidationMode.FULL,
            OptionalLong.of(cumulativeBlockGasUsed))) {
          result = new BlockProcessingResult("failed to validate output of imported block");
          handleFailedBlockProcessing(
              block, blockAccessList, result, shouldRecordBadBlock, context);
          return result;
        }

        return new BlockProcessingResult(
            Optional.of(
                new BlockProcessingOutputs(
                    worldState,
                    receipts,
                    maybeRequests,
                    processedBlockAccessList,
                    cumulativeBlockGasUsed)),
            result.getNbParallelizedTransactions());
      }
    } catch (MerkleTrieException ex) {
      context.getWorldStateArchive().heal(ex.getMaybeAddress(), ex.getLocation());
      return new BlockProcessingResult(Optional.empty(), ex);
    } catch (StorageException ex) {
      var retval = new BlockProcessingResult(Optional.empty(), ex);
      // Do not record bad block due to a local storage issue
      handleFailedBlockProcessing(block, blockAccessList, retval, false, context);
      return retval;
    } catch (Exception ex) {
      // Wrap checked autocloseable exception from try-with-resources
      throw new RuntimeException(ex);
    }
  }

  /**
   * Handles the processing of a block that has failed validation or processing.
   *
   * @param failedBlock the block that failed processing
   * @param blockAccessList optional block access list
   * @param result the result of the block processing
   * @param shouldRecordBadBlock whether to record the block as a bad block
   * @param context the ProtocolContext
   */
  protected void handleFailedBlockProcessing(
      final Block failedBlock,
      final Optional<BlockAccessList> blockAccessList,
      final BlockValidationResult result,
      final boolean shouldRecordBadBlock,
      final ProtocolContext context) {
    if (result.causedBy().isPresent()) {
      // Block processing failed exceptionally, we cannot assume the block was intrinsically invalid
      LOG.info(
          "Failed to process block {}: {}, caused by {}",
          failedBlock.toLogString(),
          result.errorMessage,
          result.causedBy().get());
      LOG.debug("with stack", result.causedBy().get());
    } else {
      if (result.errorMessage.isPresent()) {
        LOG.info("Invalid block {}: {}", failedBlock.toLogString(), result.errorMessage);
      } else {
        LOG.info("Invalid block {}", failedBlock.toLogString());
      }

      if (shouldRecordBadBlock) {
        // Result.errorMessage should not be empty on failure, but add a default to be safe
        String description = result.errorMessage.orElse("Unknown cause");
        final BadBlockCause cause = BadBlockCause.fromValidationFailure(description);
        final Optional<BlockAccessList> generatedBlockAccessList =
            result instanceof BlockProcessingResult
                ? ((BlockProcessingResult) result).getGeneratedBlockAccessList()
                : Optional.empty();
        context
            .getBadBlockManager()
            .addBadBlock(failedBlock, cause, blockAccessList, generatedBlockAccessList);
      } else {
        LOG.debug("Invalid block {} not added to badBlockManager ", failedBlock.toLogString());
      }
    }
  }

  /**
   * Processes a block, returning the result of the processing
   *
   * @param context the ProtocolContext
   * @param worldState the world state for the parent block state root hash
   * @param block the block to be processed
   * @param blockAccessList optional block access list
   * @return the result of processing the block
   */
  protected BlockProcessingResult processBlock(
      final ProtocolContext context,
      final MutableWorldState worldState,
      final Block block,
      final Optional<BlockAccessList> blockAccessList) {

    return blockProcessor.processBlock(
        context, context.getBlockchain(), worldState, block, blockAccessList);
  }

  @Override
  public boolean validateBlockForSyncing(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode,
      final BodyValidationMode bodyValidationMode) {

    if (bodyValidationMode == BodyValidationMode.FULL) {
      throw new UnsupportedOperationException(
          "Full body validation is not supported for syncing blocks");
    }

    final BlockHeader header = block.getHeader();
    if (!blockHeaderValidator.validateHeader(header, context, headerValidationMode)) {
      String description = String.format("Failed header validation (%s)", headerValidationMode);
      context
          .getBadBlockManager()
          .addBadBlock(block, BadBlockCause.fromValidationFailure(description));
      return false;
    }

    if (bodyValidationMode == BodyValidationMode.NONE) {
      return true;
    }

    if (!blockBodyValidator.validateBodyLight(
        context, block, receipts, ommerValidationMode, OptionalLong.empty())) {
      context
          .getBadBlockManager()
          .addBadBlock(
              block, BadBlockCause.fromValidationFailure("Failed body validation (light)"));
      return false;
    }
    return true;
  }

  @Override
  public int maxRlpBlockSize() {
    return maxRlpBlockSize;
  }
}
