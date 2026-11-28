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

import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult.BlockImportStatus;
import org.hyperledger.besu.ethereum.mainnet.BodyValidationMode;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockImporter;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Block importer of QBFT protocol specs, used by the QBFT round and by the network ({@code
 * NewBlock} announcements, full sync). It imports a block this node created, or validated as a
 * proposal, without executing it again: whichever imports the block first appends it with the saved
 * outputs, and the other finds it already imported. A block whose proposal is being validated is
 * imported when the validation ends, instead of being executed a second time.
 *
 * <p>The outputs are applied to the head world state with the trie log saved by the execution, so
 * they are reused only with a path-based (Bonsai) world state. Otherwise blocks are executed as
 * {@link MainnetBlockImporter} does.
 */
public class ValidatedBlockImporter implements BlockImporter {

  private static final Logger LOG = LoggerFactory.getLogger(ValidatedBlockImporter.class);

  private final BlockImporter blockImporter;
  private final BlockValidator blockValidator;
  private final ValidatedBlockCache validatedBlockCache;

  /**
   * Creates the block importer of a QBFT protocol spec.
   *
   * @param blockValidator the block validator of the protocol spec
   * @param validatedBlockCache outputs of created and validated blocks
   */
  public ValidatedBlockImporter(
      final BlockValidator blockValidator, final ValidatedBlockCache validatedBlockCache) {
    this(new MainnetBlockImporter(blockValidator), blockValidator, validatedBlockCache);
  }

  @VisibleForTesting
  ValidatedBlockImporter(
      final BlockImporter blockImporter,
      final BlockValidator blockValidator,
      final ValidatedBlockCache validatedBlockCache) {
    this.blockImporter = blockImporter;
    this.blockValidator = blockValidator;
    this.validatedBlockCache = validatedBlockCache;
  }

  @Override
  public BlockImportResult importBlock(
      final ProtocolContext context,
      final Block block,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode) {
    return importBlock(context, block, headerValidationMode, ommerValidationMode, Optional.empty());
  }

  @Override
  public BlockImportResult importBlock(
      final ProtocolContext context,
      final Block block,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode,
      final Optional<BlockAccessList> blockAccessList) {
    final Hash blockHash = block.getHash();
    if (isStored(context, blockHash)) {
      return new BlockImportResult(BlockImportStatus.ALREADY_IMPORTED);
    }
    // Both orders of NewBlock and proposal are covered. Marked before waiting for the lock below: a
    // proposal of this block validated meanwhile (NewBlock received first) waits for this import
    // in QbftBlockValidatorAdaptor, instead of executing the block too.
    final boolean importing = validatedBlockCache.tryStartExecution(blockHash);
    try {
      // Proposal received first: the round is validating it, wait for the outputs of the
      // validation, outside the lock so other imports are not blocked meanwhile. Without trie logs
      // (Forest) the outputs can't be used, and waiting would only delay the execution.
      if (!importing && canReuseOutputs(context.getWorldStateArchive())) {
        validatedBlockCache.awaitExecution(blockHash);
      }

      // the lock of MainnetBlockImporter.importBlock, so the head check, append and world state
      // move of a validated block are not interleaved with another import
      synchronized (blockImporter) {
        if (isStored(context, blockHash)) {
          return new BlockImportResult(BlockImportStatus.ALREADY_IMPORTED);
        }
        final Optional<ValidatedBlockCache.ValidatedBlock> validatedBlock =
            findValidatedBlock(context, block);
        final BlockImportResult result =
            validatedBlock.isPresent()
                ? importValidatedBlock(
                    context, block, validatedBlock.get(), headerValidationMode, ommerValidationMode)
                : blockImporter.importBlock(
                    context, block, headerValidationMode, ommerValidationMode, blockAccessList);
        if (result.isImported()) {
          validatedBlockCache.pruneUpTo(block.getHeader().getNumber());
        }
        return result;
      }
    } finally {
      if (importing) {
        validatedBlockCache.endExecution(blockHash);
      }
    }
  }

  @Override
  public BlockImportResult importBlockForSyncing(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode,
      final BodyValidationMode bodyValidationMode,
      final boolean importWithTxIndexing) {
    return blockImporter.importBlockForSyncing(
        context,
        block,
        receipts,
        headerValidationMode,
        ommerValidationMode,
        bodyValidationMode,
        importWithTxIndexing);
  }

  // the trie log of the execution moves the head world state, which must be at the parent block
  private Optional<ValidatedBlockCache.ValidatedBlock> findValidatedBlock(
      final ProtocolContext context, final Block block) {
    final BlockHeader header = block.getHeader();
    return validatedBlockCache
        .get(block.getHash())
        .filter(validatedBlock -> validatedBlock.isExecutedBody(block.getBody()))
        .filter(
            validatedBlock ->
                header.getParentHash().equals(context.getBlockchain().getChainHeadHash()))
        .filter(validatedBlock -> hasTrieLog(context.getWorldStateArchive(), header));
  }

  /**
   * Appends a block already executed by block creation or proposal validation, like {@link
   * MainnetBlockImporter} does after executing it.
   */
  private BlockImportResult importValidatedBlock(
      final ProtocolContext context,
      final Block block,
      final ValidatedBlockCache.ValidatedBlock validatedBlock,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode) {
    // the proposal was validated without commit seals: validate the header as the caller asked
    if (!blockValidator.validateBlockForSyncing(
        context,
        block,
        validatedBlock.receipts(),
        headerValidationMode,
        ommerValidationMode,
        BodyValidationMode.NONE)) {
      return new BlockImportResult(BlockImportStatus.NOT_IMPORTED);
    }
    context
        .getBlockchain()
        .appendBlock(block, validatedBlock.receipts(), validatedBlock.blockAccessList());
    // like MainnetBlockValidator: an imported block is not bad
    context.getBadBlockManager().removeBadBlock(block.getHash());
    // result ignored like MainnetBlockImporter: if head world state can't move, the provider logs
    // why, and next import rolls it forward from the trie logs
    context
        .getWorldStateArchive()
        .getWorldState(withBlockHeaderAndUpdateNodeHead(block.getHeader()));
    LOG.debug("Imported block {} without executing it again", block.toLogString());
    return new BlockImportResult(BlockImportStatus.IMPORTED);
  }

  // A block is imported once its total difficulty is stored, which happens in the same commit as
  // the block. contains() is not enough: DefaultBlockchain.appendBlock caches the header before the
  // commit, so while another import of the same block is committing it, contains() is true but a
  // caller such as EthProtocolManager.blockMined cannot read the total difficulty yet.
  private static boolean isStored(final ProtocolContext context, final Hash blockHash) {
    return context.getBlockchain().getTotalDifficultyByHash(blockHash).isPresent();
  }

  // the outputs are applied with the trie log of the execution, which only path-based storage saves
  private static boolean canReuseOutputs(final WorldStateArchive worldStateArchive) {
    return worldStateArchive instanceof PathBasedWorldStateProvider;
  }

  private static boolean hasTrieLog(
      final WorldStateArchive worldStateArchive, final BlockHeader header) {
    return worldStateArchive instanceof PathBasedWorldStateProvider provider
        && provider.getWorldStateKeyValueStorage().getTrieLog(header.getHash()).isPresent();
  }
}
