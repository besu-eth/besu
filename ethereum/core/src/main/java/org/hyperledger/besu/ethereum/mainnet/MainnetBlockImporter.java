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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult.BlockImportStatus;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.WorldStateQueryParams;

import java.util.List;
import java.util.Optional;

public class MainnetBlockImporter implements BlockImporter {

  final BlockValidator blockValidator;

  public MainnetBlockImporter(final BlockValidator blockValidator) {
    this.blockValidator = blockValidator;
  }

  @Override
  public synchronized BlockImportResult importBlock(
      final ProtocolContext context,
      final Block block,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode) {
    return importBlock(context, block, headerValidationMode, ommerValidationMode, Optional.empty());
  }

  @Override
  public synchronized BlockImportResult importBlock(
      final ProtocolContext context,
      final Block block,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode,
      final Optional<BlockAccessList> blockAccessList) {
    if (context.getBlockchain().contains(block.getHash())) {
      return new BlockImportResult(BlockImportStatus.ALREADY_IMPORTED);
    }

    final var result =
        blockValidator.validateAndProcessBlock(
            context, block, headerValidationMode, ommerValidationMode, blockAccessList, false);

    if (result.isSuccessful()) {
      result
          .getYield()
          .ifPresent(
              processingOutputs -> {
                context
                    .getBlockchain()
                    .appendBlock(
                        block,
                        processingOutputs.getReceipts(),
                        processingOutputs.getBlockAccessList());

                // move the head worldstate if block processing was successful:
                context
                    .getWorldStateArchive()
                    .getWorldState(
                        WorldStateQueryParams.newBuilder()
                            .withBlockHeader(block.getHeader())
                            .withShouldWorldStateUpdateHead(true)
                            .build());
              });
    }

    return new BlockImportResult(result.isSuccessful());
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

    if (blockValidator.validateBlockForSyncing(
        context, block, receipts, headerValidationMode, ommerValidationMode, bodyValidationMode)) {
      if (importWithTxIndexing) {
        context.getBlockchain().appendBlock(block, receipts);
      } else {
        context.getBlockchain().appendBlockWithoutIndexingTransactions(block, receipts);
      }
      return new BlockImportResult(true);
    }

    return new BlockImportResult(false);
  }
}
