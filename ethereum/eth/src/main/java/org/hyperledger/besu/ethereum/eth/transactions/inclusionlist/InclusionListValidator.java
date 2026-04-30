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
package org.hyperledger.besu.ethereum.eth.transactions.inclusionlist;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Strict inclusion list validator per EIP-7805. Applies the 3-step conditional validation
 * algorithm:
 *
 * <ol>
 *   <li>Skip if T is present in the block.
 *   <li>Skip if T.gas &gt; gas_left (transaction cannot fit in remaining gas).
 *   <li>Validate T against post-execution state S: check nonce and balance of T.origin.
 *       <ul>
 *         <li>If T is invalid (wrong nonce or insufficient balance), skip to the next transaction.
 *         <li>If T is valid (could have been included), return INCLUSION_LIST_UNSATISFIED.
 *       </ul>
 * </ol>
 *
 * Returns INCLUSION_LIST_UNSATISFIED only if a transaction could have been included but wasn't.
 */
public class InclusionListValidator {

  private static final Logger LOG = LoggerFactory.getLogger(InclusionListValidator.class);

  public InclusionListValidationResult validate(
      final BlockHeader newBlockHeader,
      final BlockProcessingResult blockProcessingResult,
      final WorldStateArchive worldStateArchive,
      final TransactionValidator transactionValidator,
      final List<Bytes> inclusionListTransactions) {

    final BlockProcessingOutputs blockProcessingOutputs =
        blockProcessingResult
            .getYield()
            .orElseThrow(() -> new IllegalStateException("No block processing outputs present"));

    final long blockGasLeft =
        newBlockHeader.getGasLimit() - blockProcessingOutputs.getCumulativeBlockGasUsed();
    final WorldStateQueryParams worldStateQueryParams =
        WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(newBlockHeader);
    try (final WorldState worldState =
        worldStateArchive
            .getWorldState(worldStateQueryParams)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Unable to load world state for block header "
                            + newBlockHeader.toLogString()))) {

      LOG.atInfo()
          .setMessage("Strict IL validation: {} IL txs, {} payload txs, gasLeft={}")
          .addArgument(inclusionListTransactions.size())
          .addArgument(blockProcessingResult.getReceipts().size())
          .addArgument(blockGasLeft)
          .log();

      // Apply the 3-step conditional algorithm for each IL transaction
      for (int i = 0; i < inclusionListTransactions.size(); i++) {
        final Bytes ilTxBytes = inclusionListTransactions.get(i);
        final Transaction tx;
        try {
          tx = TransactionDecoder.decodeOpaqueBytes(ilTxBytes, EncodingContext.BLOCK_BODY);
          LOG.atInfo()
              .setMessage("Checking IL tx not in block {}")
              .addArgument(tx::toTraceLog)
              .log();

          // Skip blob txs they are not allowed
          if (tx.getType().supportsBlob()) {
            continue;
          }

          // Step 2: Skip if T.gas > gas_left (block has no room for this transaction)
          if (tx.getGasLimit() > blockGasLeft) {
            LOG.atInfo()
                .setMessage("IL step 2: skipping tx at index {} — gasLimit={} > gasLeft={}")
                .addArgument(i)
                .addArgument(tx.getGasLimit())
                .addArgument(blockGasLeft)
                .log();
            continue;
          }

          final ValidationResult<TransactionInvalidReason> baseValidationResult =
              transactionValidator.validate(
                  tx,
                  newBlockHeader.getBaseFee(),
                  Optional.of(Wei.ZERO),
                  TransactionValidationParams.processingBlock());
          if (!baseValidationResult.isValid()) {
            LOG.info("Tx {} not valid reason {}", tx.toTraceLog(), baseValidationResult);
            continue;
          }
          final Address sender = tx.getSender();
          final ValidationResult<TransactionInvalidReason> senderValidationResult =
              transactionValidator.validateForSender(
                  tx, worldState.get(sender), TransactionValidationParams.processingBlock());
          if (!senderValidationResult.isValid()) {
            LOG.info("Tx {} not valid reason {}", tx.toTraceLog(), senderValidationResult);
            continue;
          }

          // Transaction passed all 3 checks — it could have been included but wasn't
          LOG.warn(
              "IL unsatisfied: transaction at index {} (sender={}, nonce={}) could have been included but wasn't",
              i,
              sender,
              tx.getNonce());
          return InclusionListValidationResult.unsatisfied(
              "Inclusion list not satisfied: transaction at index "
                  + i
                  + " (sender="
                  + sender
                  + ", nonce="
                  + tx.getNonce()
                  + ") could have been included but wasn't");

        } catch (final Exception e) {
          // invalid txs are ignored
          LOG.info("IL contains undecodable transaction at index {}", i);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception closing world state", e);
    }

    return InclusionListValidationResult.valid();
  }
}
