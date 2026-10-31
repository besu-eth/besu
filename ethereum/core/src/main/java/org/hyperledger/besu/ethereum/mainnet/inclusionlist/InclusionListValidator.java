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
package org.hyperledger.besu.ethereum.mainnet.inclusionlist;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BlockGasAccountingStrategy;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Collection;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Strict inclusion list validator per EIP-7805. Returns INCLUSION_LIST_UNSATISFIED only if a
 * transaction could have been included but wasn't.
 *
 * <p>A transaction that does not fit in the block is skipped. With the two-dimensional block gas
 * accounting of EIP-8037 there is no single gas_left to compare the transaction gas limit to, so
 * this uses the same worst case check that block processing applies before executing a transaction,
 * see {@link BlockGasAccountingStrategy#hasBlockCapacity}.
 *
 * <p>As specified by EIP-7805, a transaction that fits is validated against the post-execution
 * state by checking the nonce and balance of its sender, without executing it. Executing it would
 * not find more invalid transactions, since transaction processing runs the same validations, and
 * would make every block import pay for the execution of the inclusion list transactions, and
 * record their state and ancestor block accesses as if the block had made them.
 */
public class InclusionListValidator {

  private static final Logger LOG = LoggerFactory.getLogger(InclusionListValidator.class);
  // blob txs are skipped, so the blob fee does not matter
  private static final Optional<Wei> NO_BLOB_FEE = Optional.of(Wei.ZERO);

  /**
   * Validates the inclusion list transactions that are not in the block against the state after the
   * block transactions (EIP-7805).
   *
   * @param protocolSpec the protocol spec of the block
   * @param worldUpdater the world state after the block transactions, before withdrawals
   * @param newBlockHeader the header of the block
   * @param cumulativeExecutionGasUsed the execution gas used by the block transactions
   * @param cumulativeStateGasUsed the state gas used by the block transactions (EIP-8037)
   * @param inclusionListTransactions the inclusion list transactions that are not in the block
   * @return unsatisfied if one of the transactions could have been included, valid otherwise
   */
  public static InclusionListValidationResult validate(
      final ProtocolSpec protocolSpec,
      final WorldUpdater worldUpdater,
      final BlockHeader newBlockHeader,
      final long cumulativeExecutionGasUsed,
      final long cumulativeStateGasUsed,
      final Collection<Transaction> inclusionListTransactions) {

    if (inclusionListTransactions.isEmpty()) {
      return InclusionListValidationResult.valid();
    }

    LOG.atDebug()
        .setMessage("Validating {} inclusion list txs not in block {}")
        .addArgument(inclusionListTransactions::size)
        .addArgument(newBlockHeader::toLogString)
        .log();

    final TransactionValidator transactionValidator =
        protocolSpec.getTransactionValidatorFactory().get();
    final BlockGasAccountingStrategy blockGasAccountingStrategy =
        protocolSpec.getBlockGasAccountingStrategy();
    final long transactionExecutionGasLimit =
        protocolSpec.getGasCalculator().stateGasCostCalculator().transactionExecutionGasLimit();

    for (final Transaction tx : inclusionListTransactions) {
      // blob txs are not allowed in inclusion lists
      if (tx.getType().supportsBlob()) {
        LOG.atDebug().setMessage("Skipping IL blob tx {}").addArgument(tx::toTraceLog).log();
        continue;
      }

      // the block has no room left for this tx
      if (!blockGasAccountingStrategy.hasBlockCapacity(
          tx.getGasLimit(),
          transactionExecutionGasLimit,
          cumulativeExecutionGasUsed,
          cumulativeStateGasUsed,
          newBlockHeader.getGasLimit())) {
        LOG.atDebug()
            .setMessage(
                "Skipping IL tx {}, the block has no room left for it (execution={}, state={}, limit={})")
            .addArgument(tx::toTraceLog)
            .addArgument(cumulativeExecutionGasUsed)
            .addArgument(cumulativeStateGasUsed)
            .addArgument(newBlockHeader::getGasLimit)
            .log();
        continue;
      }

      final ValidationResult<TransactionInvalidReason> baseValidationResult =
          transactionValidator.validate(
              tx,
              newBlockHeader.getBaseFee(),
              NO_BLOB_FEE,
              TransactionValidationParams.processingBlock());
      if (!baseValidationResult.isValid()) {
        LOG.atDebug()
            .setMessage("Skipping IL tx {}, it is not valid: {}")
            .addArgument(tx::toTraceLog)
            .addArgument(baseValidationResult::getErrorMessage)
            .log();
        continue;
      }
      final Address sender = tx.getSender();
      final ValidationResult<TransactionInvalidReason> senderValidationResult =
          transactionValidator.validateForSender(
              tx, worldUpdater.get(sender), TransactionValidationParams.processingBlock());
      if (!senderValidationResult.isValid()) {
        LOG.atDebug()
            .setMessage("Skipping IL tx {}, it is not valid for its sender: {}")
            .addArgument(tx::toTraceLog)
            .addArgument(senderValidationResult::getErrorMessage)
            .log();
        continue;
      }

      // the tx passed all the checks: it could have been included, but it was not
      final String unsatisfiedReason =
          String.format(
              "inclusion list tx %s (sender %s, nonce %d) could have been included but was not",
              tx.getHash(), sender, tx.getNonce());
      LOG.atDebug()
          .setMessage("Block {} does not satisfy its {}")
          .addArgument(newBlockHeader::toLogString)
          .addArgument(unsatisfiedReason)
          .log();
      return InclusionListValidationResult.unsatisfied(unsatisfiedReason);
    }

    return InclusionListValidationResult.valid();
  }
}
