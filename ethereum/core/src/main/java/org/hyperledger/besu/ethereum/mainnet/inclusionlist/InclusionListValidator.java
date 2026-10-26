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
import org.hyperledger.besu.datatypes.Hash;
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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

  /**
   * Returns the inclusion list transactions that are not included in the block. A transaction is
   * included only if a block transaction has its exact bytes, so they are matched by hash: {@link
   * Transaction#equals} ignores some fields, like the access list and the authorization list.
   *
   * @param inclusionListTransactions the inclusion list transactions
   * @param blockTransactions the transactions of the block
   * @return the inclusion list transactions that are not in the block
   */
  public static Collection<Transaction> notIncludedIn(
      final Collection<Transaction> inclusionListTransactions,
      final List<Transaction> blockTransactions) {
    if (inclusionListTransactions.isEmpty()) {
      return List.of();
    }
    final Set<Hash> blockTransactionHashes =
        blockTransactions.stream().map(Transaction::getHash).collect(Collectors.toSet());
    return inclusionListTransactions.stream()
        .filter(ilTx -> !blockTransactionHashes.contains(ilTx.getHash()))
        .toList();
  }

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
              Optional.of(Wei.ZERO),
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
      LOG.debug(
          "Block {} does not satisfy its {}", newBlockHeader.toLogString(), unsatisfiedReason);
      return InclusionListValidationResult.unsatisfied(unsatisfiedReason);
    }

    return InclusionListValidationResult.valid();
  }
}
