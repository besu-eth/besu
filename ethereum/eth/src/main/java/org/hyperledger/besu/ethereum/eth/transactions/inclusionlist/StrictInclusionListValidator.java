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
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
public class StrictInclusionListValidator implements InclusionListValidator {

  private static final Logger LOG = LoggerFactory.getLogger(StrictInclusionListValidator.class);

  @Override
  public InclusionListValidationResult validate(
      final List<Bytes> payloadTransactions,
      final List<Bytes> inclusionListTransactions,
      final InclusionListValidationContext context) {

    LOG.atDebug()
        .setMessage("Strict IL validation: {} IL txs, {} payload txs, gasLeft={}")
        .addArgument(inclusionListTransactions.size())
        .addArgument(payloadTransactions.size())
        .addArgument(context.gasLeft())
        .log();

    // Step 1: Build a set of payload transactions for O(1) presence lookup
    final Set<Bytes> payloadTxSet = new HashSet<>(payloadTransactions);

    // Apply the 3-step conditional algorithm for each IL transaction
    for (int i = 0; i < inclusionListTransactions.size(); i++) {
      final Bytes ilTxBytes = inclusionListTransactions.get(i);

      // Step 1: Skip if T is present in the block
      if (payloadTxSet.contains(ilTxBytes)) {
        continue;
      }

      // Transaction is not in the block — attempt to decode it
      final Transaction tx;
      try {
        tx = TransactionDecoder.decodeOpaqueBytes(ilTxBytes, EncodingContext.BLOCK_BODY);
      } catch (final Exception e) {
        LOG.warn("IL contains undecodable transaction at index {}: {}", i, e.getMessage());
        return InclusionListValidationResult.invalid(
            "Inclusion list contains undecodable transaction at index " + i);
      }

      // Step 2: Skip if T.gas > gas_left (block has no room for this transaction)
      if (tx.getGasLimit() > context.gasLeft()) {
        LOG.atDebug()
            .setMessage("IL step 2: skipping tx at index {} — gasLimit={} > gasLeft={}")
            .addArgument(i)
            .addArgument(tx.getGasLimit())
            .addArgument(context.gasLeft())
            .log();
        continue;
      }

      // Step 3: Validate T against post-execution state S (nonce and balance of T.origin)
      final Address sender = tx.getSender();
      final long expectedNonce = context.accountNonces().getOrDefault(sender, 0L);
      final Wei senderBalance = context.accountBalances().getOrDefault(sender, Wei.ZERO);
      final Wei upfrontCost = tx.getUpfrontCost(0);

      // Check nonce: tx nonce must match the post-execution nonce of the sender
      if (tx.getNonce() != expectedNonce) {
        LOG.atDebug()
            .setMessage(
                "IL step 3: skipping tx at index {} — nonce mismatch (tx.nonce={}, expected={})")
            .addArgument(i)
            .addArgument(tx.getNonce())
            .addArgument(expectedNonce)
            .log();
        continue;
      }

      // Check balance: sender must have enough to cover the upfront cost
      if (senderBalance.compareTo(upfrontCost) < 0) {
        LOG.atDebug()
            .setMessage(
                "IL step 3: skipping tx at index {} — insufficient balance (balance={}, upfrontCost={})")
            .addArgument(i)
            .addArgument(senderBalance)
            .addArgument(upfrontCost)
            .log();
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
    }

    return InclusionListValidationResult.valid();
  }
}
