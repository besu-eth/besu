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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.util.List;

/**
 * Intrinsic gas (EIP-8037) for a transaction, split into regular and state dimensions.
 *
 * <p>Computed identically by block-building (BlockSizeTransactionSelector) and block-import
 * (AbstractBlockProcessor) so both paths feed the same per-dimension budget check.
 */
public record TransactionIntrinsicGas(long regularGas, long stateGas) {

  public static TransactionIntrinsicGas of(
      final Transaction transaction, final GasCalculator gasCalculator) {
    final List<AccessListEntry> accessListEntries = transaction.getAccessList().orElse(List.of());
    int accessListStorageCount = 0;
    for (final var entry : accessListEntries) {
      accessListStorageCount += entry.storageKeys().size();
    }
    final long accessListGas =
        gasCalculator.accessListGasCost(accessListEntries.size(), accessListStorageCount);
    final long codeDelegationGas =
        gasCalculator.delegateCodeGasCost(transaction.codeDelegationListSize());
    final long regularGas =
        gasCalculator.transactionIntrinsicGasCost(
            transaction, clampedAdd(accessListGas, codeDelegationGas));
    final long stateGas =
        gasCalculator
            .stateGasCostCalculator()
            .transactionIntrinsicStateGas(
                transaction.isContractCreation(), transaction.codeDelegationListSize());
    return new TransactionIntrinsicGas(regularGas, stateGas);
  }
}
