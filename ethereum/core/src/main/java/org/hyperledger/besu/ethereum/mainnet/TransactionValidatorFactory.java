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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.ChainId;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.PermissionTransactionFilter;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.plugin.services.txvalidator.TransactionValidationRule;

import java.util.List;
import java.util.Set;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class TransactionValidatorFactory {

  private volatile Supplier<TransactionValidator> transactionValidatorSupplier;
  private static final Set<BlobType> BLOBS_PROHIBITED = Set.of();

  public TransactionValidatorFactory(
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final boolean checkSignatureMalleability,
      final ChainId chainId) {
    this(
        gasCalculator,
        gasLimitCalculator,
        checkSignatureMalleability,
        chainId,
        Set.of(TransactionType.FRONTIER));
  }

  public TransactionValidatorFactory(
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final boolean checkSignatureMalleability,
      final ChainId chainId,
      final Set<TransactionType> acceptedTransactionTypes) {
    this(
        gasCalculator,
        gasLimitCalculator,
        FeeMarket.legacy(),
        checkSignatureMalleability,
        chainId,
        acceptedTransactionTypes,
        Integer.MAX_VALUE);
  }

  public TransactionValidatorFactory(
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final FeeMarket feeMarket,
      final boolean checkSignatureMalleability,
      final ChainId chainId,
      final Set<TransactionType> acceptedTransactionTypes,
      final int maxInitcodeSize) {
    this(
        gasCalculator,
        gasLimitCalculator,
        feeMarket,
        checkSignatureMalleability,
        chainId,
        acceptedTransactionTypes,
        BLOBS_PROHIBITED,
        maxInitcodeSize);
  }

  public TransactionValidatorFactory(
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final FeeMarket feeMarket,
      final boolean checkSignatureMalleability,
      final ChainId chainId,
      final Set<TransactionType> acceptedTransactionTypes,
      final Set<BlobType> acceptedBlobVersions,
      final int maxInitcodeSize) {

    this.transactionValidatorSupplier =
        Suppliers.memoize(
            () ->
                new MainnetTransactionValidator(
                    gasCalculator,
                    gasLimitCalculator,
                    feeMarket,
                    checkSignatureMalleability,
                    chainId,
                    acceptedTransactionTypes,
                    acceptedBlobVersions,
                    maxInitcodeSize));
  }

  public void setPermissionTransactionFilter(
      final PermissionTransactionFilter permissionTransactionFilter) {
    final TransactionValidator baseTxValidator = transactionValidatorSupplier.get();
    transactionValidatorSupplier =
        Suppliers.memoize(
            () -> new PermissionTransactionValidator(baseTxValidator, permissionTransactionFilter));
  }

  public void setAdditionalValidationRules(
      final List<TransactionValidationRule> additionalValidationRules) {
    if (!additionalValidationRules.isEmpty()) {
      final TransactionValidator baseTxValidator = transactionValidatorSupplier.get();
      transactionValidatorSupplier =
          Suppliers.memoize(
              () -> new ExtendableTransactionValidator(baseTxValidator, additionalValidationRules));
    }
  }

  public TransactionValidator get() {
    return transactionValidatorSupplier.get();
  }
}
