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

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(allParameters = true)
public interface TransactionValidationParams {

  TransactionValidationParams processingBlockParams =
      ImmutableTransactionValidationParams.of(
          false, false, false, true, false, false, false, false);

  TransactionValidationParams transactionPoolParams =
      ImmutableTransactionValidationParams.of(true, false, true, true, true, false, false, false);

  TransactionValidationParams miningParams =
      ImmutableTransactionValidationParams.of(false, false, false, true, true, false, false, false);

  TransactionValidationParams blockReplayParams =
      ImmutableTransactionValidationParams.of(
          false, false, false, false, false, false, false, false);

  TransactionValidationParams transactionSimulatorParams =
      ImmutableTransactionValidationParams.of(false, false, false, false, false, true, true, false);

  TransactionValidationParams transactionSimulatorParamsAllowFutureNonce =
      ImmutableTransactionValidationParams.of(true, false, false, false, false, true, true, false);

  TransactionValidationParams transactionSimulatorAllowUnderpricedAndFutureNonceParams =
      ImmutableTransactionValidationParams.of(true, false, true, false, false, true, true, false);

  TransactionValidationParams transactionSimulatorAllowExceedingBalanceParams =
      ImmutableTransactionValidationParams.of(false, true, false, false, false, true, true, false);

  TransactionValidationParams transactionSimulatorAllowExceedingBalanceAndFutureNonceParams =
      ImmutableTransactionValidationParams.of(true, true, false, false, false, true, true, false);

  TransactionValidationParams blockSimulatorStrictParams =
      ImmutableTransactionValidationParams.of(
          false, false, false, false, false, true, false, false);

  // eth_simulateV1 non-strict: allows exceeding balance and future nonces, and preserves
  // caller-provided gas pricing so that gas fees are actually charged during simulation.
  TransactionValidationParams blockSimulatorNonStrictParams =
      ImmutableTransactionValidationParams.of(true, true, false, false, false, true, true, true);

  @Value.Default
  default boolean isAllowFutureNonce() {
    return false;
  }

  @Value.Default
  default boolean isAllowExceedingBalance() {
    return false;
  }

  @Value.Default
  default boolean allowUnderpriced() {
    return false;
  }

  @Value.Default
  default boolean checkOnchainPermissions() {
    return false;
  }

  @Value.Default
  default boolean checkLocalPermissions() {
    return true;
  }

  @Value.Default
  default boolean isAllowContractAddressAsSender() {
    return false;
  }

  @Value.Default
  default boolean isAllowExceedingGasLimit() {
    return false;
  }

  /**
   * When true, caller-provided gas pricing is preserved during transaction simulation instead of
   * being zeroed out. This is used by eth_simulateV1 so that gas fees are actually charged from the
   * sender's balance, producing the correct stateRoot and block hash. eth_call leaves this false so
   * that gas pricing is zeroed (callers don't need sufficient balance for gas).
   */
  @Value.Default
  default boolean isPreserveCallerGasPricing() {
    return false;
  }

  static TransactionValidationParams transactionSimulator() {
    return transactionSimulatorParams;
  }

  static TransactionValidationParams transactionSimulatorAllowFutureNonce() {
    return transactionSimulatorParamsAllowFutureNonce;
  }

  static TransactionValidationParams transactionSimulatorAllowUnderpricedAndFutureNonce() {
    return transactionSimulatorAllowUnderpricedAndFutureNonceParams;
  }

  static TransactionValidationParams transactionSimulatorAllowExceedingBalance() {
    return transactionSimulatorAllowExceedingBalanceParams;
  }

  static TransactionValidationParams transactionSimulatorAllowExceedingBalanceAndFutureNonce() {
    return transactionSimulatorAllowExceedingBalanceAndFutureNonceParams;
  }

  static TransactionValidationParams blockSimulatorStrict() {
    return blockSimulatorStrictParams;
  }

  static TransactionValidationParams blockSimulatorNonStrict() {
    return blockSimulatorNonStrictParams;
  }

  static TransactionValidationParams processingBlock() {
    return processingBlockParams;
  }

  static TransactionValidationParams transactionPool() {
    return transactionPoolParams;
  }

  static TransactionValidationParams mining() {
    return miningParams;
  }

  static TransactionValidationParams blockReplay() {
    return blockReplayParams;
  }
}
