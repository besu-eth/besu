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
package org.hyperledger.besu.evm.gascalculator;

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.internal.Words.clampedMultiply;

/**
 * Strategy interface for EIP-8037 state-creation gas cost calculations.
 *
 * <p>EIP-8037 introduces multidimensional gas metering, splitting gas into regular gas and state
 * gas. State-creation operations (CREATE, SSTORE 0→nonzero, CALL to new accounts, code deposits,
 * EIP-7702 delegations) have their costs split into a regular gas portion and a state gas portion,
 * where state gas depends on a fixed {@code cost_per_state_byte} (cpsb) for the active fork.
 *
 * <p>This interface is intentionally a pure cost calculator: it returns gas amounts only. Charging
 * (and refunding) is the responsibility of the operation or transaction processor that issues the
 * cost — see e.g. {@code SStoreOperation}, {@code AbstractCreateOperation}, and {@code
 * MainnetTransactionProcessor}.
 */
public interface StateGasCostCalculator {

  /**
   * Returns the cost per state byte for the active fork.
   *
   * @return the cost per state byte
   */
  long costPerStateByte();

  /**
   * Returns the state gas for creating a new contract account (112 * cpsb). Charged for the
   * CREATE/CREATE2 opcodes and for the intrinsic charge of a contract-creation transaction.
   *
   * @return the state gas for a new contract
   */
  long newContractStateGas();

  /**
   * Returns the state gas for code deposit (cpsb * codeSize).
   *
   * @param codeSize the size of the code in bytes
   * @return the state gas for code deposit
   */
  long codeDepositStateGas(int codeSize);

  /**
   * Returns the regular gas for code deposit hashing (6 * ceil(codeSize/32)).
   *
   * @param codeSize the size of the code in bytes
   * @return the regular gas for code deposit hashing
   */
  long codeDepositHashGas(int codeSize);

  /**
   * Returns the state gas for creating a new account (112 * cpsb).
   *
   * @return the state gas for new account creation
   */
  long newAccountStateGas();

  /**
   * Returns the state gas for storage set 0→nonzero (32 * cpsb).
   *
   * @return the state gas for storage set
   */
  long storageSetStateGas();

  /**
   * Returns the regular gas for storage set (replacing the 20000 SSTORE_SET_GAS).
   *
   * @return the regular gas for storage set
   */
  long storageSetRegularGas();

  /**
   * Returns the state gas for EIP-7702 auth base (23 * cpsb).
   *
   * @return the state gas for auth base
   */
  long authBaseStateGas();

  /**
   * Returns the regular gas for EIP-7702 auth base.
   *
   * @return the regular gas for auth base
   */
  long authBaseRegularGas();

  /**
   * Returns the state gas for empty account delegation (112 * cpsb).
   *
   * @return the state gas for empty account delegation
   */
  long emptyAccountDelegationStateGas();

  /**
   * Returns the maximum regular gas allowed per transaction (TX_MAX_GAS_LIMIT from EIP-7825).
   * EIP-8037 changes this from a validation condition to a runtime revert condition on regular gas
   * only. Returns {@code Long.MAX_VALUE} when state gas metering is not active.
   *
   * @return the maximum regular gas per transaction
   */
  long transactionRegularGasLimit();

  /**
   * Returns whether multidimensional gas metering (EIP-8037) is active.
   *
   * @return true when state gas metering is active
   */
  default boolean isActive() {
    return false;
  }

  /**
   * Computes the intrinsic state gas for a transaction. This is the worst-case state gas charged
   * upfront (assuming all delegation targets are new accounts). Existing-account refunds are
   * applied later during processing.
   *
   * @param isContractCreation whether the transaction creates a contract
   * @param codeDelegationCount number of EIP-7702 code delegations
   * @return the intrinsic state gas
   */
  default long transactionIntrinsicStateGas(
      final boolean isContractCreation, final long codeDelegationCount) {
    long stateGas = isContractCreation ? newContractStateGas() : 0L;
    if (codeDelegationCount > 0) {
      // Worst case: all delegators are new accounts → (112 + 23) * cpsb each
      final long perDelegation = clampedAdd(emptyAccountDelegationStateGas(), authBaseStateGas());
      stateGas = clampedAdd(stateGas, clampedMultiply(perDelegation, codeDelegationCount));
    }
    return stateGas;
  }

  /** A no-op implementation that returns 0 for all state gas costs. */
  StateGasCostCalculator NONE =
      new StateGasCostCalculator() {
        @Override
        public long costPerStateByte() {
          return 0L;
        }

        @Override
        public long newContractStateGas() {
          return 0L;
        }

        @Override
        public long codeDepositStateGas(final int codeSize) {
          return 0L;
        }

        @Override
        public long codeDepositHashGas(final int codeSize) {
          return 0L;
        }

        @Override
        public long newAccountStateGas() {
          return 0L;
        }

        @Override
        public long storageSetStateGas() {
          return 0L;
        }

        @Override
        public long storageSetRegularGas() {
          return 0L;
        }

        @Override
        public long authBaseStateGas() {
          return 0L;
        }

        @Override
        public long authBaseRegularGas() {
          return 0L;
        }

        @Override
        public long emptyAccountDelegationStateGas() {
          return 0L;
        }

        @Override
        public long transactionRegularGasLimit() {
          return Long.MAX_VALUE;
        }
      };
}
