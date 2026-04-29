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

import org.hyperledger.besu.evm.frame.MessageFrame;

/**
 * Strategy interface for EIP-8037 state gas cost calculations under the frame-end accounting model
 * proposed in <a href="https://github.com/ethereum/EIPs/pull/11573">EIP PR 11573</a>.
 *
 * <p>State gas is no longer charged at individual opcodes; opcodes record state-change events on
 * the frame, and this calculator aggregates them at frame-end via {@link
 * #applyFrameEndStateGasAccounting(MessageFrame)}. Reverted/halted frames produce no debits or
 * credits — their recorded events are dropped via the frame's undo machinery.
 *
 * <p>The default (NONE) implementation is a no-op; the EIP-8037 implementation performs the actual
 * accounting.
 */
public interface StateGasCostCalculator {

  /**
   * Returns the cost per state byte for the given block gas limit.
   *
   * @return the cost per state byte
   */
  long costPerStateByte();

  /**
   * Returns the state gas for a CREATE operation (112 * cpsb).
   *
   * @return the state gas for CREATE
   */
  long createStateGas();

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
   * Returns the state gas for storage set 0->nonzero (32 * cpsb).
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
   * Charges state gas for EIP-7702 code delegation intrinsic costs. This is the only intrinsic
   * state-gas charge that remains inline at tx setup time.
   *
   * @param frame the message frame
   * @param totalDelegations total number of code delegations
   * @param alreadyExistingDelegators number of delegators that already existed
   * @return true if gas was successfully charged, false if insufficient gas
   */
  default boolean chargeCodeDelegationStateGas(
      final MessageFrame frame, final long totalDelegations, final long alreadyExistingDelegators) {
    return true;
  }

  /**
   * Aggregates the state-change events recorded since this frame's construction and applies the
   * resulting net charge to the frame's state-gas accounting. Invoked at frame success
   * (post-execution, pre-commit). Returns {@code false} on out-of-gas, in which case the caller
   * must transition the frame to {@link MessageFrame.State#EXCEPTIONAL_HALT}.
   *
   * <p>Reverted/halted frames must not call this method; their recorded events are dropped via
   * {@link MessageFrame#rollback()} so they contribute no debits or credits.
   *
   * @param frame the frame whose events are to be aggregated
   * @return true on success, false on insufficient gas
   */
  default boolean applyFrameEndStateGasAccounting(final MessageFrame frame) {
    return true;
  }

  /**
   * Applies the end-of-transaction refund for accounts that were both created and self-destructed
   * within the same transaction (EIP-6780). Per EIP-8037 (ethereum/EIPs PR 11573): for each such
   * account, refund to state_gas_reservoir (and decrement execution_state_gas_used) the state gas
   * for:
   *
   * <ul>
   *   <li>account creation: {@code 112 × cost_per_state_byte}
   *   <li>code deposit: {@code len(code) × cost_per_state_byte}
   *   <li>non-zero storage slots: {@code 32 × cost_per_state_byte} per slot
   * </ul>
   *
   * @param initialFrame the initial (depth-0) frame after transaction execution
   */
  default void refundSameTransactionSelfDestructStateGas(final MessageFrame initialFrame) {}

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
    long stateGas = 0;
    if (isContractCreation) {
      stateGas += createStateGas();
    }
    if (codeDelegationCount > 0) {
      // Worst case: all delegators are new accounts → (112 + 23) * cpsb each
      stateGas += (emptyAccountDelegationStateGas() + authBaseStateGas()) * codeDelegationCount;
    }
    return stateGas;
  }

  /** A no-op implementation that returns 0 for all state gas costs and performs no charging. */
  StateGasCostCalculator NONE =
      new StateGasCostCalculator() {
        @Override
        public long costPerStateByte() {
          return 0L;
        }

        @Override
        public long createStateGas() {
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
