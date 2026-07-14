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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.function.Supplier;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Strategy interface for EIP-8037 state creation gas cost calculations.
 *
 * <p>EIP-8037 introduces multidimensional gas metering, splitting gas into regular gas and state
 * gas. State-creation operations (CREATE, SSTORE 0->nonzero, CALL to new accounts, code deposits,
 * EIP-7702 delegations) get their costs split into a regular gas portion and a state gas portion,
 * where the state gas depends on a dynamic cost_per_state_byte (cpsb) derived from the block gas
 * limit.
 *
 * <p>Operations call the {@code charge*} methods to deduct state gas. The default (NONE)
 * implementation is a no-op; the EIP-8037 implementation performs the actual deduction.
 */
public interface StateGasCostCalculator {

  /**
   * Returns the cost per state byte for the given block gas limit.
   *
   * @return the cost per state byte
   */
  long costPerStateByte();

  /**
   * Returns the EIP-8037 state gas for a CREATE operation: {@code STATE_BYTES_PER_NEW_ACCOUNT *
   * cpsb}.
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
   * Returns the EIP-8037 state gas for creating a new account: {@code STATE_BYTES_PER_NEW_ACCOUNT *
   * cpsb}.
   *
   * @return the state gas for new account creation
   */
  long newAccountStateGas();

  /**
   * Returns the EIP-8037 state gas for storage set 0→nonzero: {@code STATE_BYTES_PER_STORAGE_SLOT *
   * cpsb}.
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
   * Returns the EIP-8037 state gas for EIP-7702 auth base: {@code STATE_BYTES_PER_AUTH * cpsb}.
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
   * Returns the EIP-8037 state gas for an EIP-7702 delegation targeting a previously empty account:
   * {@code STATE_BYTES_PER_NEW_ACCOUNT * cpsb}.
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

  // ---- Charge methods (strategy pattern for state gas deduction) ----

  /**
   * Charges state gas for SSTORE 0→nonzero (storage set). Only charges when the original value is
   * zero, current value is zero, and the new value is nonzero.
   *
   * @param frame the message frame
   * @param newValue the new storage value being written
   * @param currentValue supplier for the current storage value
   * @param originalValue supplier for the original storage value
   * @return true if gas was successfully charged, false if insufficient gas
   */
  default boolean chargeStorageSetStateGas(
      final MessageFrame frame,
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    return true;
  }

  /**
   * Charges EIP-8037 state gas for CREATE/CREATE2 operations.
   *
   * @param frame the message frame
   * @return true if gas was successfully charged, false if insufficient gas
   */
  default boolean chargeCreateStateGas(final MessageFrame frame) {
    return true;
  }

  /**
   * Charges state gas for code deposit (cpsb * codeSize).
   *
   * @param frame the message frame
   * @param codeSize the size of the deployed code in bytes
   * @return true if gas was successfully charged, false if insufficient gas
   */
  default boolean chargeCodeDepositStateGas(final MessageFrame frame, final int codeSize) {
    return true;
  }

  /**
   * Charges state gas for CALL-family operations that create a new account. Only charges when the
   * transfer value is nonzero and the recipient does not exist or is empty.
   *
   * @param frame the message frame
   * @param recipientAddress the recipient address
   * @param transferValue the value being transferred
   * @return true if gas was successfully charged, false if insufficient gas
   */
  default boolean chargeCallNewAccountStateGas(
      final MessageFrame frame, final Address recipientAddress, final Wei transferValue) {
    return true;
  }

  /**
   * Refunds the NEW_ACCOUNT state gas charged by {@link #chargeCallNewAccountStateGas} when a CALL
   * fails before the child frame runs (insufficient balance or max call depth), so no account is
   * created. Mirrors the charge condition; no-op when no charge applied.
   *
   * @param frame the message frame
   * @param recipientAddress the recipient address
   * @param transferValue the value being transferred
   */
  default void refundCallNewAccountStateGas(
      final MessageFrame frame, final Address recipientAddress, final Wei transferValue) {}

  /**
   * Charges state gas for SELFDESTRUCT that sends to a new account. Only charges when the
   * beneficiary does not exist or is empty and the originator has nonzero balance.
   *
   * @param frame the message frame
   * @param beneficiary the beneficiary account (may be null)
   * @param originatorBalance the originator's balance
   * @return true if gas was successfully charged, false if insufficient gas
   */
  default boolean chargeSelfDestructNewAccountStateGas(
      final MessageFrame frame, final Account beneficiary, final Wei originatorBalance) {
    return true;
  }

  /**
   * Charges the EIP-2780 transaction-entry costs on the depth-0 frame of a non-create transaction,
   * before any opcode runs (mirrors EELS {@code process_message_call}): NEW_ACCOUNT state gas for a
   * positive value transfer to a non-alive recipient (precompiles included, per EIP-2780), plus a
   * cold-account-access regular charge when the recipient carries an EIP-7702 delegation. Reads the
   * recipient's pre-value-transfer state from {@code worldState} (the shallow transaction-level
   * updater, which already has the recipient cached from code resolution, so this is not an extra
   * trie/disk lookup) and sets the frame to {@code EXCEPTIONAL_HALT} on insufficient gas. No-op for
   * the NONE (pre-Amsterdam) strategy, so callers invoke it unconditionally.
   *
   * @param initialFrame the depth-0 message-call frame
   * @param gasCalculator the active gas calculator, for the EIP-7702 cold-account-access cost
   * @param worldState the transaction-level world updater to read the recipient account from
   */
  default void chargeTransactionEntry(
      final MessageFrame initialFrame,
      final GasCalculator gasCalculator,
      final WorldUpdater worldState) {}

  /**
   * Refunds state gas for SSTORE when reverting a storage set (0→X→0). Only refunds when the new
   * value is zero, the current value is nonzero, and the original value is zero.
   *
   * @param frame the message frame
   * @param newValue the new storage value being written
   * @param currentValue supplier for the current storage value
   * @param originalValue supplier for the original storage value
   */
  default void refundStorageSetStateGas(
      final MessageFrame frame,
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {}

  /**
   * Refunds the EIP-8037 state gas previously charged by {@link
   * #chargeCreateStateGas(MessageFrame)} when a CREATE/CREATE2 silently fails at the opcode level
   * (insufficient balance, nonce overflow, stack depth limit, or address collision), before a child
   * frame is entered. No account is created, so no state gas should be paid — the refund is
   * credited directly to state_gas_reservoir and execution_state_gas_used is decremented.
   *
   * @param frame the message frame performing the CREATE
   */
  default void refundCreateStateGas(final MessageFrame frame) {}

  /**
   * Refunds a top-frame state-gas charge whose state effect rolls back when the transaction fails —
   * the created contract's NEW_ACCOUNT, or the recipient's NEW_ACCOUNT when value materialised an
   * empty leaf. Neither leaf persists, so the state-gas dimension nets to zero. An authorization's
   * state gas is NOT refunded this way: its delegation survives a dispatch failure.
   *
   * <p>Where the credit lands decides who pays. The reservoir-drawn part is restored, but the part
   * that spilled out of gasRemaining follows gasRemaining: a revert preserves it (the sender gets
   * it back), while an exceptional halt burns it. Pass that spilled amount as {@code burnedSpill}
   * on a halt so it is dropped from the credit — parking it in the reservoir, which is never
   * burned, would let the sender underpay. Pass 0 on a revert.
   *
   * @param initialFrame the initial (depth-0) frame of the failed transaction
   * @param amount the top-frame state gas that was charged
   * @param burnedSpill the spilled portion that is burned rather than credited back
   */
  default void refundFailedTopFrameStateGas(
      final MessageFrame initialFrame, final long amount, final long burnedSpill) {}

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
      // EIP-8037 worst case: every delegation creates a new account, so charge
      // NEW_ACCOUNT + AUTH_BASE state gas per auth. Refunds restore the unused portion later.
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
