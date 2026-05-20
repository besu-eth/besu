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

import java.util.function.Supplier;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * EIP-8037 state gas cost calculator implementation.
 *
 * <p>Computes state gas costs based on a dynamic cost_per_state_byte (cpsb) derived from the block
 * gas limit:
 *
 * <pre>
 *   cpsb = ceil(((gas_limit / 2) * 7200 * 365) / TARGET_STATE_GROWTH_PER_YEAR)
 * </pre>
 *
 * <p>Where TARGET_STATE_GROWTH_PER_YEAR = 100 * 1024^3 bytes (100 GiB).
 */
public class Eip8037StateGasCostCalculator implements StateGasCostCalculator {
  /** Number of state bytes per new account. */
  static final int STATE_BYTES_PER_NEW_ACCOUNT = 120;

  /** Number of state bytes per storage slot. */
  static final int STATE_BYTES_PER_STORAGE_SLOT = 64;

  /** Number of state bytes per auth delegation (23 bytes). */
  static final int STATE_BYTES_PER_AUTH = 23;

  /**
   * Regular gas for storage set (GAS_STORAGE_UPDATE - GAS_COLD_SLOAD = 5000 - 2100 = 2900). The
   * state portion ({@code STATE_BYTES_PER_STORAGE_SLOT * cpsb}) is charged separately.
   */
  static final long STORAGE_SET_REGULAR_GAS = 2_900L;

  /**
   * Regular gas for EIP-7702 auth base (calldata + ecrecover + cold access + warm write ≈ 7500).
   * The state portion ({@code STATE_BYTES_PER_AUTH * cpsb}) is charged separately.
   */
  static final long AUTH_BASE_REGULAR_GAS = 7_500L;

  /** Keccak256 word gas cost for code deposit hashing. */
  static final long KECCAK256_WORD_GAS_COST = 6L;

  /** The mainnet transaction gas limit cap from EIP-7825, enforced at runtime on regular gas. */
  static final long TX_MAX_GAS_LIMIT = 16_777_216L;

  static final long COST_PER_STATE_BYTE = 1530L;

  /** Instantiates a new EIP-8037 state gas cost calculator. */
  public Eip8037StateGasCostCalculator() {}

  @Override
  public long costPerStateByte() {
    return COST_PER_STATE_BYTE;
  }

  @Override
  public long createStateGas() {
    return STATE_BYTES_PER_NEW_ACCOUNT * costPerStateByte();
  }

  @Override
  public long codeDepositStateGas(final int codeSize) {
    return costPerStateByte() * codeSize;
  }

  @Override
  public long codeDepositHashGas(final int codeSize) {
    // 6 * ceil(codeSize / 32)
    return KECCAK256_WORD_GAS_COST * ((codeSize + 31) / 32);
  }

  @Override
  public long newAccountStateGas() {
    return createStateGas();
  }

  @Override
  public long storageSetStateGas() {
    return STATE_BYTES_PER_STORAGE_SLOT * costPerStateByte();
  }

  @Override
  public long storageSetRegularGas() {
    return STORAGE_SET_REGULAR_GAS;
  }

  @Override
  public long authBaseStateGas() {
    return STATE_BYTES_PER_AUTH * costPerStateByte();
  }

  @Override
  public long authBaseRegularGas() {
    return AUTH_BASE_REGULAR_GAS;
  }

  @Override
  public long emptyAccountDelegationStateGas() {
    return createStateGas();
  }

  @Override
  public long transactionRegularGasLimit() {
    return TX_MAX_GAS_LIMIT;
  }

  @Override
  public boolean isActive() {
    return true;
  }

  // ---- Charge method overrides ----

  @Override
  public boolean chargeStorageSetStateGas(
      final MessageFrame frame,
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    if (StorageTransition.of(newValue, currentValue, originalValue).isStorageSet()) {
      return frame.consumeStateGas(storageSetStateGas());
    }
    return true;
  }

  @Override
  public boolean chargeCreateStateGas(final MessageFrame frame) {
    return frame.consumeStateGas(createStateGas());
  }

  @Override
  public boolean chargeCodeDepositStateGas(final MessageFrame frame, final int codeSize) {
    return frame.consumeStateGas(codeDepositStateGas(codeSize));
  }

  @Override
  public boolean chargeCallNewAccountStateGas(
      final MessageFrame frame, final Address recipientAddress, final Wei transferValue) {
    if (!transferValue.isZero()) {
      final Account recipient = frame.getWorldUpdater().get(recipientAddress);
      if (recipient == null || recipient.isEmpty()) {
        return frame.consumeStateGas(newAccountStateGas());
      }
    }
    return true;
  }

  @Override
  public boolean chargeSelfDestructNewAccountStateGas(
      final MessageFrame frame, final Account beneficiary, final Wei originatorBalance) {
    if ((beneficiary == null || beneficiary.isEmpty()) && !originatorBalance.isZero()) {
      return frame.consumeStateGas(newAccountStateGas());
    }
    return true;
  }

  @Override
  public boolean chargeCodeDelegationStateGas(
      final MessageFrame frame,
      final long totalDelegations,
      final long alreadyExistingDelegators,
      final long authBaseRefundCount) {
    // Charge the full worst-case intrinsic up-front so block_regular = tx_gas -
    // intrinsic_state_gas regardless of authority pre-state. Refunds below restore the actual
    // state growth to both the reservoir (for the sender) and stateGasUsed (for block accounting).
    final long perAuthIntrinsic = authBaseStateGas() + emptyAccountDelegationStateGas();
    if (!frame.consumeStateGas(perAuthIntrinsic * totalDelegations)) {
      return false;
    }
    long refund = emptyAccountDelegationStateGas() * alreadyExistingDelegators;
    refund += authBaseStateGas() * authBaseRefundCount;
    if (refund > 0L) {
      creditReservoir(frame, refund);
    }
    return true;
  }

  @Override
  public void refundCreateStateGas(final MessageFrame frame) {
    creditReservoir(frame, createStateGas());
  }

  @Override
  public void refundTxCreateIntrinsicStateGas(final MessageFrame initialFrame) {
    creditReservoir(initialFrame, createStateGas());
  }

  @Override
  public void refundStorageSetStateGas(
      final MessageFrame frame,
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    if (StorageTransition.of(newValue, currentValue, originalValue).isUnwoundSet()) {
      creditReservoir(frame, storageSetStateGas());
    }
  }

  /**
   * Credits {@code amount} back to the reservoir and decrements stateGasUsed by the same amount.
   * Both counters are UndoScalar-tracked, so the credit is rolled back if the enclosing frame fails
   * — the frame-failure handler then restores the spill via a reservoir credit, so same-frame
   * charge/refund pairs net to zero for the sender.
   */
  private static void creditReservoir(final MessageFrame frame, final long amount) {
    frame.incrementStateGasReservoir(amount);
    frame.decrementStateGasUsed(amount);
  }

  /**
   * The three booleans that determine SSTORE state-gas treatment under EIP-8037: the slot's value
   * at tx-entry (original), at the SSTORE site (current), and the new value being written.
   */
  private record StorageTransition(
      boolean originalIsZero, boolean currentIsZero, boolean newIsZero) {

    static StorageTransition of(
        final UInt256 newValue,
        final Supplier<UInt256> currentValue,
        final Supplier<UInt256> originalValue) {
      return new StorageTransition(
          originalValue.get().isZero(), currentValue.get().isZero(), newValue.isZero());
    }

    /** SSTORE_SET (0 → 0 → X): the only transition that consumes storage-set state gas. */
    boolean isStorageSet() {
      return originalIsZero && currentIsZero && !newIsZero;
    }

    /** In-tx unwind (0 → X → 0): the only transition that refunds storage-set state gas. */
    boolean isUnwoundSet() {
      return originalIsZero && !currentIsZero && newIsZero;
    }
  }
}
