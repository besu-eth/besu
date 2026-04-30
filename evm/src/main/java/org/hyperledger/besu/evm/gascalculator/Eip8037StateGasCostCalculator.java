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
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.Set;
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
  /**
   * Number of state bytes per new account (20-byte address + 8-byte nonce + 32-byte balance +
   * 32-byte code hash + 20-byte storage root = 112 bytes).
   */
  static final int STATE_BYTES_PER_NEW_ACCOUNT = 112;

  /** Number of state bytes per storage slot (32 bytes for key + value). */
  static final int STATE_BYTES_PER_STORAGE_SLOT = 32;

  /** Number of state bytes per auth delegation (23 bytes). */
  static final int STATE_BYTES_PER_AUTH = 23;

  /**
   * Regular gas for storage set (GAS_STORAGE_UPDATE - GAS_COLD_SLOAD = 5000 - 2100 = 2900). The
   * state portion (32 * cpsb) is charged separately.
   */
  static final long STORAGE_SET_REGULAR_GAS = 2_900L;

  /**
   * Regular gas for EIP-7702 auth base (calldata + ecrecover + cold access + warm write ≈ 7500).
   * The state portion (23 * cpsb) is charged separately.
   */
  static final long AUTH_BASE_REGULAR_GAS = 7_500L;

  /** Keccak256 word gas cost for code deposit hashing. */
  static final long KECCAK256_WORD_GAS_COST = 6L;

  /** The mainnet transaction gas limit cap from EIP-7825, enforced at runtime on regular gas. */
  static final long TX_MAX_GAS_LIMIT = 16_777_216L;

  static final long COST_PER_STATE_BYTE = 1174L;

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
      final MessageFrame frame, final long totalDelegations, final long alreadyExistingDelegators) {
    // Each authorization incurs auth base state gas (23 * cpsb)
    if (!frame.consumeStateGas(authBaseStateGas() * totalDelegations)) {
      return false;
    }
    // New empty accounts incur additional state gas (112 * cpsb)
    final long newEmptyAccounts = totalDelegations - alreadyExistingDelegators;
    if (newEmptyAccounts > 0
        && !frame.consumeStateGas(emptyAccountDelegationStateGas() * newEmptyAccounts)) {
      return false;
    }
    // EIP-8037: intrinsic state gas is sized assuming every
    // authority is a new empty account ((112 + 23) × cpsb per auth) and is immutable after
    // transaction validation. When an authority already exists, the 112 × cpsb portion is
    // refunded directly to state_gas_reservoir during authorization processing. The intrinsic
    // value itself is not mutated; the refund is reflected in the final state_gas_reservoir.
    //
    // Because intrinsic state gas is not pre-deducted from tx.gas in this implementation (it is
    // charged explicitly via consumeStateGas above, drawing from reservoir/gas_left), the
    // reservoir credit is paired with a matching gas_left debit to preserve the sender
    // bookkeeping invariant tx.gas - (gas_left + reservoir) = gas actually charged.
    if (alreadyExistingDelegators > 0) {
      final long reservoirCredit = emptyAccountDelegationStateGas() * alreadyExistingDelegators;
      if (frame.getRemainingGas() < reservoirCredit) {
        return false;
      }
      frame.decrementRemainingGas(reservoirCredit);
      frame.incrementStateGasReservoir(reservoirCredit);
    }
    return true;
  }

  @Override
  public void refundSameTransactionSelfDestructStateGas(final MessageFrame initialFrame) {
    final Set<Address> destroyed = initialFrame.getSelfDestructs();
    if (destroyed.isEmpty()) {
      return;
    }
    final Set<Address> created = initialFrame.getCreates();
    final long storageSlotGas = storageSetStateGas();
    long totalRefund = 0L;
    for (final Address address : destroyed) {
      if (!created.contains(address)) {
        // Only refund for accounts both created AND destroyed in this transaction (EIP-6780).
        continue;
      }
      final MutableAccount account = initialFrame.getWorldUpdater().getAccount(address);
      if (account == null) {
        continue;
      }
      totalRefund += createStateGas();
      totalRefund += codeDepositStateGas(account.getCode().size());
      // The account was created in this transaction, so every slot it currently holds is in
      // the updater's journaled writes — Bonsai does not support trie enumeration.
      for (final UInt256 value : account.getUpdatedStorage().values()) {
        if (!value.isZero()) {
          totalRefund += storageSlotGas;
        }
      }
    }
    if (totalRefund > 0L) {
      initialFrame.incrementStateGasReservoir(totalRefund);
      initialFrame.decrementStateGasUsed(totalRefund);
    }
  }

  @Override
  public void refundCreateStateGas(final MessageFrame frame) {
    applyNoGrowthRefund(frame, createStateGas());
  }

  @Override
  public void refundStorageSetStateGas(
      final MessageFrame frame,
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    if (StorageTransition.of(newValue, currentValue, originalValue).isUnwoundSet()) {
      applyNoGrowthRefund(frame, storageSetStateGas());
    }
  }

  /**
   * Applies a no-growth state-gas refund: credits {@code amount} back to the reservoir, decrements
   * {@code stateGasUsed}, and records the refund in the no-growth counter.
   *
   * <p>The credit goes directly to {@code state_gas_reservoir}, bypassing the 20% refund-counter
   * cap, so the full amount is returned regardless of cost_per_state_byte. The refund mutates the
   * frame's {@code UndoScalar} counters and is therefore undone via {@link MessageFrame#rollback()}
   * on revert/halt — it contributes to the reservoir only when the full frame chain succeeds.
   *
   * <p>The amount is also recorded via {@link MessageFrame#recordNoGrowthStateGasRefund(long)} so
   * {@code AbstractMessageProcessor.handleStateGasSpill} can subtract refunds-in-scope from the
   * spill credit on revert/halt — those refunds must contribute nothing to a parent's reservoir
   * when any frame in the chain fails. Mirrors the {@code state_gas_refund} counter.
   */
  private static void applyNoGrowthRefund(final MessageFrame frame, final long amount) {
    frame.incrementStateGasReservoir(amount);
    frame.decrementStateGasUsed(amount);
    frame.recordNoGrowthStateGasRefund(amount);
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
