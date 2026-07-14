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

import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Optional;
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

  /**
   * Whether a value-bearing CALL to {@code recipientAddress} would create a new account leaf (the
   * shared predicate for {@link #chargeCallNewAccountStateGas} and {@link
   * #refundCallNewAccountStateGas}): a nonzero transfer to a non-existent or empty recipient.
   */
  private static boolean callCreatesNewAccount(
      final MessageFrame frame, final Address recipientAddress, final Wei transferValue) {
    if (transferValue.isZero()) {
      return false;
    }
    final Account recipient = frame.getWorldUpdater().get(recipientAddress);
    return recipient == null || recipient.isEmpty();
  }

  @Override
  public boolean chargeCallNewAccountStateGas(
      final MessageFrame frame, final Address recipientAddress, final Wei transferValue) {
    return !callCreatesNewAccount(frame, recipientAddress, transferValue)
        || frame.consumeStateGas(newAccountStateGas());
  }

  @Override
  public void refundCallNewAccountStateGas(
      final MessageFrame frame, final Address recipientAddress, final Wei transferValue) {
    if (callCreatesNewAccount(frame, recipientAddress, transferValue)) {
      creditStateGasRefund(frame, newAccountStateGas());
    }
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
  public long transactionIntrinsicStateGas(
      final boolean isContractCreation, final long codeDelegationCount) {
    // EIP-2780 (devnet-7): all state-dependent costs — the contract-creation NEW_ACCOUNT and the
    // per-authorization NEW_ACCOUNT/AUTH_BASE — are charged at the top frame on the transaction's
    // pre-state, not pre-consumed in the intrinsic. The intrinsic state gas is therefore zero, so
    // the intrinsic gas-limit validity check no longer reserves them; an unaffordable runtime
    // charge instead halts the top frame with out-of-gas.
    return 0L;
  }

  @Override
  public void chargeTransactionEntry(
      final MessageFrame initialFrame,
      final GasCalculator gasCalculator,
      final WorldUpdater worldState) {
    if (initialFrame.getType() == MessageFrame.Type.CONTRACT_CREATION) {
      return;
    }
    final Address to = initialFrame.getRecipientAddress();
    // Read from the shallow transaction-level updater (already cached from code resolution) rather
    // than the frame's grandchild updater, to avoid an extra updater-chain traversal.
    final Account recipient = worldState.get(to);
    boolean outOfGas = false;
    // NEW_ACCOUNT state gas: positive value sent to a non-alive recipient. Precompiles are NOT
    // excluded here — EELS amsterdam (EIP-2780, fixed in tests-glamsterdam-devnet@v6.1.0) charges
    // account creation when value is transferred to a previously zero-balance precompile, since
    // such an address is not "alive" under EIP-161.
    if (!initialFrame.getValue().isZero() && (recipient == null || recipient.isEmpty())) {
      outOfGas = !initialFrame.consumeStateGas(newAccountStateGas());
    }
    // EIP-2780 (devnet-7, #3045): top-level access to a delegated recipient's target is warm/cold
    // aware — WARM_ACCESS when the target is already in the access list (e.g. via the tx access
    // list), otherwise COLD_ACCOUNT_ACCESS with the target added to the warm set.
    if (!outOfGas && recipient != null && hasCodeDelegation(recipient.getCode())) {
      final Address target = CodeDelegationHelper.getTargetAddress(recipient.getCode());
      // EELS prepare_message seeds the top-frame accessed set with all precompiles, so a delegation
      // to a precompile resolves warm. warmUpAddress still records the target (its side effect must
      // stand for a subsequent access), but a precompile counts as already warm regardless.
      final boolean targetWasWarm =
          initialFrame.warmUpAddress(target) || gasCalculator.isPrecompile(target);
      final long delegationAccessCost =
          targetWasWarm
              ? gasCalculator.getWarmStorageReadCost()
              : gasCalculator.getColdAccountAccessCost();
      if (initialFrame.getRemainingGas() >= delegationAccessCost) {
        initialFrame.decrementRemainingGas(delegationAccessCost);
        // EIP-7928: the target is loaded only once its access is paid for, so it enters the block
        // access list here and not during the frame's eager code resolution — an access charge that
        // runs out of gas leaves the target out of the list entirely.
        initialFrame.getEip7928AccessList().ifPresent(bal -> bal.addTouchedAccount(target));
      } else {
        outOfGas = true;
      }
    }
    if (outOfGas) {
      initialFrame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
      initialFrame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
    }
  }

  @Override
  public void refundCreateStateGas(final MessageFrame frame) {
    creditStateGasRefund(frame, createStateGas());
  }

  @Override
  public void refundFailedTopFrameStateGas(
      final MessageFrame initialFrame, final long amount, final long burnedSpill) {
    // stateGasUsed always drops by the full charge (the leaf did not persist), but only the part
    // that is not burned with gasRemaining is credited back as spendable gas.
    final long credited = Math.max(0L, amount - burnedSpill);
    if (credited > 0L) {
      creditStateGasRefund(initialFrame, credited);
    }
    if (burnedSpill > 0L) {
      initialFrame.decrementStateGasUsed(burnedSpill);
    }
  }

  @Override
  public void refundStorageSetStateGas(
      final MessageFrame frame,
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    if (StorageTransition.of(newValue, currentValue, originalValue).isUnwoundSet()) {
      creditStateGasRefund(frame, storageSetStateGas());
    }
  }

  /**
   * Credits {@code amount} of state gas back to the frame in LIFO order, matching EELS {@code
   * credit_state_gas_refund}. State-gas charges draw from the reservoir first and from gasRemaining
   * (spill) last, so a refund credits the pool charged last first: gasRemaining up to the frame's
   * spilled amount, then the reservoir. stateGasUsed is always decremented by the full amount. This
   * routing is observable — a refund that lands in gasRemaining is visible only to the current
   * frame's regular gas, never to a sub-call that can only draw state gas from the reservoir.
   */
  private static void creditStateGasRefund(final MessageFrame frame, final long amount) {
    final long fromGasLeft = Math.min(amount, frame.getStateGasSpilled());
    if (fromGasLeft > 0L) {
      frame.incrementRemainingGas(fromGasLeft);
      frame.decrementStateGasSpilled(fromGasLeft);
    }
    final long toReservoir = amount - fromGasLeft;
    if (toReservoir > 0L) {
      frame.incrementStateGasReservoir(toReservoir);
    }
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
