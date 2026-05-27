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

  /** The target state growth per year in bytes (100 GiB). */
  static final long TARGET_STATE_GROWTH_PER_YEAR = 100L * 1024L * 1024L * 1024L;

  /** Seconds per year used in cpsb calculation (7200 slots/day * 365 days). */
  static final long SLOTS_PER_YEAR = 7200L * 365L;

  /**
   * Number of state bytes per new account (20-byte address + 8-byte nonce + 32-byte balance +
   * 32-byte code hash + 20-byte storage root = 112 bytes).
   */
  static final int STATE_BYTES_PER_ACCOUNT = 112;

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

  /** Offset added before quantization, subtracted after. */
  static final int CPSB_OFFSET = 9578;

  /** Number of significant bits retained in quantized cpsb. */
  static final int CPSB_SIGNIFICANT_BITS = 5;

  /** Keccak256 word gas cost for code deposit hashing. */
  static final long KECCAK256_WORD_GAS_COST = 6L;

  /** The mainnet transaction gas limit cap from EIP-7825, enforced at runtime on regular gas. */
  static final long TX_MAX_GAS_LIMIT = 16_777_216L;

  /** Instantiates a new EIP-8037 state gas cost calculator. */
  public Eip8037StateGasCostCalculator() {}

  /**
   * Hardcoded cost per state byte for devnet-3. This value (1174) corresponds to a 100M block gas
   * limit. The test framework cannot currently handle dynamic gas prices, so cpsb is treated as a
   * fork constant. For devnet-4, the full EIP-8037 dynamic calculation will be restored.
   */
  static final long DEVNET_COST_PER_STATE_BYTE = 1174L;

  @Override
  public long costPerStateByte(final long blockGasLimit) {
    // TODO(devnet-4): Restore dynamic cpsb calculation based on block gas limit:
    // return costPerStateByteFromGasLimit(blockGasLimit);
    return DEVNET_COST_PER_STATE_BYTE;
  }

  /**
   * Dynamic cost per state byte calculation from the full EIP-8037 specification. Derives cpsb from
   * the block gas limit by targeting TARGET_STATE_GROWTH_PER_YEAR at 50% average gas utilization,
   * then quantizes to retain CPSB_SIGNIFICANT_BITS significant bits.
   *
   * <p>Currently unused (hardcoded for devnet-3). Will be re-enabled for devnet-4.
   *
   * @param blockGasLimit the block gas limit
   * @return the quantized cost per state byte
   */
  static long costPerStateByteFromGasLimit(final long blockGasLimit) {
    // cpsb = ceil(((gas_limit / 2) * SLOTS_PER_YEAR) / TARGET_STATE_GROWTH_PER_YEAR)
    final long numerator = (blockGasLimit / 2) * SLOTS_PER_YEAR;
    final long raw = (numerator + TARGET_STATE_GROWTH_PER_YEAR - 1) / TARGET_STATE_GROWTH_PER_YEAR;
    if (raw == 0) {
      return 0L;
    }
    // Quantize: retain only CPSB_SIGNIFICANT_BITS significant bits of (raw + CPSB_OFFSET)
    final long shifted = raw + CPSB_OFFSET;
    final int bitLen = Long.SIZE - Long.numberOfLeadingZeros(shifted);
    final int shift = Math.max(bitLen - CPSB_SIGNIFICANT_BITS, 0);
    return Math.max(((shifted >> shift) << shift) - CPSB_OFFSET, 1L);
  }

  @Override
  public long createStateGas(final long blockGasLimit) {
    return STATE_BYTES_PER_ACCOUNT * costPerStateByte(blockGasLimit);
  }

  @Override
  public long codeDepositStateGas(final int codeSize, final long blockGasLimit) {
    return costPerStateByte(blockGasLimit) * codeSize;
  }

  @Override
  public long codeDepositHashGas(final int codeSize) {
    // 6 * ceil(codeSize / 32)
    return KECCAK256_WORD_GAS_COST * ((codeSize + 31) / 32);
  }

  @Override
  public long newAccountStateGas(final long blockGasLimit) {
    return createStateGas(blockGasLimit);
  }

  @Override
  public long storageSetStateGas(final long blockGasLimit) {
    return STATE_BYTES_PER_STORAGE_SLOT * costPerStateByte(blockGasLimit);
  }

  @Override
  public long storageSetRegularGas() {
    return STORAGE_SET_REGULAR_GAS;
  }

  @Override
  public long authBaseStateGas(final long blockGasLimit) {
    return STATE_BYTES_PER_AUTH * costPerStateByte(blockGasLimit);
  }

  @Override
  public long authBaseRegularGas() {
    return AUTH_BASE_REGULAR_GAS;
  }

  @Override
  public long emptyAccountDelegationStateGas(final long blockGasLimit) {
    return createStateGas(blockGasLimit);
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
    final UInt256 currentVal = currentValue.get();
    final UInt256 originalVal = originalValue.get();
    // State gas applies only for the SSTORE_SET case: original is zero and we're setting nonzero
    if (originalVal.isZero() && currentVal.isZero() && !newValue.isZero()) {
      final long blockGasLimit = frame.getBlockValues().getGasLimit();
      return frame.consumeStateGas(storageSetStateGas(blockGasLimit));
    }
    return true;
  }

  @Override
  public boolean chargeCreateStateGas(final MessageFrame frame) {
    final long blockGasLimit = frame.getBlockValues().getGasLimit();
    return frame.consumeStateGas(createStateGas(blockGasLimit));
  }

  @Override
  public boolean chargeCodeDepositStateGas(final MessageFrame frame, final int codeSize) {
    final long blockGasLimit = frame.getBlockValues().getGasLimit();
    return frame.consumeStateGas(codeDepositStateGas(codeSize, blockGasLimit));
  }

  @Override
  public boolean chargeCallNewAccountStateGas(
      final MessageFrame frame, final Address recipientAddress, final Wei transferValue) {
    if (!transferValue.isZero()) {
      final Account recipient = frame.getWorldUpdater().get(recipientAddress);
      if (recipient == null || recipient.isEmpty()) {
        final long blockGasLimit = frame.getBlockValues().getGasLimit();
        return frame.consumeStateGas(newAccountStateGas(blockGasLimit));
      }
    }
    return true;
  }

  @Override
  public boolean chargeSelfDestructNewAccountStateGas(
      final MessageFrame frame, final Account beneficiary, final Wei originatorBalance) {
    if ((beneficiary == null || beneficiary.isEmpty()) && !originatorBalance.isZero()) {
      final long blockGasLimit = frame.getBlockValues().getGasLimit();
      return frame.consumeStateGas(newAccountStateGas(blockGasLimit));
    }
    return true;
  }

  @Override
  public boolean chargeCodeDelegationStateGas(
      final MessageFrame frame, final long totalDelegations, final long alreadyExistingDelegators) {
    final long blockGasLimit = frame.getBlockValues().getGasLimit();
    // Each authorization incurs auth base state gas (23 * cpsb)
    if (!frame.consumeStateGas(authBaseStateGas(blockGasLimit) * totalDelegations)) {
      return false;
    }
    // New empty accounts incur additional state gas (112 * cpsb)
    final long newEmptyAccounts = totalDelegations - alreadyExistingDelegators;
    if (newEmptyAccounts > 0
        && !frame.consumeStateGas(
            emptyAccountDelegationStateGas(blockGasLimit) * newEmptyAccounts)) {
      return false;
    }
    // EIP-8037: the intrinsic state gas is sized assuming every authority is a new empty
    // account (emptyAccountDelegationStateGas per auth). For authorities that already exist,
    // that pre-charge was not consumed, park it in the state gas reservoir so it is returned
    // to the sender alongside any unused gas on halt/revert, matching the specification's
    // set_delegation behavior (intrinsic_state_gas -= refund; state_gas_reservoir += refund).
    if (alreadyExistingDelegators > 0) {
      final long reservoirCredit =
          emptyAccountDelegationStateGas(blockGasLimit) * alreadyExistingDelegators;
      if (frame.getRemainingGas() < reservoirCredit) {
        return false;
      }
      frame.decrementRemainingGas(reservoirCredit);
      frame.incrementStateGasReservoir(reservoirCredit);
    }
    return true;
  }

  @Override
  public void refundStorageSetStateGas(
      final MessageFrame frame,
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    // Only refund for 0→X→0: original is zero, current is nonzero, new is zero
    if (newValue.isZero() && !currentValue.get().isZero() && originalValue.get().isZero()) {
      final long blockGasLimit = frame.getBlockValues().getGasLimit();
      final long refundAmount = storageSetStateGas(blockGasLimit);
      // State gas refund goes into the regular refund counter, subject to the 1/5 cap.
      // stateGasUsed is NOT decremented — it tracks gross state gas consumed.
      frame.incrementGasRefund(refundAmount);
    }
  }
}
