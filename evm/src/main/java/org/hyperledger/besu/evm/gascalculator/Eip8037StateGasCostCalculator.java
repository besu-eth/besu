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
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.StateChange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * EIP-8037 state gas cost calculator implementing the frame-end accounting model from <a
 * href="https://github.com/ethereum/EIPs/pull/11573">EIP PR 11573</a>.
 *
 * <p>cost_per_state_byte (cpsb) is a fixed constant of 1174.
 */
public class Eip8037StateGasCostCalculator implements StateGasCostCalculator {
  /**
   * Number of state bytes per new account (20-byte address + 8-byte nonce + 32-byte balance +
   * 32-byte code hash + 20-byte storage root = 112 bytes).
   */
  static final int STATE_BYTES_PER_NEW_ACCOUNT = 112;

  /** Number of state bytes per storage slot set (32 bytes for key + value). */
  static final int STATE_BYTES_PER_STORAGE_SET = 32;

  /** Base number of state bytes per auth delegation (23 bytes). */
  static final int STATE_BYTES_PER_AUTH_BASE = 23;

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
    return STATE_BYTES_PER_STORAGE_SET * costPerStateByte();
  }

  @Override
  public long storageSetRegularGas() {
    return STORAGE_SET_REGULAR_GAS;
  }

  @Override
  public long authBaseStateGas() {
    return STATE_BYTES_PER_AUTH_BASE * costPerStateByte();
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
    // EIP-8037: intrinsic state gas is sized assuming every authority is a new empty account
    // ((112 + 23) × cpsb per auth) and is immutable after transaction validation. When an
    // authority already exists, the 112 × cpsb portion is refunded directly to
    // state_gas_reservoir during authorization processing. Because intrinsic state gas is not
    // pre-deducted from tx.gas in this implementation (it is charged explicitly via consumeStateGas
    // above, drawing from reservoir/gas_left), the reservoir credit is paired with a matching
    // gas_left debit to preserve the sender bookkeeping invariant
    //   tx.gas - (gas_left + reservoir) = gas actually charged.
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

  // ---- Frame-end aggregation ----

  @Override
  public boolean applyFrameEndStateGasAccounting(final MessageFrame frame) {
    final List<StateChange> events = frame.getStateChanges();
    final int frameStart = frame.getStateGasFrameStartIndex();
    final int end = events.size();
    if (frameStart >= end) {
      return true;
    }

    // Per-cell aggregate flags: tx-entry value, frame-entry value, frame-exit value (all "isZero").
    final Table<Address, Bytes32, FrameStorageFlags> storageAgg = HashBasedTable.create();
    final Set<Address> accountAgg = new HashSet<>();
    final Map<Address, Integer> codeAgg = new HashMap<>();
    final List<Integer> consumedIndices = new ArrayList<>();

    for (int i = frameStart; i < end; i++) {
      if (frame.isStateGasEventAccounted(i)) {
        continue;
      }
      final StateChange change = events.get(i);
      consumedIndices.add(i);
      switch (change) {
        case StateChange.Storage s -> {
          FrameStorageFlags slot = storageAgg.get(s.address(), s.key());
          if (slot == null) {
            storageAgg.put(
                s.address(),
                s.key(),
                new FrameStorageFlags(s.txEntryIsZero(), s.beforeIsZero(), s.afterIsZero()));
          } else {
            slot.exitIsZero = s.afterIsZero();
          }
        }
        case StateChange.AccountCreated a -> accountAgg.add(a.address());
        case StateChange.CodeDeposit c -> codeAgg.put(c.address(), c.codeLength());
      }
    }

    if (consumedIndices.isEmpty()) {
      return true;
    }

    long totalCharge = 0L;
    long totalRefund = 0L;
    final List<Runnable> pendingLedgerUpdates = new ArrayList<>();

    for (final Table.Cell<Address, Bytes32, FrameStorageFlags> cell : storageAgg.cellSet()) {
      final Address addr = cell.getRowKey();
      final Bytes32 key = cell.getColumnKey();
      final FrameStorageFlags slot = cell.getValue();
      if (slot.entryIsZero == slot.exitIsZero) {
        // Either 0→...→0 or non-zero→...→non-zero — no state-gas effect.
        continue;
      }

      if (!slot.exitIsZero && slot.txEntryIsZero && !frame.isStateGasNewSlotCharged(addr, key)) {
        // New slot: frame-exit non-zero AND tx-entry zero.
        totalCharge += storageSetStateGas();
        pendingLedgerUpdates.add(() -> frame.claimStateGasNewSlotCharge(addr, key));
      } else if (slot.exitIsZero
          && !slot.entryIsZero
          && slot.txEntryIsZero
          && !frame.isStateGasSlotRefunded(addr, key)) {
        // Cleared slot, zero at tx start: refund is unconditional; the matching charge is booked
        // elsewhere (other frames' aggregation) and the once-only ledger balances them.
        totalRefund += storageSetStateGas();
        pendingLedgerUpdates.add(() -> frame.claimStateGasSlotRefund(addr, key));
      }
      // Other transitions: no state-gas effect; SSTORE handles the regular-gas refund.
    }

    for (final Address addr : accountAgg) {
      if (!frame.isStateGasNewAccountCharged(addr)) {
        totalCharge += newAccountStateGas();
        pendingLedgerUpdates.add(() -> frame.claimStateGasNewAccountCharge(addr));
      }
    }

    for (final Map.Entry<Address, Integer> entry : codeAgg.entrySet()) {
      final Address addr = entry.getKey();
      final int length = entry.getValue();
      if (!frame.isStateGasCodeDepositCharged(addr)) {
        totalCharge += codeDepositStateGas(length);
        pendingLedgerUpdates.add(() -> frame.claimStateGasCodeDepositCharge(addr));
      }
    }

    // Apply refund first to raise the gas budget available to cover the charge.
    if (totalRefund > 0L) {
      frame.incrementStateGasReservoir(totalRefund);
      frame.decrementStateGasUsed(totalRefund);
    }
    if (totalCharge > 0L && !frame.consumeStateGas(totalCharge)) {
      return false;
    }

    for (final int idx : consumedIndices) {
      frame.markStateGasEventAccounted(idx);
    }
    for (final Runnable update : pendingLedgerUpdates) {
      update.run();
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
      // the updater's journaled writes — Bonsai does not support trie enumeration. Slots whose
      // final value is non-zero are exactly the slots whose new-slot charge has not already been
      // compensated by the frame-end 0→X→0 refund.
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

  /** Per-cell aggregate state used by {@link #applyFrameEndStateGasAccounting}. */
  private static final class FrameStorageFlags {
    final boolean txEntryIsZero;
    final boolean entryIsZero;
    boolean exitIsZero;

    FrameStorageFlags(
        final boolean txEntryIsZero, final boolean entryIsZero, final boolean exitIsZero) {
      this.txEntryIsZero = txEntryIsZero;
      this.entryIsZero = entryIsZero;
      this.exitIsZero = exitIsZero;
    }
  }
}
