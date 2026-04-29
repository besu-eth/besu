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
import org.hyperledger.besu.evm.frame.Eip8037Trace;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.StateChange;

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

  // ---- Frame-end byte-diff accounting (matches EELS apply_frame_state_gas) ----

  @Override
  public boolean applyFrameEndStateGasAccounting(final MessageFrame frame) {
    final List<StateChange> events = frame.getStateChanges();
    final int frameStart = frame.getStateGasFrameStartIndex();
    final int end = events.size();

    // Build the touch list from events in [frameStart, end) — INCLUDING events emitted by
    // descendants, since their first-seen `before` carries this frame's frame-entry value.
    // For each (addr, slot): keep the FIRST event's flags (tx-entry / frame-entry zero) and
    // overwrite exit on each later event so the LAST event's `after` is the frame-exit value.
    final Table<Address, Bytes32, StorageDelta> storage = HashBasedTable.create();
    final Set<Address> accountsCreated = new HashSet<>();
    final Map<Address, Integer> codeDeposits = new HashMap<>();
    for (int i = frameStart; i < end; i++) {
      switch (events.get(i)) {
        case StateChange.Storage s -> {
          final StorageDelta d = storage.get(s.address(), s.key());
          if (d == null) {
            storage.put(
                s.address(),
                s.key(),
                new StorageDelta(s.txEntryIsZero(), s.beforeIsZero(), s.afterIsZero()));
          } else {
            d.exitIsZero = s.afterIsZero();
          }
        }
        case StateChange.AccountCreated a -> accountsCreated.add(a.address());
        case StateChange.CodeDeposit c -> codeDeposits.put(c.address(), c.codeLength());
      }
    }

    long byteDiff = 0L;

    // Storage 4-case rule from EIP-8037 / EELS compute_state_byte_diff.
    for (final Table.Cell<Address, Bytes32, StorageDelta> cell : storage.cellSet()) {
      final StorageDelta d = cell.getValue();
      if (!d.exitIsZero && d.entryIsZero && d.txEntryIsZero) {
        byteDiff += STATE_BYTES_PER_STORAGE_SET; // new slot
      } else if (d.exitIsZero && !d.entryIsZero && d.txEntryIsZero) {
        byteDiff -= STATE_BYTES_PER_STORAGE_SET; // cleared from tx-zero (in-tx unwind)
      }
      // Other transitions (cleared from tx-nonzero, non-zero→non-zero, transient): 0.
    }

    // Account creation: +112 per address that is materialised at frame-exit. Reverted child
    // frames have their AccountCreated events truncated by UndoList rollback, so the touch list
    // here is exactly the set of accounts whose creation persisted.
    if (!accountsCreated.isEmpty()) {
      for (final Address addr : accountsCreated) {
        final var live = frame.getWorldUpdater().get(addr);
        if (live != null && !live.isEmpty()) {
          byteDiff += STATE_BYTES_PER_NEW_ACCOUNT;
        }
      }
    }

    // Code deposit: +len(code) per address whose code persisted to frame-exit.
    if (!codeDeposits.isEmpty()) {
      for (final Map.Entry<Address, Integer> entry : codeDeposits.entrySet()) {
        final var live = frame.getWorldUpdater().get(entry.getKey());
        if (live != null && live.getCode() != null && !live.getCode().isEmpty()) {
          byteDiff += entry.getValue();
        }
      }
    }

    // This frame's net growth cost minus what its successful descendants already paid.
    // We branch on the comparison so `charge` and `refund` can be named as always-positive
    // quantities.
    final long growthCost = byteDiff * costPerStateByte();
    final long alreadyPaid = frame.getStateGasUsed() - frame.getFrameEntryStateGasUsed();

    final boolean ok;
    final String action;
    if (growthCost > alreadyPaid) {
      // Net-grow residual: charge the difference. consumeStateGas drains the reservoir first then
      // spills into gasRemaining; on success it increments stateGasUsed by `charge`.
      final long charge = growthCost - alreadyPaid;
      ok = frame.consumeStateGas(charge);
      action = "CHARGE";
    } else if (growthCost < alreadyPaid) {
      // Net-shrink residual: descendants over-paid (e.g., a child set a slot this frame later
      // cleared). Credit the over-payment back to the reservoir. We do NOT decrement
      // stateGasUsed — the over-credit is bounded when an ancestor's growthCost flips positive
      // and recharges.
      final long refund = alreadyPaid - growthCost;
      frame.incrementStateGasReservoir(refund);
      ok = true;
      action = "REFUND";
    } else {
      ok = true;
      action = "NOOP";
    }
    if (Eip8037Trace.ENABLED) {
      Eip8037Trace.applyFrame(
          frame.getMessageStackSize(),
          byteDiff,
          growthCost,
          alreadyPaid,
          growthCost - alreadyPaid,
          action,
          ok,
          frame.getStateGasReservoir(),
          frame.getRemainingGas(),
          frame.getStateGasUsed());
    }
    return ok;
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
  private static final class StorageDelta {
    final boolean txEntryIsZero;
    final boolean entryIsZero;
    boolean exitIsZero;

    StorageDelta(final boolean txEntryIsZero, final boolean entryIsZero, final boolean exitIsZero) {
      this.txEntryIsZero = txEntryIsZero;
      this.entryIsZero = entryIsZero;
      this.exitIsZero = exitIsZero;
    }
  }
}
