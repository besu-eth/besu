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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.Set;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * EIP-8037 state-gas refund helpers that mutate a {@link MessageFrame} but are NOT part of the
 * {@link StateGasCostCalculator} interface — that interface is a pure cost calculator. Lives here
 * because the refund amounts come from a {@code StateGasCostCalculator}.
 */
public final class StateGasRefunds {

  private StateGasRefunds() {}

  /**
   * EIP-8037 end-of-tx refund (EIP-6780): for accounts both created and self-destructed within the
   * same transaction, return to {@code state_gas_reservoir} the state gas paid for account
   * creation, code deposit, and non-zero storage slots. The total is capped at execution-time state
   * gas — the intrinsic charge paid at transaction start is never refunded. Storage slots restored
   * to zero during execution (0→X→0) are skipped: the SSTORE restoration refund has already
   * returned their state gas.
   *
   * <p>Must run before {@code tx_gas_used_before_refund} is computed so the sender is not charged
   * for state that was destroyed. Matches geth / nethermind / erigon / ethrex behaviour.
   *
   * @param initialFrame the depth-0 frame after transaction execution
   * @param intrinsicStateGas the intrinsic state gas charged at tx start; refund cannot consume
   *     this portion of {@code stateGasUsed}
   * @param stateGasCalc the active state-gas cost calculator
   */
  public static void applySameTransactionSelfDestructRefund(
      final MessageFrame initialFrame,
      final long intrinsicStateGas,
      final StateGasCostCalculator stateGasCalc) {
    final Set<Address> destroyed = initialFrame.getSelfDestructs();
    if (destroyed.isEmpty()) {
      return;
    }
    final Set<Address> created = initialFrame.getCreates();
    final long storageSlotGas = stateGasCalc.storageSetStateGas();
    final long newContractStateGas = stateGasCalc.newContractStateGas();
    long totalRefund = 0L;
    for (final Address address : destroyed) {
      if (!created.contains(address)) {
        // EIP-6780: only refund when the account was also created in this transaction.
        continue;
      }
      final MutableAccount account = initialFrame.getWorldUpdater().getAccount(address);
      if (account == null) {
        continue;
      }
      totalRefund = clampedAdd(totalRefund, newContractStateGas);
      totalRefund =
          clampedAdd(totalRefund, stateGasCalc.codeDepositStateGas(account.getCode().size()));
      // The account was created in this transaction, so every slot it currently holds is in the
      // updater's journaled writes — Bonsai does not support trie enumeration.
      for (final UInt256 value : account.getUpdatedStorage().values()) {
        if (!value.isZero()) {
          totalRefund = clampedAdd(totalRefund, storageSlotGas);
        }
      }
    }
    if (totalRefund > 0L) {
      final long executionStateGas =
          Math.max(0L, initialFrame.getStateGasUsed() - intrinsicStateGas);
      final long cappedRefund = Math.min(totalRefund, executionStateGas);
      if (cappedRefund > 0L) {
        initialFrame.incrementStateGasReservoir(cappedRefund);
        initialFrame.decrementStateGasUsed(cappedRefund);
      }
    }
  }
}
