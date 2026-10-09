/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.evm.v2.operation;

import static org.hyperledger.besu.evm.v2.operation.StackUtil.readBytes32At;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;
import org.hyperledger.besu.evm.gascalculator.StorageTransition;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.units.bigints.UInt256;

/** The SStore operation. */
public class SStoreOperationV2 extends AbstractOperationV2 {

  private static final OperationResult ILLEGAL_STATE_CHANGE =
      new OperationResult(0L, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);

  private final long minimumGasRemaining;

  /**
   * Instantiates a new SStore operation.
   *
   * @param gasCalculator the gas calculator
   * @param minimumGasRemaining the minimum gas remaining
   */
  public SStoreOperationV2(final GasCalculator gasCalculator, final long minimumGasRemaining) {
    super(0x55, "SSTORE", 2, 0, gasCalculator);
    this.minimumGasRemaining = minimumGasRemaining;
  }

  /**
   * Gets minimum gas remaining.
   *
   * @return the minimum gas remaining
   */
  public long getMinimumGasRemaining() {
    return minimumGasRemaining;
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (!frame.stackHasItemsV2(2)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final UInt256 key = UInt256.fromBytes(readBytes32At(stack, top, 0));
    final UInt256 newValue = UInt256.fromBytes(readBytes32At(stack, top, 1));
    frame.setTopV2(top - 2);

    // EIP-8038: resolve the account ahead of the gas checks below, so that an SSTORE which halts
    // for insufficient gas has still recorded the account in the block access list.
    final MutableAccount account = getMutableAccount(frame.getRecipientAddress(), frame);
    if (account == null) {
      return ILLEGAL_STATE_CHANGE;
    }

    final long remainingGas = frame.getRemainingGas();
    if (frame.isStatic()) {
      return new OperationResult(remainingGas, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    if (remainingGas <= minimumGasRemaining) {
      return new OperationResult(minimumGasRemaining, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Address address = account.getAddress();
    final boolean slotIsWarm = frame.warmUpStorage(address, key);

    // EIP-8038: the repriced access cost can exceed the EIP-2200 stipend, so the sentry above no
    // longer guarantees the access is affordable. Check before the current-value read below, which
    // would otherwise record the slot in the block access list (EIP-7928) for an unpaid access.
    final long accessCost =
        gasCalculator().getWarmStorageReadCost()
            + (slotIsWarm ? 0L : gasCalculator().getSStoreColdAccessGasCost());
    if (remainingGas < accessCost) {
      return new OperationResult(accessCost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Supplier<UInt256> currentValueSupplier =
        Suppliers.memoize(() -> getStorageValue(account, key, frame));
    final Supplier<UInt256> originalValueSupplier =
        Suppliers.memoize(() -> account.getOriginalStorageValue(key));

    final long cost =
        gasCalculator().slotAccessCost(newValue, currentValueSupplier, originalValueSupplier)
            + (slotIsWarm ? 0L : gasCalculator().getSStoreColdAccessGasCost());
    if (remainingGas < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // EIP-8037: Deduct execution gas before charging state gas (ordering requirement).
    // State gas draws from the reservoir first, then from gasRemaining; deducting execution
    // gas first ensures the reservoir/gasRemaining split is correct.
    frame.decrementRemainingGas(cost);

    frame.incrementGasRefund(
        gasCalculator()
            .calculateStorageRefundAmount(newValue, currentValueSupplier, originalValueSupplier));

    final StateGasCostCalculator stateGasCalc = gasCalculator().stateGasCostCalculator();
    final StorageTransition transition =
        StorageTransition.of(newValue, currentValueSupplier, originalValueSupplier);
    final long storageSetStateGas = stateGasCalc.storageSetStateGas();
    // EIP-8037: Refund state gas for 0→X→0 (storage set then clear), otherwise charge state gas
    // for a storage set (0 → nonzero). The two transitions are mutually exclusive.
    if (transition.isUnwoundSet()) {
      frame.refillStateGasReservoir(storageSetStateGas);
    } else if (transition.isStorageSet() && !frame.consumeStateGas(storageSetStateGas)) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // Add execution gas back — the EVM loop will deduct it via the OperationResult.
    frame.incrementRemainingGas(cost);

    account.setStorageValue(key, newValue);
    frame.storageWasUpdated(key, newValue);
    frame.getEip7928AccessList().ifPresent(t -> t.addSlotAccessForAccount(address, key));
    return new OperationResult(cost, null);
  }
}
