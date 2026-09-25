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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;
import org.hyperledger.besu.evm.gascalculator.StorageTransition;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EVM v2 SSTORE operation using long[] stack representation.
 *
 * <p>Pops a storage key and new value from the stack, then writes the value to contract storage.
 * Applies EIP-2200 gas costs and refund accounting, the EIP-8038 execution/state gas split, and
 * EIP-8037 state gas metering.
 */
public class SStoreOperationV2 extends AbstractOperationV2 {

  private static final Logger LOG = LoggerFactory.getLogger(SStoreOperationV2.class);

  /** Minimum gas remaining for Frontier (no minimum). */
  public static final long FRONTIER_MINIMUM = 0L;

  /** Minimum gas remaining for EIP-1706. */
  public static final long EIP_1706_MINIMUM = 2300L;

  /** Illegal state change result (static context or missing account). */
  protected static final OperationResult ILLEGAL_STATE_CHANGE =
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
    return staticOperation(frame, frame.stackDataV2(), gasCalculator(), minimumGasRemaining);
  }

  /**
   * Execute the SSTORE opcode on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack as a long[] array
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator) {
    return staticOperation(frame, s, gasCalculator, FRONTIER_MINIMUM);
  }

  /**
   * Execute the SSTORE opcode with an explicit minimum-gas-remaining check.
   *
   * @param frame the message frame
   * @param s the stack as a long[] array
   * @param gasCalculator the gas calculator
   * @param minimumGasRemaining minimum gas required before executing (EIP-1706: 2300 for Istanbul+)
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame,
      final long[] s,
      final GasCalculator gasCalculator,
      final long minimumGasRemaining) {
    if (!frame.stackHasItemsV2(2)) {
      return new OperationResult(0, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }

    final int top = frame.stackTopV2();

    // Extract key (depth 0) and new value (depth 1) as raw 32-byte arrays
    final byte[] keyBytes = new byte[32];
    final byte[] newValueBytes = new byte[32];
    StackArithmetic.toBytesAt(s, top, 0, keyBytes);
    StackArithmetic.toBytesAt(s, top, 1, newValueBytes);

    // Pop 2
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

    final Bytes32 keyBytes32 = Bytes32.wrap(keyBytes);
    final UInt256 key = UInt256.fromBytes(keyBytes32);
    final UInt256 newValue = UInt256.fromBytes(Bytes32.wrap(newValueBytes));

    final Address address = account.getAddress();
    final boolean slotIsWarm = frame.warmUpStorage(address, keyBytes32);

    // EIP-8038: the repriced access cost can exceed the EIP-2200 stipend, so the sentry above no
    // longer guarantees the access is affordable. Check before the current-value read below, which
    // would otherwise record the slot in the block access list (EIP-7928) for an unpaid access.
    final long accessCost =
        gasCalculator.getWarmStorageReadCost()
            + (slotIsWarm ? 0L : gasCalculator.getSStoreColdAccessGasCost());
    if (remainingGas < accessCost) {
      return new OperationResult(accessCost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Supplier<UInt256> currentValueSupplier =
        Suppliers.memoize(() -> getStorageValue(account, key, frame));
    final Supplier<UInt256> originalValueSupplier =
        Suppliers.memoize(() -> account.getOriginalStorageValue(key));

    final long cost =
        gasCalculator.slotAccessCost(newValue, currentValueSupplier, originalValueSupplier)
            + (slotIsWarm ? 0L : gasCalculator.getSStoreColdAccessGasCost());
    if (remainingGas < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // EIP-8037: Deduct execution gas before charging state gas (ordering requirement).
    // State gas draws from the reservoir first, then from gasRemaining; deducting execution
    // gas first ensures the reservoir/gasRemaining split is correct.
    frame.decrementRemainingGas(cost);

    // Increment the refund counter.
    frame.incrementGasRefund(
        gasCalculator.calculateStorageRefundAmount(
            newValue, currentValueSupplier, originalValueSupplier));

    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "EIP-8037 REC_STORAGE depth={} addr={} key={} txEntryIsZero={} beforeIsZero={} afterIsZero={}",
          frame.getDepth(),
          address.toHexString(),
          "0x" + key.toHexString().substring(2),
          originalValueSupplier.get().isZero(),
          currentValueSupplier.get().isZero(),
          newValue.isZero());
    }

    final StateGasCostCalculator stateGasCalc = gasCalculator.stateGasCostCalculator();
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
    frame.storageWasUpdated(key, Bytes.wrap(newValueBytes));
    frame.getEip7928AccessList().ifPresent(t -> t.addSlotAccessForAccount(address, key));

    return new OperationResult(cost, null);
  }
}
