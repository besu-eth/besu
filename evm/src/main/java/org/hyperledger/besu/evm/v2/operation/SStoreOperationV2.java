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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * EVM v2 SSTORE operation using long[] stack representation.
 *
 * <p>Pops a storage key and new value from the stack, then writes the value to contract storage.
 * Applies EIP-2200 gas costs and refund accounting.
 */
public class SStoreOperationV2 extends AbstractOperationV2 {

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
    if (!frame.stackHasItems(2)) {
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

    final MutableAccount account = frame.getWorldUpdater().getAccount(frame.getRecipientAddress());
    frame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(frame.getRecipientAddress()));

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

    final boolean slotIsWarm = frame.warmUpStorage(frame.getRecipientAddress(), keyBytes32);
    final Supplier<UInt256> currentValueSupplier =
        Suppliers.memoize(() -> account.getStorageValue(key));
    final Supplier<UInt256> originalValueSupplier =
        Suppliers.memoize(() -> account.getOriginalStorageValue(key));

    final long cost =
        gasCalculator.calculateStorageCost(newValue, currentValueSupplier, originalValueSupplier)
            + (slotIsWarm ? 0L : gasCalculator.getColdSloadCost());

    if (remainingGas < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // Increment the refund counter
    frame.incrementGasRefund(
        gasCalculator.calculateStorageRefundAmount(
            newValue, currentValueSupplier, originalValueSupplier));

    account.setStorageValue(key, newValue);
    frame.storageWasUpdated(key, Bytes.wrap(newValueBytes));
    frame
        .getEip7928AccessList()
        .ifPresent(t -> t.addSlotAccessForAccount(frame.getRecipientAddress(), key));

    return new OperationResult(cost, null);
  }
}
